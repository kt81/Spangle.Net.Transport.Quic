using System.Net;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using Spangle.Net.Moqt.Data;
using Spangle.Net.Moqt.Messages;
using Spangle.Net.Moqt.Wire;
using Spangle.Net.Transport.Quic;
using Spangle.Net.Transport.Quic.InMemory;
using Spangle.Net.Transport.Quic.MsQuic;

namespace Spangle.Net.Moqt.Tests;

/// <summary>
/// The same publish → subscribe → objects flow as <see cref="PubSubFlowTests"/>, but driven
/// through the <see cref="MoqPublisher"/> / <see cref="MoqSubscriber"/> facades instead of hand-
/// written control frames and subgroup streams. This is the surface the Spangle media bridge
/// calls; the test reads as the bridge's own call sequence. The flow runs over the in-memory
/// transport everywhere and, where QUIC is available, over real msquic too (M1 in-process, M2
/// real-QUIC loopback — same code, two backends).
/// </summary>
public class PublisherSubscriberFacadeTests
{
    private static readonly SslApplicationProtocol Alpn = new(MoqtConstants.Alpn);

    private static SetupMessage Setup() => new();

    [Fact]
    public async Task Facades_PublishAndSubscribe_OverInMemory()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var transport = new InMemoryQuicTransport();
        await using IQuicServer server = await transport.ListenAsync(new QuicServerOptions
        {
            ListenEndPoint = new IPEndPoint(IPAddress.Loopback, 0),
            ApplicationProtocols = [Alpn],
        }, cts.Token);

        (IQuicConnection serverConn, IQuicConnection clientConn) = await ConnectPairAsync(transport, server, cts.Token);
        await RunFlowAndAssertAsync(serverConn, clientConn, cts.Token);
    }

    [SkippableFact]
    public async Task Facades_PublishAndSubscribe_OverRealQuic()
    {
        Skip.IfNot(MsQuicTransport.Shared.IsSupported, "the in-memory flow covers the logic where QUIC cannot run");

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        IQuicTransport transport = MsQuicTransport.Shared;
        using X509Certificate2 certificate = TestCertificates.CreateSelfSigned();
        await using IQuicServer server = await transport.ListenAsync(new QuicServerOptions
        {
            ListenEndPoint = new IPEndPoint(IPAddress.Loopback, 0),
            ApplicationProtocols = [Alpn],
            ServerCertificate = certificate,
        }, cts.Token);

        (IQuicConnection serverConn, IQuicConnection clientConn) = await ConnectPairAsync(transport, server, cts.Token);
        await RunFlowAndAssertAsync(serverConn, clientConn, cts.Token);
    }

    /// <summary>
    /// A group opened with <c>endOfGroup</c> reaches the subscriber saying so. The bit is the only
    /// thing that tells a receiver a group ended deliberately rather than being still in flight, so
    /// a facade that quietly dropped it would cost every subscriber a timeout per group — a fault
    /// that never shows up as a missing object.
    /// </summary>
    [Fact]
    public async Task AGroupOpenedAsEndOfGroup_SaysSoOnTheWire()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        CancellationToken ct = cts.Token;
        var transport = new InMemoryQuicTransport();
        await using IQuicServer server = await transport.ListenAsync(new QuicServerOptions
        {
            ListenEndPoint = new IPEndPoint(IPAddress.Loopback, 0),
            ApplicationProtocols = [Alpn],
        }, ct);

        (IQuicConnection serverConn, IQuicConnection clientConn) = await ConnectPairAsync(transport, server, ct);
        FullTrackName track = FullTrackName.FromStrings(["live"], "video0");

        Task<MoqSession> pubSessionTask = MoqSession.AcceptAsync(serverConn, Setup(), cancellationToken: ct);
        await using MoqSession subSession = await MoqSession.ConnectAsync(clientConn, Setup(), cancellationToken: ct);
        await using MoqSession pubSession = await pubSessionTask;

        MoqPublisher publisher = MoqPublisher.Create(pubSession);
        MoqPublishedTrack published = publisher.PublishTrack(track);
        using var runCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        Task run = publisher.RunAsync(runCts.Token);

        // Subscribed by hand: the raw subgroup header is the object of this test, and the
        // session demux would claim the stream before the test could read it off the wire.
        (ulong alias, IQuicStream subscribeRequest) = await SubscribeByHandAsync(subSession, track, ct);
        await using IQuicStream request = subscribeRequest;
        Task<SubgroupHeader> headerTask = FirstSubgroupHeaderAsync(clientConn, alias, ct);

        await using (MoqGroupWriter group = await published.BeginGroupAsync(7, publisherPriority: 100,
            endOfGroup: true, subgroupId: 3, cancellationToken: ct))
        {
            await group.WriteObjectAsync(0, Encoding.UTF8.GetBytes("only"), cancellationToken: ct);
            await group.CompleteAsync(ct);
        }

        SubgroupHeader header = await headerTask;
        header.EndOfGroup.Should().BeTrue();
        header.SubgroupId.Should().Be(3UL, "a publisher spreading a group over subgroups names each one");
        header.GroupId.Should().Be(7UL);

        await runCts.CancelAsync();
        try
        {
            await run;
        }
        catch (OperationCanceledException)
        {
            // the demux loop is cancelled once the flow is verified
        }
    }

    /// <summary>
    /// A second subscription renames the track, and objects follow the new name. A relay
    /// re-subscribes with a fresh Track Alias each time its own subscriber count rises from none —
    /// so a publisher that kept using the first alias would answer every viewer after the first with
    /// objects the relay has nowhere to put ("unknown track alias"), and they would see nothing.
    /// </summary>
    [Fact]
    public async Task ASecondSubscription_MovesTheTrackToItsNewAlias()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        CancellationToken ct = cts.Token;
        var transport = new InMemoryQuicTransport();
        await using IQuicServer server = await transport.ListenAsync(new QuicServerOptions
        {
            ListenEndPoint = new IPEndPoint(IPAddress.Loopback, 0),
            ApplicationProtocols = [Alpn],
        }, ct);

        (IQuicConnection serverConn, IQuicConnection clientConn) = await ConnectPairAsync(transport, server, ct);
        FullTrackName track = FullTrackName.FromStrings(["live"], "video0");

        Task<MoqSession> pubSessionTask = MoqSession.AcceptAsync(serverConn, Setup(), cancellationToken: ct);
        await using MoqSession subSession = await MoqSession.ConnectAsync(clientConn, Setup(), cancellationToken: ct);
        await using MoqSession pubSession = await pubSessionTask;

        MoqPublisher publisher = MoqPublisher.Create(pubSession);
        MoqPublishedTrack published = publisher.PublishTrack(track);
        using var runCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        Task run = publisher.RunAsync(runCts.Token);

        // Subscribed by hand: this test inspects the raw subgroup wire, which the session
        // demux would otherwise claim.
        (ulong firstAlias, IQuicStream firstRequest) = await SubscribeByHandAsync(subSession, track, ct);
        await using IQuicStream request1 = firstRequest;
        (await published.WaitForSubscriberAsync().WaitAsync(ct)).Should().Be(firstAlias);

        // The same peer subscribes again, as a relay does when a new viewer arrives.
        (ulong secondAlias, IQuicStream secondRequest) = await SubscribeByHandAsync(subSession, track, ct);
        await using IQuicStream request2 = secondRequest;
        secondAlias.Should().NotBe(firstAlias, "each subscription gets its own alias");

        Task<SubgroupHeader> headerTask = FirstSubgroupHeaderAsync(clientConn, secondAlias, ct);
        await using (MoqGroupWriter group = await published.BeginGroupAsync(0, publisherPriority: 100,
            cancellationToken: ct))
        {
            await group.WriteObjectAsync(0, Encoding.UTF8.GetBytes("frame"), cancellationToken: ct);
            await group.CompleteAsync(ct);
        }

        SubgroupHeader header = await headerTask;
        header.TrackAlias.Should().Be(secondAlias, "objects go where the newest subscription says");
        published.CurrentAlias.Should().Be(secondAlias);

        await runCts.CancelAsync();
        try
        {
            await run;
        }
        catch (OperationCanceledException)
        {
            // the demux loop is cancelled once the flow is verified
        }
    }

    /// <summary>
    /// A publisher waiting for a subscriber gives up when the session it would arrive on is gone.
    /// The wait is the whole hazard: a publisher that has announced and has no subscribers yet is
    /// silent by design, so a connection can die under it with nothing to notice — and then a track
    /// waits for a subscriber that has no way of ever arriving, while the demux loop's exception
    /// goes to a task nobody awaited.
    /// </summary>
    [Fact]
    public async Task WhenTheSessionDies_TracksStopWaitingForASubscriber()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        CancellationToken ct = cts.Token;
        var transport = new InMemoryQuicTransport();
        await using IQuicServer server = await transport.ListenAsync(new QuicServerOptions
        {
            ListenEndPoint = new IPEndPoint(IPAddress.Loopback, 0),
            ApplicationProtocols = [Alpn],
        }, ct);

        (IQuicConnection serverConn, IQuicConnection clientConn) = await ConnectPairAsync(transport, server, ct);

        Task<MoqSession> pubSessionTask = MoqSession.AcceptAsync(serverConn, Setup(), cancellationToken: ct);
        await using MoqSession subSession = await MoqSession.ConnectAsync(clientConn, Setup(), cancellationToken: ct);
        MoqSession pubSession = await pubSessionTask;

        MoqPublisher publisher = MoqPublisher.Create(pubSession);
        MoqPublishedTrack published = publisher.PublishTrack(FullTrackName.FromStrings(["live"], "video0"));
        Task run = publisher.RunAsync(ct);

        // Nobody ever subscribes; the connection goes away instead.
        ValueTask<MoqGroupWriter> waiting = published.BeginGroupAsync(0, publisherPriority: 100,
            cancellationToken: ct);
        await subSession.DisposeAsync();
        await clientConn.DisposeAsync();

        Func<Task> act = async () => await waiting;
        await act.Should().ThrowAsync<Exception>("a subscriber cannot arrive on a connection that is gone");

        // `run` is this test's own task; awaiting it is not the foreign-task hazard VSTHRD003 warns of.
#pragma warning disable VSTHRD003
        Func<Task> loop = async () => await run;
#pragma warning restore VSTHRD003
        await loop.Should().ThrowAsync<Exception>("the demux loop reports what stopped it");

        // And a track declared after the fact is not left waiting either.
        MoqPublishedTrack late = publisher.PublishTrack(FullTrackName.FromStrings(["live"], "audio0"));
        Func<Task> lateWait = async () => await late.BeginGroupAsync(0, publisherPriority: 100, cancellationToken: ct);
        await lateWait.Should().ThrowAsync<Exception>();
    }

    /// <summary>
    /// Two concurrent subscriptions on one session, each receiving its own track — the reason
    /// the session owns a single demux loop. Under the previous design every subscription ran
    /// its own accept loop and they raced each other for streams: whichever accepted first
    /// disposed the other's data.
    /// </summary>
    [Fact]
    public async Task TwoSubscriptions_OnOneSession_EachReceiveTheirOwnTrack()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        CancellationToken ct = cts.Token;
        var transport = new InMemoryQuicTransport();
        await using IQuicServer server = await transport.ListenAsync(new QuicServerOptions
        {
            ListenEndPoint = new IPEndPoint(IPAddress.Loopback, 0),
            ApplicationProtocols = [Alpn],
        }, ct);

        (IQuicConnection serverConn, IQuicConnection clientConn) = await ConnectPairAsync(transport, server, ct);
        FullTrackName video = FullTrackName.FromStrings(["live"], "video0");
        FullTrackName audio = FullTrackName.FromStrings(["live"], "audio0");

        Task<MoqSession> pubSessionTask = MoqSession.AcceptAsync(serverConn, Setup(), cancellationToken: ct);
        await using MoqSession subSession = await MoqSession.ConnectAsync(clientConn, Setup(), cancellationToken: ct);
        await using MoqSession pubSession = await pubSessionTask;

        MoqPublisher publisher = MoqPublisher.Create(pubSession);
        MoqPublishedTrack videoTrack = publisher.PublishTrack(video);
        MoqPublishedTrack audioTrack = publisher.PublishTrack(audio);
        using var runCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        Task run = publisher.RunAsync(runCts.Token);
        Task subRun = subSession.RunAsync(runCts.Token);

        MoqSubscriber subscriber = MoqSubscriber.Create(subSession);
        await using MoqSubscription videoSub = await subscriber.SubscribeAsync(video, ct);
        await using MoqSubscription audioSub = await subscriber.SubscribeAsync(audio, ct);

        await using (MoqGroupWriter g = await videoTrack.BeginGroupAsync(0, publisherPriority: 100,
            cancellationToken: ct))
        {
            await g.WriteObjectAsync(0, Encoding.UTF8.GetBytes("v"), cancellationToken: ct);
            await g.CompleteAsync(ct);
        }

        await using (MoqGroupWriter g = await audioTrack.BeginGroupAsync(0, publisherPriority: 100,
            cancellationToken: ct))
        {
            await g.WriteObjectAsync(0, Encoding.UTF8.GetBytes("a"), cancellationToken: ct);
            await g.CompleteAsync(ct);
        }

        MoqObject videoObject = await FirstObjectAsync(videoSub, ct);
        MoqObject audioObject = await FirstObjectAsync(audioSub, ct);
        Encoding.UTF8.GetString(videoObject.Payload.Span).Should().Be("v");
        Encoding.UTF8.GetString(audioObject.Payload.Span).Should().Be("a");

        await runCts.CancelAsync();
        foreach (Task loop in new[] { run, subRun })
        {
            try
            {
                await loop;
            }
            catch (OperationCanceledException)
            {
                // the demux loops are cancelled once the flow is verified
            }
        }
    }

    /// <summary>
    /// An unknown track is answered with REQUEST_ERROR — the code moxygen uses for "no such
    /// namespace or track" — not a bare reset, which reads as a transient failure and invites
    /// the peer to retry forever.
    /// </summary>
    [Fact]
    public async Task SubscribeForAnUnknownTrack_IsAnsweredWithRequestError()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        CancellationToken ct = cts.Token;
        var transport = new InMemoryQuicTransport();
        await using IQuicServer server = await transport.ListenAsync(new QuicServerOptions
        {
            ListenEndPoint = new IPEndPoint(IPAddress.Loopback, 0),
            ApplicationProtocols = [Alpn],
        }, ct);

        (IQuicConnection serverConn, IQuicConnection clientConn) = await ConnectPairAsync(transport, server, ct);

        Task<MoqSession> pubSessionTask = MoqSession.AcceptAsync(serverConn, Setup(), cancellationToken: ct);
        await using MoqSession subSession = await MoqSession.ConnectAsync(clientConn, Setup(), cancellationToken: ct);
        await using MoqSession pubSession = await pubSessionTask;

        MoqPublisher publisher = MoqPublisher.Create(pubSession); // declares no tracks at all
        using var runCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        Task run = publisher.RunAsync(runCts.Token);

        await using IQuicStream request = await subSession.OpenRequestStreamAsync(ct);
        var payload = new System.Buffers.ArrayBufferWriter<byte>();
        new SubscribeMessage(0, FullTrackName.FromStrings(["live"], "nope")).EncodePayload(new MoqWriter(payload));
        var frame = new System.Buffers.ArrayBufferWriter<byte>();
        ControlMessage.Write(frame, MoqControlMessageType.Subscribe, payload.WrittenSpan);
        await request.WriteAsync(frame.WrittenMemory, completeWrites: false, ct);

        (ulong type, byte[] replyPayload) = await ControlMessage.ReadAsync(request, ct);
        type.Should().Be(MoqControlMessageType.RequestError);
        RequestErrorMessage error = RequestErrorMessage.DecodePayload(replyPayload);
        error.ErrorCode.Should().Be(0x10UL, "0x10 is what moxygen answers for a track it does not know");

        await runCts.CancelAsync();
        try
        {
            await run;
        }
        catch (OperationCanceledException)
        {
            // the demux loop is cancelled once the reply is verified
        }
    }

    private static async Task<MoqObject> FirstObjectAsync(MoqSubscription subscription, CancellationToken ct)
    {
        await foreach (MoqObject moqObject in subscription.ReadObjectsAsync(ct))
        {
            return moqObject;
        }

        throw new InvalidOperationException("the subscription ended before an object arrived");
    }

    /// <summary>
    /// SUBSCRIBEs outside the facade, so a test can keep the raw wire to itself: with the
    /// session demux running, the subgroup streams would be claimed before the test saw them.
    /// </summary>
    private static async Task<(ulong Alias, IQuicStream Request)> SubscribeByHandAsync(MoqSession session,
        FullTrackName track, CancellationToken ct)
    {
        IQuicStream request = await session.OpenRequestStreamAsync(ct);
        var payload = new System.Buffers.ArrayBufferWriter<byte>();
        new SubscribeMessage(0, track).EncodePayload(new MoqWriter(payload));
        var frame = new System.Buffers.ArrayBufferWriter<byte>();
        ControlMessage.Write(frame, MoqControlMessageType.Subscribe, payload.WrittenSpan);
        await request.WriteAsync(frame.WrittenMemory, completeWrites: false, ct);

        (ulong type, byte[] okPayload) = await ControlMessage.ReadAsync(request, ct);
        type.Should().Be(MoqControlMessageType.SubscribeOk);
        return (SubscribeOkMessage.DecodePayload(okPayload).TrackAlias, request);
    }

    private static async Task<SubgroupHeader> FirstSubgroupHeaderAsync(IQuicConnection connection, ulong alias,
        CancellationToken ct)
    {
        while (true)
        {
            MoqIncomingStream incoming = await MoqStreamRouter.AcceptAsync(connection, cancellationToken: ct);
            if (incoming is MoqSubgroupStream subgroup && subgroup.Reader.Header.TrackAlias == alias)
            {
                return subgroup.Reader.Header;
            }
        }
    }

    private static async Task<(IQuicConnection Server, IQuicConnection Client)> ConnectPairAsync(
        IQuicTransport transport, IQuicServer server, CancellationToken ct)
    {
        ValueTask<IQuicConnection> acceptConn = server.AcceptConnectionAsync(ct);
        IQuicConnection clientConn = await transport.ConnectAsync(new QuicClientOptions
        {
            RemoteEndPoint = server.LocalEndPoint,
            ApplicationProtocols = [Alpn],
            TargetHost = "localhost",
            AllowUntrustedCertificates = true,
        }, ct);
        IQuicConnection serverConn = await acceptConn;
        return (serverConn, clientConn);
    }

    /// <summary>The bridge's own call sequence: publish two groups, subscribe, assert order.</summary>
    private static async Task RunFlowAndAssertAsync(IQuicConnection serverConn, IQuicConnection clientConn,
        CancellationToken ct)
    {
        FullTrackName track = FullTrackName.FromStrings(["live", "demo"], "video0");

        // SETUP is a concurrent handshake: the acceptor waits for the connector's control stream,
        // so both sides must be established at once.
        Task<MoqSession> pubSessionTask = MoqSession.AcceptAsync(serverConn, Setup(), cancellationToken: ct);
        await using MoqSession subSession = await MoqSession.ConnectAsync(clientConn, Setup(), cancellationToken: ct);
        await using MoqSession pubSession = await pubSessionTask;

        MoqPublisher publisher = MoqPublisher.Create(pubSession);
        MoqPublishedTrack published = publisher.PublishTrack(track);
        using var runCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        Task run = publisher.RunAsync(runCts.Token);
        // The subscriber side needs its own demux loop: subgroup streams only arrive through it.
        Task subRun = subSession.RunAsync(runCts.Token);

        MoqSubscriber subscriber = MoqSubscriber.Create(subSession);
        Task<IReadOnlyList<(ulong Group, ulong Id, string Text)>> subscriberSide =
            CollectAsync(subscriber, track, expected: 3, ct);

        // Two groups: group 0 with objects {0,1}, group 1 with object {0}.
        await using (MoqGroupWriter g0 = await published.BeginGroupAsync(0, publisherPriority: 100,
            cancellationToken: ct))
        {
            await g0.WriteObjectAsync(0, Encoding.UTF8.GetBytes("g0o0"), cancellationToken: ct);
            await g0.WriteObjectAsync(1, Encoding.UTF8.GetBytes("g0o1"), cancellationToken: ct);
            await g0.CompleteAsync(ct);
        }

        await using (MoqGroupWriter g1 = await published.BeginGroupAsync(1, publisherPriority: 100,
            cancellationToken: ct))
        {
            await g1.WriteObjectAsync(0, Encoding.UTF8.GetBytes("g1o0"), cancellationToken: ct);
            await g1.CompleteAsync(ct);
        }

        IReadOnlyList<(ulong Group, ulong Id, string Text)> received = await subscriberSide;
        received.Should().Equal(
            (0UL, 0UL, "g0o0"),
            (0UL, 1UL, "g0o1"),
            (1UL, 0UL, "g1o0"));

        await runCts.CancelAsync();
        foreach (Task loop in new[] { run, subRun })
        {
            try
            {
                await loop;
            }
            catch (OperationCanceledException)
            {
                // the demux loops are cancelled once the flow is verified
            }
        }
    }

    private static async Task<IReadOnlyList<(ulong Group, ulong Id, string Text)>> CollectAsync(
        MoqSubscriber subscriber, FullTrackName track, int expected, CancellationToken ct)
    {
        await using MoqSubscription subscription = await subscriber.SubscribeAsync(track, ct);
        var received = new List<(ulong, ulong, string)>();
        await foreach (MoqObject moqObject in subscription.ReadObjectsAsync(ct))
        {
            received.Add((moqObject.GroupId, moqObject.ObjectId, Encoding.UTF8.GetString(moqObject.Payload.Span)));
            if (received.Count == expected)
            {
                break;
            }
        }

        return received;
    }
}
