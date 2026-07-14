using System.Net;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using Spangle.Net.Moqt.Messages;
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

    [Fact]
    public async Task Facades_PublishAndSubscribe_OverRealQuic()
    {
        if (!MsQuicTransport.Shared.IsSupported)
        {
            return; // the in-memory flow covers the logic where QUIC cannot run
        }

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
        Task<MoqSession> pubSessionTask = MoqSession.AcceptAsync(serverConn, Setup(), ct);
        await using MoqSession subSession = await MoqSession.ConnectAsync(clientConn, Setup(), ct);
        await using MoqSession pubSession = await pubSessionTask;

        MoqPublisher publisher = MoqPublisher.Create(pubSession);
        MoqPublishedTrack published = publisher.PublishTrack(track);
        using var runCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        Task run = publisher.RunAsync(runCts.Token);

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
        try
        {
            await run;
        }
        catch (OperationCanceledException)
        {
            // the demux loop is cancelled once the flow is verified
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
