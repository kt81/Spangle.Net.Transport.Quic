using System.Buffers;
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
/// The end-to-end subscribe path over the in-memory transport: a subscriber sends SUBSCRIBE
/// on a bidirectional request stream; the publisher answers SUBSCRIBE_OK with a Track Alias
/// and streams the track's objects on a unidirectional subgroup stream tagged with that
/// alias; the subscriber matches the alias and receives the objects. This is the package
/// boundary the Spangle bridge drives.
/// </summary>
public class PubSubFlowTests
{
    private static readonly SslApplicationProtocol Alpn = new(MoqtConstants.Alpn);
    private const ulong AssignedAlias = 42;

    private static byte[] Frame(ulong type, Action<MoqWriter> writePayload)
    {
        var payload = new ArrayBufferWriter<byte>();
        writePayload(new MoqWriter(payload));
        var frame = new ArrayBufferWriter<byte>();
        ControlMessage.Write(frame, type, payload.WrittenSpan);
        return frame.WrittenSpan.ToArray();
    }

    private static async Task RunPublisherAsync(IQuicConnection connection, CancellationToken ct)
    {
        await using IQuicStream request = await connection.AcceptStreamAsync(ct);
        (ulong type, byte[] payload) = await ControlMessage.ReadAsync(request, ct);
        type.Should().Be(MoqControlMessageType.Subscribe);
        SubscribeMessage subscribe = SubscribeMessage.DecodePayload(payload);
        subscribe.Track.NameAsString.Should().Be("cam");

        byte[] ok = Frame(MoqControlMessageType.SubscribeOk, new SubscribeOkMessage(AssignedAlias).EncodePayload);
        await request.WriteAsync(ok, completeWrites: false, ct);

        await using IQuicStream data = await connection.OpenStreamAsync(QuicStreamDirection.Unidirectional, ct);
        var header = new SubgroupHeader
        {
            TrackAlias = AssignedAlias,
            GroupId = 0,
            SubgroupIdMode = SubgroupIdMode.Explicit,
            SubgroupId = 0,
            PublisherPriority = 100,
        };
        var writer = new SubgroupStreamWriter(data, header);
        for (ulong id = 0; id < 3; id++)
        {
            await writer.WriteObjectAsync(MoqObject.Normal(0, id, 0, 100, Encoding.UTF8.GetBytes($"f{id}")), ct);
        }

        await writer.CompleteAsync(ct);
    }

    private static async Task<IReadOnlyList<string>> RunSubscriberAsync(IQuicConnection connection, CancellationToken ct)
    {
        // subscriber: send SUBSCRIBE on a bidirectional request stream
        await using IQuicStream request = await connection.OpenStreamAsync(QuicStreamDirection.Bidirectional, ct);
        byte[] subscribe = Frame(MoqControlMessageType.Subscribe,
            new SubscribeMessage(1, FullTrackName.FromStrings(["live"], "cam")).EncodePayload);
        await request.WriteAsync(subscribe, completeWrites: false, ct);

        // read SUBSCRIBE_OK and learn the Track Alias
        (ulong type, byte[] payload) = await ControlMessage.ReadAsync(request, ct);
        type.Should().Be(MoqControlMessageType.SubscribeOk);
        SubscribeOkMessage ok = SubscribeOkMessage.DecodePayload(payload);
        ok.TrackAlias.Should().Be(AssignedAlias);

        // receive the track's objects on the subgroup stream carrying that alias
        await using IQuicStream data = await connection.AcceptStreamAsync(ct);
        var reader = await SubgroupStreamReader.OpenAsync(data, cancellationToken: ct);
        reader.Header.TrackAlias.Should().Be(ok.TrackAlias);

        var payloads = new List<string>();
        while (await reader.ReadObjectAsync(ct) is { } moqObject)
        {
            payloads.Add(Encoding.UTF8.GetString(moqObject.Payload.Span));
        }

        return payloads;
    }

    [Fact]
    public async Task Subscribe_ThenObjectsFlowOnTheAssignedTrackAlias()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        CancellationToken ct = cts.Token;

        var transport = new InMemoryQuicTransport();
        await using IQuicServer server = await transport.ListenAsync(new QuicServerOptions
        {
            ListenEndPoint = new IPEndPoint(IPAddress.Loopback, 0),
            ApplicationProtocols = [Alpn],
        }, ct);

        ValueTask<IQuicConnection> acceptConn = server.AcceptConnectionAsync(ct);
        await using IQuicConnection clientConn = await transport.ConnectAsync(new QuicClientOptions
        {
            RemoteEndPoint = server.LocalEndPoint,
            ApplicationProtocols = [Alpn],
        }, ct);
        await using IQuicConnection serverConn = await acceptConn;

        Task publisher = RunPublisherAsync(serverConn, ct);
        IReadOnlyList<string> payloads = await RunSubscriberAsync(clientConn, ct);
        await publisher;

        payloads.Should().Equal("f0", "f1", "f2");
    }

    [Fact]
    public async Task Subscribe_ThenObjectsFlow_OverRealQuic()
    {
        if (!MsQuicTransport.Shared.IsSupported)
        {
            return; // the in-memory flow covers the logic where QUIC cannot run
        }

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        CancellationToken ct = cts.Token;
        IQuicTransport transport = MsQuicTransport.Shared;

        using X509Certificate2 certificate = TestCertificates.CreateSelfSigned();
        await using IQuicServer server = await transport.ListenAsync(new QuicServerOptions
        {
            ListenEndPoint = new IPEndPoint(IPAddress.Loopback, 0),
            ApplicationProtocols = [Alpn],
            ServerCertificate = certificate,
        }, ct);

        ValueTask<IQuicConnection> acceptConn = server.AcceptConnectionAsync(ct);
        await using IQuicConnection clientConn = await transport.ConnectAsync(new QuicClientOptions
        {
            RemoteEndPoint = server.LocalEndPoint,
            ApplicationProtocols = [Alpn],
            TargetHost = "localhost",
            AllowUntrustedCertificates = true,
        }, ct);
        await using IQuicConnection serverConn = await acceptConn;

        Task publisher = RunPublisherAsync(serverConn, ct);
        IReadOnlyList<string> payloads = await RunSubscriberAsync(clientConn, ct);
        await publisher;

        payloads.Should().Equal("f0", "f1", "f2");
    }
}
