using System.Buffers;
using System.Net;
using System.Net.Security;
using System.Text;
using Spangle.Net.Moqt.Data;
using Spangle.Net.Moqt.Messages;
using Spangle.Net.Moqt.Wire;
using Spangle.Net.Transport.Quic;
using Spangle.Net.Transport.Quic.InMemory;

namespace Spangle.Net.Moqt.Tests;

/// <summary>
/// The stream-type dispatch primitive: a peer that knows nothing of the MOQT wire accepts a
/// stream and gets back a typed <see cref="MoqIncomingStream"/> — a request stream with its
/// first control message read, or a subgroup stream with its header read — without touching a
/// varint. Runs over the in-memory transport.
/// </summary>
public class MoqStreamRouterTests
{
    private static readonly SslApplicationProtocol Alpn = new(MoqtConstants.Alpn);

    private static async Task<(IQuicConnection Client, IQuicConnection Server)> ConnectPairAsync(
        InMemoryQuicTransport transport, IQuicServer server, CancellationToken ct)
    {
        ValueTask<IQuicConnection> acceptTask = server.AcceptConnectionAsync(ct);
        IQuicConnection client = await transport.ConnectAsync(new QuicClientOptions
        {
            RemoteEndPoint = server.LocalEndPoint,
            ApplicationProtocols = [Alpn],
        }, ct);
        IQuicConnection accepted = await acceptTask;
        return (client, accepted);
    }

    [Fact]
    public async Task Classify_BidirectionalStream_IsARequestWithItsFirstMessageRead()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        CancellationToken ct = cts.Token;

        var transport = new InMemoryQuicTransport();
        await using IQuicServer server = await transport.ListenAsync(new QuicServerOptions
        {
            ListenEndPoint = new IPEndPoint(IPAddress.Loopback, 0),
            ApplicationProtocols = [Alpn],
        }, ct);
        (IQuicConnection client, IQuicConnection serverConn) = await ConnectPairAsync(transport, server, ct);

        await using (client)
        await using (serverConn)
        {
            // A subscriber opens a bidirectional request stream and sends SUBSCRIBE.
            await using IQuicStream request =
                await client.OpenStreamAsync(QuicStreamDirection.Bidirectional, ct);
            var payload = new ArrayBufferWriter<byte>();
            new SubscribeMessage(1, FullTrackName.FromStrings(["live"], "cam")).EncodePayload(new MoqWriter(payload));
            var frame = new ArrayBufferWriter<byte>();
            ControlMessage.Write(frame, MoqControlMessageType.Subscribe, payload.WrittenSpan);
            await request.WriteAsync(frame.WrittenMemory, completeWrites: false, ct);

            MoqIncomingStream incoming = await MoqStreamRouter.AcceptAsync(serverConn, ct);

            var req = incoming.Should().BeOfType<MoqRequestStream>().Subject;
            req.MessageType.Should().Be(MoqControlMessageType.Subscribe);
            SubscribeMessage decoded = SubscribeMessage.DecodePayload(req.Payload.Span);
            decoded.Track.NameAsString.Should().Be("cam");
        }
    }

    [Fact]
    public async Task Classify_UnidirectionalStream_IsASubgroupWithItsHeaderRead()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        CancellationToken ct = cts.Token;

        var transport = new InMemoryQuicTransport();
        await using IQuicServer server = await transport.ListenAsync(new QuicServerOptions
        {
            ListenEndPoint = new IPEndPoint(IPAddress.Loopback, 0),
            ApplicationProtocols = [Alpn],
        }, ct);
        (IQuicConnection client, IQuicConnection serverConn) = await ConnectPairAsync(transport, server, ct);

        await using (client)
        await using (serverConn)
        {
            // A publisher opens a unidirectional subgroup stream and writes one object.
            await using IQuicStream data =
                await client.OpenStreamAsync(QuicStreamDirection.Unidirectional, ct);
            var header = new SubgroupHeader
            {
                TrackAlias = 42, GroupId = 0, SubgroupIdMode = SubgroupIdMode.Explicit, SubgroupId = 0,
                PublisherPriority = 100,
            };
            var writer = new SubgroupStreamWriter(data, header);
            await writer.WriteObjectAsync(MoqObject.Normal(0, 0, 0, 100, Encoding.UTF8.GetBytes("f0")), ct);
            await writer.CompleteAsync(ct);

            MoqIncomingStream incoming = await MoqStreamRouter.AcceptAsync(serverConn, ct);

            var sub = incoming.Should().BeOfType<MoqSubgroupStream>().Subject;
            sub.Reader.Header.TrackAlias.Should().Be(42UL);
            MoqObject? first = await sub.Reader.ReadObjectAsync(ct);
            first.Should().NotBeNull();
            Encoding.UTF8.GetString(first!.Payload.Span).Should().Be("f0");
        }
    }
}
