using System.Net;
using System.Net.Security;
using System.Text;
using Spangle.Net.Moqt;
using Spangle.Net.Moqt.Messages;
using Spangle.Net.Moqt.Wire;
using Spangle.Net.Transport.Quic;
using Spangle.Net.Transport.Quic.InMemory;

namespace Spangle.Net.Moqt.Tests;

/// <summary>
/// The SETUP handshake over a QUIC connection: each endpoint opens its own control stream
/// (draft-18 §10), sends SETUP, and reads the peer's. Run over the in-memory transport, so
/// it needs no msquic or IPv6.
/// </summary>
public class MoqSessionTests
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
    public async Task Handshake_ExchangesSetupOptionsBothWays()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        CancellationToken ct = cts.Token;

        var transport = new InMemoryQuicTransport();
        await using IQuicServer server = await transport.ListenAsync(new QuicServerOptions
        {
            ListenEndPoint = new IPEndPoint(IPAddress.Loopback, 0),
            ApplicationProtocols = [Alpn],
        }, ct);

        (IQuicConnection clientConn, IQuicConnection serverConn) = await ConnectPairAsync(transport, server, ct);

        var clientSetup = new SetupMessage([MoqKeyValuePair.Varint(MoqSetupOption.MaxAuthTokenCacheSize, 1234)]);
        var serverSetup = new SetupMessage(
            [MoqKeyValuePair.FromBytes(MoqSetupOption.MoqtImplementation, Encoding.UTF8.GetBytes("spangle"))]);

        // server reads then answers; client opens+sends then reads — run concurrently
        Task<MoqSession> serverSessionTask = MoqSession.AcceptAsync(serverConn, serverSetup, ct);
        await using MoqSession clientSession = await MoqSession.ConnectAsync(clientConn, clientSetup, ct);
        await using MoqSession serverSession = await serverSessionTask;

        serverSession.IsServer.Should().BeTrue();
        clientSession.IsServer.Should().BeFalse();

        // each endpoint received the other's SETUP options
        clientSession.RemoteSetup.Options.Should().ContainSingle()
            .Which.Type.Should().Be(MoqSetupOption.MoqtImplementation);
        Encoding.UTF8.GetString(clientSession.RemoteSetup.Options[0].Bytes).Should().Be("spangle");

        MoqKeyValuePair received = serverSession.RemoteSetup.Options.Should().ContainSingle().Subject;
        received.Type.Should().Be(MoqSetupOption.MaxAuthTokenCacheSize);
        received.VarintValue.Should().Be(1234UL);
    }

    [Fact]
    public async Task Connect_WhenPeerSendsWrongFirstMessage_Throws()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        CancellationToken ct = cts.Token;

        var transport = new InMemoryQuicTransport();
        await using IQuicServer server = await transport.ListenAsync(new QuicServerOptions
        {
            ListenEndPoint = new IPEndPoint(IPAddress.Loopback, 0),
            ApplicationProtocols = [Alpn],
        }, ct);

        (IQuicConnection clientConn, IQuicConnection serverConn) = await ConnectPairAsync(transport, server, ct);
        await using (clientConn)
        await using (serverConn)
        {
            // The "server" opens a control stream but sends GOAWAY instead of SETUP.
            await using IQuicStream bogus = await serverConn.OpenStreamAsync(QuicStreamDirection.Unidirectional, ct);
            var frame = new System.Buffers.ArrayBufferWriter<byte>();
            ControlMessage.Write(frame, MoqControlMessageType.GoAway, ReadOnlySpan<byte>.Empty);
            await bogus.WriteAsync(frame.WrittenMemory, completeWrites: false, ct);

            // The client sends SETUP fine, but reading the peer's first message rejects non-SETUP.
            IQuicStream clientOut = await clientConn.OpenStreamAsync(QuicStreamDirection.Unidirectional, ct);
            var setupFrame = new System.Buffers.ArrayBufferWriter<byte>();
            ControlMessage.Write(setupFrame, MoqControlMessageType.Setup, ReadOnlySpan<byte>.Empty);
            await clientOut.WriteAsync(setupFrame.WrittenMemory, completeWrites: false, ct);

            IQuicStream peer = await clientConn.AcceptStreamAsync(ct);
            Func<Task> act = async () => await ControlMessageAssertSetup(peer, ct);
            await act.Should().ThrowAsync<MoqProtocolException>();
        }
    }

    private static async Task ControlMessageAssertSetup(IQuicStream stream, CancellationToken ct)
    {
        (ulong type, _) = await ControlMessage.ReadAsync(stream, ct);
        if (type != MoqControlMessageType.Setup)
        {
            throw new MoqProtocolException($"Expected SETUP, got 0x{type:X}.");
        }
    }
}
