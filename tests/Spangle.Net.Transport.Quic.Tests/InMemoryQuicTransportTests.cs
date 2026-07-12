using System.Net;
using System.Net.Security;
using System.Text;
using Spangle.Net.Transport.Quic;
using Spangle.Net.Transport.Quic.InMemory;

namespace Spangle.Net.Transport.Quic.Tests;

/// <summary>
/// The in-process backend is what lets MoQ protocol code be exercised where msquic (and the
/// IPv6 stack System.Net.Quic needs) is absent. These tests pin its contract: connect/accept,
/// ALPN negotiation, one- and two-way streams over real backpressured pipes, graceful EOF,
/// abrupt abort, and connection-refused.
/// </summary>
public class InMemoryQuicTransportTests
{
    private static readonly SslApplicationProtocol Moq = new("moq-00");

    private static QuicServerOptions ServerOptions() => new()
    {
        ListenEndPoint = new IPEndPoint(IPAddress.Loopback, 0),
        ApplicationProtocols = [Moq],
    };

    private static QuicClientOptions ClientOptions(EndPoint remote) => new()
    {
        RemoteEndPoint = remote,
        ApplicationProtocols = [Moq],
    };

    private static async Task<(IQuicConnection Client, IQuicConnection Server)> ConnectPairAsync(
        IQuicTransport transport, IQuicServer server, CancellationToken ct)
    {
        ValueTask<IQuicConnection> acceptTask = server.AcceptConnectionAsync(ct);
        IQuicConnection client = await transport.ConnectAsync(ClientOptions(server.LocalEndPoint), ct);
        IQuicConnection accepted = await acceptTask;
        return (client, accepted);
    }

    private static async Task<byte[]> ReadToEndAsync(IQuicStream stream, CancellationToken ct)
    {
        var sink = new List<byte>();
        var buffer = new byte[4096];
        while (true)
        {
            int read = await stream.ReadAsync(buffer, ct);
            if (read == 0)
            {
                break;
            }

            sink.AddRange(buffer.AsSpan(0, read));
        }

        return [.. sink];
    }

    [Fact]
    public async Task Connect_NegotiatesAlpn_AndReportsEndpoints()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        CancellationToken ct = cts.Token;
        var transport = new InMemoryQuicTransport();
        await using IQuicServer server = await transport.ListenAsync(ServerOptions(), ct);

        (IQuicConnection client, IQuicConnection accepted) = await ConnectPairAsync(transport, server, ct);
        await using (client)
        await using (accepted)
        {
            client.NegotiatedApplicationProtocol.Should().Be(Moq);
            accepted.NegotiatedApplicationProtocol.Should().Be(Moq);
            client.RemoteEndPoint.Should().Be(server.LocalEndPoint);
        }
    }

    [Fact]
    public async Task Connect_WithNoListener_IsRefused()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var transport = new InMemoryQuicTransport();
        Func<Task> act = async () => await transport.ConnectAsync(
            ClientOptions(new IPEndPoint(IPAddress.Loopback, 51234)), cts.Token);
        await act.Should().ThrowAsync<InvalidOperationException>();
    }

    [Fact]
    public async Task Connect_WithoutCommonAlpn_FailsHandshake()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        CancellationToken ct = cts.Token;
        var transport = new InMemoryQuicTransport();
        await using IQuicServer server = await transport.ListenAsync(ServerOptions(), ct);

        var clientOptions = new QuicClientOptions
        {
            RemoteEndPoint = server.LocalEndPoint,
            ApplicationProtocols = [new SslApplicationProtocol("something-else")],
        };
        Func<Task> act = async () => await transport.ConnectAsync(clientOptions, ct);
        await act.Should().ThrowAsync<InvalidOperationException>();
    }

    [Fact]
    public async Task UnidirectionalStream_OpenerWrites_AcceptorReadsToEof()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        CancellationToken ct = cts.Token;
        var transport = new InMemoryQuicTransport();
        await using IQuicServer server = await transport.ListenAsync(ServerOptions(), ct);
        (IQuicConnection client, IQuicConnection accepted) = await ConnectPairAsync(transport, server, ct);

        await using (client)
        await using (accepted)
        {
            byte[] payload = Encoding.UTF8.GetBytes("hello over an object stream");

            ValueTask<IQuicStream> acceptTask = accepted.AcceptStreamAsync(ct);
            IQuicStream send = await client.OpenStreamAsync(QuicStreamDirection.Unidirectional, ct);
            IQuicStream recv = await acceptTask;

            send.CanWrite.Should().BeTrue();
            send.CanRead.Should().BeFalse();
            recv.CanRead.Should().BeTrue();
            recv.CanWrite.Should().BeFalse();
            recv.Direction.Should().Be(QuicStreamDirection.Unidirectional);

            await send.WriteAsync(payload, completeWrites: true, ct);
            byte[] received = await ReadToEndAsync(recv, ct);

            received.Should().Equal(payload);
        }
    }

    [Fact]
    public async Task BidirectionalStream_EchoesBothWays()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        CancellationToken ct = cts.Token;
        var transport = new InMemoryQuicTransport();
        await using IQuicServer server = await transport.ListenAsync(ServerOptions(), ct);
        (IQuicConnection client, IQuicConnection accepted) = await ConnectPairAsync(transport, server, ct);

        await using (client)
        await using (accepted)
        {
            ValueTask<IQuicStream> acceptTask = accepted.AcceptStreamAsync(ct);
            IQuicStream clientStream = await client.OpenStreamAsync(QuicStreamDirection.Bidirectional, ct);
            IQuicStream serverStream = await acceptTask;

            clientStream.CanRead.Should().BeTrue();
            clientStream.CanWrite.Should().BeTrue();

            byte[] request = Encoding.UTF8.GetBytes("ping");
            await clientStream.WriteAsync(request, completeWrites: true, ct);
            byte[] serverGot = await ReadToEndAsync(serverStream, ct);
            serverGot.Should().Equal(request);

            byte[] response = Encoding.UTF8.GetBytes("PONG");
            await serverStream.WriteAsync(response, completeWrites: true, ct);
            byte[] clientGot = await ReadToEndAsync(clientStream, ct);
            clientGot.Should().Equal(response);
        }
    }

    [Fact]
    public async Task LargeTransfer_RoundTripsIntact_UnderBackpressure()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        CancellationToken ct = cts.Token;
        var transport = new InMemoryQuicTransport();
        await using IQuicServer server = await transport.ListenAsync(ServerOptions(), ct);
        (IQuicConnection client, IQuicConnection accepted) = await ConnectPairAsync(transport, server, ct);

        await using (client)
        await using (accepted)
        {
            var payload = new byte[512 * 1024];
            for (int i = 0; i < payload.Length; i++)
            {
                payload[i] = (byte)(i * 31 + 7);
            }

            ValueTask<IQuicStream> acceptTask = accepted.AcceptStreamAsync(ct);
            IQuicStream send = await client.OpenStreamAsync(QuicStreamDirection.Unidirectional, ct);
            IQuicStream recv = await acceptTask;

            // reader and writer run concurrently so pipe backpressure is actually exercised
            Task<byte[]> reader = ReadToEndAsync(recv, ct);
            await send.WriteAsync(payload, completeWrites: true, ct);
            byte[] received = await reader;

            received.Should().Equal(payload);
        }
    }

    [Fact]
    public async Task Abort_MakesPeerReadFail()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        CancellationToken ct = cts.Token;
        var transport = new InMemoryQuicTransport();
        await using IQuicServer server = await transport.ListenAsync(ServerOptions(), ct);
        (IQuicConnection client, IQuicConnection accepted) = await ConnectPairAsync(transport, server, ct);

        await using (client)
        await using (accepted)
        {
            ValueTask<IQuicStream> acceptTask = accepted.AcceptStreamAsync(ct);
            IQuicStream send = await client.OpenStreamAsync(QuicStreamDirection.Unidirectional, ct);
            IQuicStream recv = await acceptTask;

            await send.WriteAsync(Encoding.UTF8.GetBytes("partial"), completeWrites: false, ct);
            send.Abort(errorCode: 42);

            Func<Task> readAll = async () =>
            {
                var buffer = new byte[64];
                while (await recv.ReadAsync(buffer, ct) != 0)
                {
                    // drain until the abort surfaces as an IOException
                }
            };
            await readAll.Should().ThrowAsync<IOException>();
        }
    }

    [Fact]
    public async Task Accept_AfterServerDisposed_Throws()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        CancellationToken ct = cts.Token;
        var transport = new InMemoryQuicTransport();
        IQuicServer server = await transport.ListenAsync(ServerOptions(), ct);
        await server.DisposeAsync();

        Func<Task> act = async () => await server.AcceptConnectionAsync(ct);
        await act.Should().ThrowAsync<ObjectDisposedException>();
    }
}
