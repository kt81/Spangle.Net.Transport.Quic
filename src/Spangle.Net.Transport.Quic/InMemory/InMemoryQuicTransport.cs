using System.Buffers;
using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.IO.Pipelines;
using System.Net;
using System.Net.Security;
using System.Threading.Channels;

namespace Spangle.Net.Transport.Quic.InMemory;

/// <summary>
/// An in-process QUIC-shaped transport: connections and streams are wired with
/// <see cref="Pipe"/>s, no sockets and no msquic. Its reason to exist is that
/// System.Net.Quic cannot run without native msquic and an IPv6 stack (for its dual-mode
/// sockets), so protocol code written against <see cref="IQuicTransport"/> can still be
/// exercised end to end — a client and a server on the same instance talk over real
/// backpressured byte streams. Each instance is an isolated "network"; a client can only
/// reach a server that is listening on the same instance.
/// </summary>
public sealed class InMemoryQuicTransport : IQuicTransport
{
    private readonly ConcurrentDictionary<EndPoint, InMemoryQuicServer> _servers = new();
    private int _syntheticPort = 50_000;

    /// <inheritdoc />
    public bool IsSupported => true;

    /// <inheritdoc />
    public ValueTask<IQuicServer> ListenAsync(QuicServerOptions options,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(options);
        cancellationToken.ThrowIfCancellationRequested();

        IPEndPoint endPoint = options.ListenEndPoint.Port == 0
            ? new IPEndPoint(options.ListenEndPoint.Address, Interlocked.Increment(ref _syntheticPort))
            : options.ListenEndPoint;

        var server = new InMemoryQuicServer(this, endPoint, options.ApplicationProtocols);
        if (!_servers.TryAdd(endPoint, server))
        {
            throw new InvalidOperationException($"An in-memory server is already listening on {endPoint}.");
        }

        return ValueTask.FromResult<IQuicServer>(server);
    }

    /// <inheritdoc />
    [SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope",
        Justification = "Both connection halves are owned by their receivers: the client is returned to the caller and the accepted half is handed to the server's accept queue; disposing here would close them.")]
    public ValueTask<IQuicConnection> ConnectAsync(QuicClientOptions options,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(options);
        cancellationToken.ThrowIfCancellationRequested();

        if (!_servers.TryGetValue(options.RemoteEndPoint, out InMemoryQuicServer? server))
        {
            throw new InvalidOperationException($"Connection refused: nothing is listening on {options.RemoteEndPoint}.");
        }

        SslApplicationProtocol negotiated = Negotiate(options.ApplicationProtocols, server.ApplicationProtocols);

        var client = new InMemoryQuicConnection(server.LocalEndPoint, negotiated);
        var accepted = new InMemoryQuicConnection(options.RemoteEndPoint, negotiated);
        InMemoryQuicConnection.Link(client, accepted);
        server.EnqueueConnection(accepted);
        return ValueTask.FromResult<IQuicConnection>(client);
    }

    internal void Unregister(EndPoint endPoint) => _servers.TryRemove(endPoint, out _);

    private static SslApplicationProtocol Negotiate(
        IReadOnlyList<SslApplicationProtocol> client, IReadOnlyList<SslApplicationProtocol> server)
    {
        foreach (SslApplicationProtocol protocol in client)
        {
            if (server.Contains(protocol))
            {
                return protocol;
            }
        }

        throw new InvalidOperationException("QUIC handshake failed: no common ALPN protocol.");
    }
}

/// <summary>A listening in-memory endpoint.</summary>
public sealed class InMemoryQuicServer : IQuicServer
{
    private readonly InMemoryQuicTransport _transport;
    private readonly Channel<InMemoryQuicConnection> _incoming =
        Channel.CreateUnbounded<InMemoryQuicConnection>();

    internal InMemoryQuicServer(InMemoryQuicTransport transport, EndPoint localEndPoint,
        IReadOnlyList<SslApplicationProtocol> applicationProtocols)
    {
        _transport = transport;
        LocalEndPoint = localEndPoint;
        ApplicationProtocols = applicationProtocols;
    }

    /// <inheritdoc />
    public EndPoint LocalEndPoint { get; }

    internal IReadOnlyList<SslApplicationProtocol> ApplicationProtocols { get; }

    /// <inheritdoc />
    public async ValueTask<IQuicConnection> AcceptConnectionAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            return await _incoming.Reader.ReadAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (ChannelClosedException)
        {
            throw new ObjectDisposedException(nameof(InMemoryQuicServer), "The server has been disposed.");
        }
    }

    internal void EnqueueConnection(InMemoryQuicConnection connection) => _incoming.Writer.TryWrite(connection);

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        _incoming.Writer.TryComplete();
        _transport.Unregister(LocalEndPoint);
        return ValueTask.CompletedTask;
    }
}

/// <summary>One end of an in-memory connection pair.</summary>
public sealed class InMemoryQuicConnection : IQuicConnection
{
    private readonly Channel<InMemoryQuicStream> _incomingStreams =
        Channel.CreateUnbounded<InMemoryQuicStream>();

    [SuppressMessage("Usage", "CA2213:Disposable fields should be disposed",
        Justification = "The peer is the other end of the pair, owned by its own receiver; a connection never disposes its peer.")]
    private InMemoryQuicConnection _peer = null!;
    private long _nextStreamId;

    internal InMemoryQuicConnection(EndPoint remoteEndPoint, SslApplicationProtocol negotiated)
    {
        RemoteEndPoint = remoteEndPoint;
        NegotiatedApplicationProtocol = negotiated;
    }

    /// <inheritdoc />
    public EndPoint RemoteEndPoint { get; }

    /// <inheritdoc />
    public SslApplicationProtocol NegotiatedApplicationProtocol { get; }

    internal static void Link(InMemoryQuicConnection client, InMemoryQuicConnection server)
    {
        client._peer = server;
        server._peer = client;
        // Client-initiated stream ids are even, server-initiated odd (QUIC convention),
        // so a fresh stream id is unique across the pair.
        client._nextStreamId = 0;
        server._nextStreamId = 1;
    }

    /// <inheritdoc />
    [SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope",
        Justification = "Both stream halves are owned by their receivers: the local half is returned to the caller and the remote half is handed to the peer's accept queue.")]
    public ValueTask<IQuicStream> OpenStreamAsync(QuicStreamDirection direction,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        long id = Interlocked.Add(ref _nextStreamId, 2) - 2;

        InMemoryQuicStream local;
        InMemoryQuicStream remote;
        if (direction == QuicStreamDirection.Unidirectional)
        {
            var pipe = new Pipe();
            remote = new InMemoryQuicStream(id, direction, inbound: pipe.Reader, outbound: null);
            local = new InMemoryQuicStream(id, direction, inbound: null, outbound: pipe.Writer,
                onActivate: () => PublishInboundStream(remote));
        }
        else
        {
            var toAcceptor = new Pipe();
            var toOpener = new Pipe();
            remote = new InMemoryQuicStream(id, direction, inbound: toAcceptor.Reader, outbound: toOpener.Writer);
            local = new InMemoryQuicStream(id, direction, inbound: toOpener.Reader, outbound: toAcceptor.Writer,
                onActivate: () => PublishInboundStream(remote));
        }

        // Like real QUIC, the peer does not see the stream until the opener first writes to it
        // (or completes/aborts it); OpenStreamAsync alone is invisible on the wire. The remote
        // half is published on that first activation, not here.
        return ValueTask.FromResult<IQuicStream>(local);
    }

    private void PublishInboundStream(InMemoryQuicStream stream)
    {
        if (!_peer._incomingStreams.Writer.TryWrite(stream))
        {
            throw new InvalidOperationException("The peer connection is closed.");
        }
    }

    /// <inheritdoc />
    public async ValueTask<IQuicStream> AcceptStreamAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            return await _incomingStreams.Reader.ReadAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (ChannelClosedException)
        {
            throw new ObjectDisposedException(nameof(InMemoryQuicConnection), "The connection has been closed.");
        }
    }

    /// <inheritdoc />
    public ValueTask CloseAsync(long errorCode, CancellationToken cancellationToken = default)
    {
        _ = errorCode;
        cancellationToken.ThrowIfCancellationRequested();
        _incomingStreams.Writer.TryComplete();
        _peer._incomingStreams.Writer.TryComplete();
        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        _incomingStreams.Writer.TryComplete();
        return ValueTask.CompletedTask;
    }
}

/// <summary>One end of an in-memory stream, backed by pipes in one or both directions.</summary>
public sealed class InMemoryQuicStream : IQuicStream
{
    private readonly PipeReader? _inbound;
    private readonly PipeWriter? _outbound;
    private Action? _onActivate;
    private bool _writesCompleted;

    internal InMemoryQuicStream(long id, QuicStreamDirection direction, PipeReader? inbound, PipeWriter? outbound,
        Action? onActivate = null)
    {
        Id = id;
        Direction = direction;
        _inbound = inbound;
        _outbound = outbound;
        _onActivate = onActivate;
    }

    // Runs once, on the first write/complete/abort, to make the stream visible to the peer —
    // mirroring QUIC, where opening a stream sends nothing until the first STREAM frame.
    private void Activate() => Interlocked.Exchange(ref _onActivate, null)?.Invoke();

    /// <inheritdoc />
    public long Id { get; }

    /// <inheritdoc />
    public QuicStreamDirection Direction { get; }

    /// <inheritdoc />
    public bool CanRead => _inbound is not null;

    /// <inheritdoc />
    public bool CanWrite => _outbound is not null;

    /// <inheritdoc />
    public async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        if (_inbound is null)
        {
            throw new InvalidOperationException("This stream half is write-only.");
        }

        if (buffer.IsEmpty)
        {
            return 0;
        }

        ReadResult result = await _inbound.ReadAsync(cancellationToken).ConfigureAwait(false);
        ReadOnlySequence<byte> sequence = result.Buffer;
        if (sequence.IsEmpty && result.IsCompleted)
        {
            _inbound.AdvanceTo(sequence.End);
            return 0;
        }

        int count = (int)Math.Min(buffer.Length, sequence.Length);
        sequence.Slice(0, count).CopyTo(buffer.Span);
        _inbound.AdvanceTo(sequence.GetPosition(count));
        return count;
    }

    /// <inheritdoc />
    public async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, bool completeWrites = false,
        CancellationToken cancellationToken = default)
    {
        if (_outbound is null)
        {
            throw new InvalidOperationException("This stream half is read-only.");
        }

        Activate();
        if (!buffer.IsEmpty)
        {
            await _outbound.WriteAsync(buffer, cancellationToken).ConfigureAwait(false);
        }

        if (completeWrites)
        {
            CompleteWrites();
        }
    }

    // CompleteWrites and Abort are synchronous by the IQuicStream contract (they mirror
    // QuicStream's own sync CompleteWrites/Abort). PipeWriter/PipeReader.Complete on a plain
    // in-memory Pipe just flips completion state — it does not flush or block — so the
    // "await CompleteAsync instead" analyzers do not apply here.
#pragma warning disable CA1849, VSTHRD103
    /// <inheritdoc />
    public void CompleteWrites()
    {
        Activate();
        if (_writesCompleted || _outbound is null)
        {
            return;
        }

        _writesCompleted = true;
        _outbound.Complete();
    }

    /// <inheritdoc />
    public void Abort(long errorCode)
    {
        Activate();
        var reset = new IOException($"Stream aborted (error {errorCode}).");
        _outbound?.Complete(reset);
        _inbound?.Complete(reset);
        _writesCompleted = true;
    }
#pragma warning restore CA1849, VSTHRD103

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        CompleteWrites();
        if (_inbound is not null)
        {
            await _inbound.CompleteAsync().ConfigureAwait(false);
        }
    }
}
