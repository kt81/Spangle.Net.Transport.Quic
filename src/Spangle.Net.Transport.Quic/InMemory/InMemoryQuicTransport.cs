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
/// <para>
/// The whole value of this backend is behaving like the real one where protocol code
/// branches: failures throw <see cref="QuicTransportException"/> with the same
/// classification msquic's do, closing a connection aborts the streams on it (a blocked
/// read fails instead of hanging forever), and a peer that stopped reading is visible to
/// the writer. The shared contract test suite runs against both backends to keep it so.
/// </para>
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
            throw new QuicTransportException(QuicTransportError.ConnectionRefused,
                $"The dial to {options.RemoteEndPoint} failed: nothing is listening there.");
        }

        SslApplicationProtocol negotiated = Negotiate(options.ApplicationProtocols, server.ApplicationProtocols);

        var client = new InMemoryQuicConnection(server.LocalEndPoint, negotiated);
        var accepted = new InMemoryQuicConnection(options.RemoteEndPoint, negotiated);
        InMemoryQuicConnection.Link(client, accepted);
        if (!server.TryEnqueueConnection(accepted))
        {
            // The server was disposed between the lookup and the enqueue; without this check
            // the caller would get a "connected" zombie no one will ever accept.
            throw new QuicTransportException(QuicTransportError.ConnectionRefused,
                $"The dial to {options.RemoteEndPoint} failed: the server is gone.");
        }

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

        throw new QuicTransportException(QuicTransportError.ConnectionRefused,
            "The handshake failed: no common ALPN protocol.");
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

    internal bool TryEnqueueConnection(InMemoryQuicConnection connection) =>
        _incoming.Writer.TryWrite(connection);

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

    private readonly Lock _closeLock = new();
    private readonly List<InMemoryQuicStream> _streams = [];
    private QuicTransportException? _closed;
    private bool _disposedLocally;

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

        if (!TryTrack(local))
        {
            throw ClosedException();
        }

        // Like real QUIC, the peer does not see the stream until the opener first writes to it
        // (or completes/aborts it); OpenStreamAsync alone is invisible on the wire. The remote
        // half is published on that first activation, not here.
        return ValueTask.FromResult<IQuicStream>(local);
    }

    // Runs on the opener's first write/complete/abort. Returns whether the peer can ever see
    // the stream; a false return means the connection under it is gone.
    private bool PublishInboundStream(InMemoryQuicStream stream)
    {
        if (!_peer.TryTrack(stream) || !_peer._incomingStreams.Writer.TryWrite(stream))
        {
            stream.FailConnectionGone(new QuicTransportException(QuicTransportError.ConnectionAborted,
                "The connection is gone; the stream never reached the peer."));
            return false;
        }

        return true;
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
            throw ClosedException();
        }
    }

    /// <inheritdoc />
    public ValueTask CloseAsync(long errorCode, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        CloseCore(new QuicTransportException(QuicTransportError.OperationAborted,
                $"The connection was closed locally (error {errorCode})."),
            notifyPeer: true, errorCode);
        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        // Disposing without CloseAsync is the abrupt path, like the real backend's dispose:
        // everything on the connection dies, on both ends, and pending local operations see
        // ObjectDisposedException — which is what msquic's own dispose surfaces.
        lock (_closeLock)
        {
            _disposedLocally = true;
        }

        CloseCore(new QuicTransportException(QuicTransportError.OperationAborted,
                "The connection was disposed locally."),
            notifyPeer: true, errorCode: 0);
        return ValueTask.CompletedTask;
    }

    internal void CloseByPeer(long errorCode) =>
        CloseCore(new QuicTransportException(QuicTransportError.ConnectionAborted,
                $"The peer closed the connection (error {errorCode})."),
            notifyPeer: false, errorCode);

    private void CloseCore(QuicTransportException reason, bool notifyPeer, long errorCode)
    {
        List<InMemoryQuicStream> streams;
        lock (_closeLock)
        {
            if (_closed is not null)
            {
                return;
            }

            _closed = reason;
            streams = [.. _streams];
            _streams.Clear();
        }

        _incomingStreams.Writer.TryComplete();

        // Real QUIC aborts every stream when the connection closes; a blocked read must fail,
        // not hang forever. This is exactly where the in-memory backend used to diverge —
        // hang-shaped bugs stayed green here and only fired on msquic.
        var streamReason = new QuicTransportException(QuicTransportError.ConnectionAborted,
            reason.Message);
        foreach (InMemoryQuicStream stream in streams)
        {
            stream.FailConnectionGone(streamReason);
        }

        if (notifyPeer)
        {
            _peer?.CloseByPeer(errorCode);
        }
    }

    private bool TryTrack(InMemoryQuicStream stream)
    {
        lock (_closeLock)
        {
            if (_closed is not null)
            {
                return false;
            }

            _streams.Add(stream);
            return true;
        }
    }

    // What a pending or later local operation on a closed connection throws: after a local
    // dispose, ObjectDisposedException (matching msquic's own dispose); otherwise the stored
    // transport reason (OperationAborted for a local close, ConnectionAborted for the peer's).
    private Exception ClosedException()
    {
        lock (_closeLock)
        {
            if (_disposedLocally)
            {
                return new ObjectDisposedException(nameof(InMemoryQuicConnection));
            }

            QuicTransportException reason = _closed ?? new QuicTransportException(
                QuicTransportError.ConnectionAborted, "The connection is closed.");
            return new QuicTransportException(reason.Error, reason.Message);
        }
    }
}

/// <summary>One end of an in-memory stream, backed by pipes in one or both directions.</summary>
public sealed class InMemoryQuicStream : IQuicStream
{
    private readonly PipeReader? _inbound;
    private readonly PipeWriter? _outbound;
    private Func<bool>? _onActivate;
    private volatile QuicTransportException? _failed;
    private bool _writesCompleted;

    internal InMemoryQuicStream(long id, QuicStreamDirection direction, PipeReader? inbound, PipeWriter? outbound,
        Func<bool>? onActivate = null)
    {
        Id = id;
        Direction = direction;
        _inbound = inbound;
        _outbound = outbound;
        _onActivate = onActivate;
    }

    // Runs once, on the first write/complete/abort, to make the stream visible to the peer —
    // mirroring QUIC, where opening a stream sends nothing until the first STREAM frame.
    // Returns false when the stream could not reach the peer (the connection is gone).
    private bool Activate()
    {
        Func<bool>? activate = Interlocked.Exchange(ref _onActivate, null);
        return activate is null || activate();
    }

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

        ThrowIfFailed();
        if (buffer.IsEmpty)
        {
            return 0;
        }

        ReadResult result = await _inbound.ReadAsync(cancellationToken).ConfigureAwait(false);
        ReadOnlySequence<byte> sequence = result.Buffer;
        if (result.IsCanceled)
        {
            // Woken, not fed: the stream failed while this read was in flight.
            _inbound.AdvanceTo(sequence.Start);
            ThrowIfFailed();
            throw new QuicTransportException(QuicTransportError.OperationAborted,
                "The read was interrupted.");
        }

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

        ThrowIfFailed();
        if (!Activate())
        {
            ThrowIfFailed(); // FailConnectionGone set the reason during activation
        }

        if (!buffer.IsEmpty)
        {
            FlushResult result = await _outbound.WriteAsync(buffer, cancellationToken).ConfigureAwait(false);
            if (result.IsCanceled)
            {
                // Woken, not flushed: the stream failed while this write was in flight.
                ThrowIfFailed();
                throw new QuicTransportException(QuicTransportError.OperationAborted,
                    "The write was interrupted.");
            }

            if (result.IsCompleted)
            {
                // The peer completed its read side: STOP_SENDING in real QUIC, where the
                // writer gets StreamAborted — not a silent "write succeeded" into the void.
                throw new QuicTransportException(QuicTransportError.StreamAborted,
                    "The peer is no longer reading this stream.");
            }
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
        _ = Activate();
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
        // Activation first: an abort on a never-written stream is still a RESET the peer must
        // see, exactly as QUIC sends RESET_STREAM for a stream its peer never got data on.
        _ = Activate();
        FailCore(new QuicTransportException(QuicTransportError.StreamAborted,
            $"The stream was aborted (error {errorCode})."));
    }

    // The connection under the stream is gone: everything pending or future on it fails with
    // the connection-level reason. Idempotent; called from the connection's close path (both
    // ends of the pair fail their streams together) and for streams that never reached the
    // peer.
    internal void FailConnectionGone(QuicTransportException reason)
    {
        _onActivate = null; // the peer can no longer be reached; do not try on a later write
        FailCore(reason);
    }

    private void FailCore(QuicTransportException reason)
    {
        _failed ??= reason;
        _writesCompleted = true;
        // Completing our writer with the exception is the RESET the peer's reader sees;
        // completing our reader is the STOP_SENDING its writer meets on the next flush. A
        // pipe refuses completion while an operation is in flight on it — then the pending
        // operation is woken instead, and observes _failed when it resumes.
        if (_outbound is not null)
        {
            try
            {
                _outbound.Complete(reason);
            }
            catch (Exception)
            {
                _outbound.CancelPendingFlush();
            }
        }

        if (_inbound is not null)
        {
            try
            {
                _inbound.Complete(reason);
            }
            catch (Exception)
            {
                _inbound.CancelPendingRead();
            }
        }
    }
#pragma warning restore CA1849, VSTHRD103

    private void ThrowIfFailed()
    {
        if (_failed is { } failed)
        {
            throw new QuicTransportException(failed.Error, failed.Message);
        }
    }

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
