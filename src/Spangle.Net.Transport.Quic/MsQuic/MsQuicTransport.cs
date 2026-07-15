using System.Net;
using System.Net.Quic;
using System.Net.Security;

namespace Spangle.Net.Transport.Quic.MsQuic;

// System.Net.Quic carries platform annotations; every entry point here is reached only
// after IsSupported (a documented platform guard) is checked by the caller, and the
// backend throws PlatformNotSupportedException on its own if construction is attempted
// where QUIC cannot run. Suppressing CA1416 keeps the cross-target build clean.
#pragma warning disable CA1416

/// <summary>
/// The real QUIC backend: <see cref="System.Net.Quic"/> over native msquic. Requires the
/// platform to support QUIC (msquic present, and an IPv6 stack for the dual-mode sockets
/// System.Net.Quic binds); <see cref="IsSupported"/> reports whether it will run here.
/// </summary>
public sealed class MsQuicTransport : IQuicTransport
{
    /// <summary>A ready-to-use shared instance; the backend holds no per-instance state.</summary>
    public static MsQuicTransport Shared { get; } = new();

    /// <inheritdoc />
    public bool IsSupported => QuicListener.IsSupported && QuicConnection.IsSupported;

    /// <inheritdoc />
    public async ValueTask<IQuicServer> ListenAsync(QuicServerOptions options,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(options);
        ThrowIfUnsupported();

        var protocols = new List<SslApplicationProtocol>(options.ApplicationProtocols);
        var listenerOptions = new QuicListenerOptions
        {
            ListenEndPoint = options.ListenEndPoint,
            ApplicationProtocols = protocols,
            ConnectionOptionsCallback = (_, _, _) => ValueTask.FromResult(new QuicServerConnectionOptions
            {
                DefaultStreamErrorCode = 0,
                DefaultCloseErrorCode = 0,
                IdleTimeout = options.IdleTimeout,
                MaxInboundUnidirectionalStreams = options.MaxConcurrentInboundStreams,
                MaxInboundBidirectionalStreams = options.MaxConcurrentInboundStreams,
                ServerAuthenticationOptions = new SslServerAuthenticationOptions
                {
                    ApplicationProtocols = protocols,
                    ServerCertificate = options.ServerCertificate,
                },
            }),
        };

        QuicListener listener = await QuicListener.ListenAsync(listenerOptions, cancellationToken)
            .ConfigureAwait(false);
        return new MsQuicServer(listener);
    }

    /// <inheritdoc />
    public async ValueTask<IQuicConnection> ConnectAsync(QuicClientOptions options,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(options);
        ThrowIfUnsupported();

        var clientAuthentication = new SslClientAuthenticationOptions
        {
            ApplicationProtocols = new List<SslApplicationProtocol>(options.ApplicationProtocols),
            TargetHost = options.TargetHost
                ?? (options.RemoteEndPoint as IPEndPoint)?.Address.ToString()
                ?? string.Empty,
        };
        if (options.AllowUntrustedCertificates)
        {
            // Dev servers and interop harnesses present self-signed certs; opt-in only.
#pragma warning disable CA5359
            clientAuthentication.RemoteCertificateValidationCallback = static (_, _, _, _) => true;
#pragma warning restore CA5359
        }

        var connectionOptions = new QuicClientConnectionOptions
        {
            RemoteEndPoint = options.RemoteEndPoint,
            DefaultStreamErrorCode = 0,
            DefaultCloseErrorCode = 0,
            IdleTimeout = options.IdleTimeout,
            KeepAliveInterval = options.KeepAliveInterval ?? Timeout.InfiniteTimeSpan,
            MaxInboundUnidirectionalStreams = options.MaxConcurrentInboundStreams,
            MaxInboundBidirectionalStreams = options.MaxConcurrentInboundStreams,
            ClientAuthenticationOptions = clientAuthentication,
        };

        QuicConnection connection = await QuicConnection.ConnectAsync(connectionOptions, cancellationToken)
            .ConfigureAwait(false);
        return new MsQuicConnection(connection);
    }

    private void ThrowIfUnsupported()
    {
        if (!IsSupported)
        {
            throw new PlatformNotSupportedException(
                "QUIC is not supported here: native msquic must be present and the OS must provide "
                + "the dual-mode sockets System.Net.Quic requires (an IPv6 stack). "
                + "Use InMemoryQuicTransport for tests and single-process loopback.");
        }
    }
}

internal sealed class MsQuicServer : IQuicServer
{
    private readonly QuicListener _listener;

    public MsQuicServer(QuicListener listener) => _listener = listener;

    public EndPoint LocalEndPoint => _listener.LocalEndPoint;

    public async ValueTask<IQuicConnection> AcceptConnectionAsync(CancellationToken cancellationToken = default)
    {
        QuicConnection connection = await _listener.AcceptConnectionAsync(cancellationToken).ConfigureAwait(false);
        return new MsQuicConnection(connection);
    }

    public ValueTask DisposeAsync() => _listener.DisposeAsync();
}

internal sealed class MsQuicConnection : IQuicConnection
{
    private readonly QuicConnection _connection;

    public MsQuicConnection(QuicConnection connection) => _connection = connection;

    public EndPoint RemoteEndPoint => _connection.RemoteEndPoint;

    public SslApplicationProtocol NegotiatedApplicationProtocol => _connection.NegotiatedApplicationProtocol;

    public async ValueTask<IQuicStream> OpenStreamAsync(QuicStreamDirection direction,
        CancellationToken cancellationToken = default)
    {
        QuicStreamType type = direction == QuicStreamDirection.Bidirectional
            ? QuicStreamType.Bidirectional
            : QuicStreamType.Unidirectional;
        QuicStream stream = await _connection.OpenOutboundStreamAsync(type, cancellationToken).ConfigureAwait(false);
        return new MsQuicStream(stream);
    }

    public async ValueTask<IQuicStream> AcceptStreamAsync(CancellationToken cancellationToken = default)
    {
        QuicStream stream = await _connection.AcceptInboundStreamAsync(cancellationToken).ConfigureAwait(false);
        return new MsQuicStream(stream);
    }

    public ValueTask CloseAsync(long errorCode, CancellationToken cancellationToken = default) =>
        _connection.CloseAsync(errorCode, cancellationToken);

    public ValueTask DisposeAsync() => _connection.DisposeAsync();
}

internal sealed class MsQuicStream : IQuicStream
{
    private readonly QuicStream _stream;

    public MsQuicStream(QuicStream stream) => _stream = stream;

    public long Id => _stream.Id;

    public QuicStreamDirection Direction => _stream.Type == QuicStreamType.Bidirectional
        ? QuicStreamDirection.Bidirectional
        : QuicStreamDirection.Unidirectional;

    public bool CanRead => _stream.CanRead;

    public bool CanWrite => _stream.CanWrite;

    public ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default) =>
        _stream.ReadAsync(buffer, cancellationToken);

    public ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, bool completeWrites = false,
        CancellationToken cancellationToken = default) =>
        _stream.WriteAsync(buffer, completeWrites, cancellationToken);

    public void CompleteWrites() => _stream.CompleteWrites();

    public void Abort(long errorCode)
    {
        if (_stream.CanRead)
        {
            _stream.Abort(QuicAbortDirection.Read, errorCode);
        }

        if (_stream.CanWrite)
        {
            _stream.Abort(QuicAbortDirection.Write, errorCode);
        }
    }

    public ValueTask DisposeAsync() => _stream.DisposeAsync();
}
