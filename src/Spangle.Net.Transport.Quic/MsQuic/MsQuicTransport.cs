using System.Net;
using System.Net.Quic;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Authentication;

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
/// Every transport-level failure is surfaced as <see cref="QuicTransportException"/> — the
/// backend's own <see cref="QuicException"/> never escapes, so protocol code written against
/// <see cref="IQuicTransport"/> sees one error contract on every backend.
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
        if (options.ServerCertificate is null)
        {
            // Without a certificate the listener binds happily and then fails every handshake
            // with an opaque TLS alert on the client. Failing here names the actual mistake.
            throw new ArgumentException(
                "QuicServerOptions.ServerCertificate is required for the msquic backend: without one, "
                + "every handshake fails with an unexplained TLS alert on the client side.",
                nameof(options));
        }

        var protocols = new List<SslApplicationProtocol>(options.ApplicationProtocols);
        var listenerOptions = new QuicListenerOptions
        {
            ListenEndPoint = options.ListenEndPoint,
            ApplicationProtocols = protocols,
            ConnectionOptionsCallback = (_, _, _) => ValueTask.FromResult(new QuicServerConnectionOptions
            {
                DefaultStreamErrorCode = options.DefaultStreamErrorCode,
                DefaultCloseErrorCode = options.DefaultCloseErrorCode,
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
            // No fallback to the IP literal: RFC 6066 forbids IP addresses in SNI, and while
            // many servers shrug, some refuse the handshake over it. No TargetHost, no SNI.
            TargetHost = options.TargetHost ?? string.Empty,
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
            DefaultStreamErrorCode = options.DefaultStreamErrorCode,
            DefaultCloseErrorCode = options.DefaultCloseErrorCode,
            IdleTimeout = options.IdleTimeout,
            KeepAliveInterval = options.KeepAliveInterval ?? Timeout.InfiniteTimeSpan,
            MaxInboundUnidirectionalStreams = options.MaxConcurrentInboundStreams,
            MaxInboundBidirectionalStreams = options.MaxConcurrentInboundStreams,
            ClientAuthenticationOptions = clientAuthentication,
        };

        try
        {
            QuicConnection connection = await QuicConnection.ConnectAsync(connectionOptions, cancellationToken)
                .ConfigureAwait(false);
            return new MsQuicConnection(connection);
        }
        catch (QuicException e)
        {
            throw new QuicTransportException(QuicTransportError.ConnectionRefused,
                $"The dial to {options.RemoteEndPoint} failed: {e.Message}", e);
        }
        catch (SocketException e)
        {
            // An unreachable host surfaces from msquic as a raw SocketException (ICMP
            // unreachable on loopback, for one), not a QuicException.
            throw new QuicTransportException(QuicTransportError.ConnectionRefused,
                $"The dial to {options.RemoteEndPoint} failed: {e.Message}", e);
        }
        catch (AuthenticationException e)
        {
            throw new QuicTransportException(QuicTransportError.ConnectionRefused,
                $"The dial to {options.RemoteEndPoint} failed TLS authentication: {e.Message}", e);
        }
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

/// <summary>Maps System.Net.Quic's exception onto the backend-independent contract.</summary>
internal static class MsQuicErrors
{
    public static QuicTransportException Map(QuicException e) => e.QuicError switch
    {
        QuicError.ConnectionRefused or QuicError.ConnectionTimeout or QuicError.VersionNegotiationError
            or QuicError.AlpnInUse =>
            new QuicTransportException(QuicTransportError.ConnectionRefused, e.Message, e),
        QuicError.StreamAborted =>
            new QuicTransportException(QuicTransportError.StreamAborted, e.Message, e),
        QuicError.OperationAborted =>
            new QuicTransportException(QuicTransportError.OperationAborted, e.Message, e),
        // ConnectionAborted, ConnectionIdle, and anything unforeseen: the connection is gone.
        _ => new QuicTransportException(QuicTransportError.ConnectionAborted, e.Message, e),
    };
}

internal sealed class MsQuicServer : IQuicServer
{
    private readonly QuicListener _listener;

    public MsQuicServer(QuicListener listener) => _listener = listener;

    public EndPoint LocalEndPoint => _listener.LocalEndPoint;

    public async ValueTask<IQuicConnection> AcceptConnectionAsync(CancellationToken cancellationToken = default)
    {
        while (true)
        {
            try
            {
                QuicConnection connection = await _listener.AcceptConnectionAsync(cancellationToken)
                    .ConfigureAwait(false);
                return new MsQuicConnection(connection);
            }
            catch (QuicException e) when (e.QuicError != QuicError.OperationAborted)
            {
                // One connection's failed handshake — a TLS probe, an ALPN mismatch, a port
                // scanner's single packet — surfaces here in System.Net.Quic. It is that
                // stranger's problem, not the listener's; the loop keeps accepting everyone
                // else, per the IQuicServer contract.
            }
            catch (QuicException e)
            {
                // OperationAborted: the listener itself is going away underneath us.
                throw new ObjectDisposedException(nameof(MsQuicServer), e.Message);
            }
        }
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
        try
        {
            QuicStream stream = await _connection.OpenOutboundStreamAsync(type, cancellationToken)
                .ConfigureAwait(false);
            return new MsQuicStream(stream);
        }
        catch (QuicException e)
        {
            throw MsQuicErrors.Map(e);
        }
    }

    public async ValueTask<IQuicStream> AcceptStreamAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            QuicStream stream = await _connection.AcceptInboundStreamAsync(cancellationToken).ConfigureAwait(false);
            return new MsQuicStream(stream);
        }
        catch (QuicException e)
        {
            throw MsQuicErrors.Map(e);
        }
    }

    public async ValueTask CloseAsync(long errorCode, CancellationToken cancellationToken = default)
    {
        try
        {
            await _connection.CloseAsync(errorCode, cancellationToken).ConfigureAwait(false);
        }
        catch (QuicException e)
        {
            throw MsQuicErrors.Map(e);
        }
    }

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

    public async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        try
        {
            return await _stream.ReadAsync(buffer, cancellationToken).ConfigureAwait(false);
        }
        catch (QuicException e)
        {
            throw MsQuicErrors.Map(e);
        }
    }

    public async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, bool completeWrites = false,
        CancellationToken cancellationToken = default)
    {
        try
        {
            await _stream.WriteAsync(buffer, completeWrites, cancellationToken).ConfigureAwait(false);
        }
        catch (QuicException e)
        {
            throw MsQuicErrors.Map(e);
        }
    }

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
