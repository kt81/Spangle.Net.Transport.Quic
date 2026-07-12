using System.Net;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;

namespace Spangle.Net.Transport.Quic;

/// <summary>
/// The seam between Spangle's QUIC-based protocols (MoQ first) and a concrete QUIC
/// implementation. Two backends implement it: <c>MsQuicTransport</c> (the real thing,
/// System.Net.Quic over native msquic) and <c>InMemoryQuicTransport</c> (an in-process
/// loopback). Keeping the protocol code on this interface lets it run and be tested
/// where msquic — or the IPv6 stack System.Net.Quic requires for its dual-mode sockets —
/// is not present, and leaves room to drop to a direct-msquic backend later for the
/// features System.Net.Quic does not surface (QUIC datagrams, stream priority).
/// </summary>
public interface IQuicTransport
{
    /// <summary>Whether this backend can actually run in the current process/OS.</summary>
    bool IsSupported { get; }

    /// <summary>Starts accepting inbound QUIC connections.</summary>
    ValueTask<IQuicServer> ListenAsync(QuicServerOptions options, CancellationToken cancellationToken = default);

    /// <summary>Dials an outbound QUIC connection.</summary>
    ValueTask<IQuicConnection> ConnectAsync(QuicClientOptions options, CancellationToken cancellationToken = default);
}

/// <summary>A listening QUIC endpoint. One per bound port.</summary>
public interface IQuicServer : IAsyncDisposable
{
    /// <summary>The address the server is bound to (with the concrete port once bound).</summary>
    EndPoint LocalEndPoint { get; }

    /// <summary>Awaits the next fully established inbound connection.</summary>
    ValueTask<IQuicConnection> AcceptConnectionAsync(CancellationToken cancellationToken = default);
}

/// <summary>
/// One QUIC connection. Streams are the unit of delivery: MoQ opens a bidirectional
/// control stream and a unidirectional data stream per subgroup.
/// </summary>
public interface IQuicConnection : IAsyncDisposable
{
    /// <summary>The peer's address.</summary>
    EndPoint RemoteEndPoint { get; }

    /// <summary>The ALPN protocol negotiated in the TLS handshake.</summary>
    SslApplicationProtocol NegotiatedApplicationProtocol { get; }

    /// <summary>Opens an outbound stream. May await peer flow-control credit.</summary>
    ValueTask<IQuicStream> OpenStreamAsync(QuicStreamDirection direction,
        CancellationToken cancellationToken = default);

    /// <summary>Awaits the next stream the peer opened.</summary>
    ValueTask<IQuicStream> AcceptStreamAsync(CancellationToken cancellationToken = default);

    /// <summary>Closes the connection with an application error code (0 = normal).</summary>
    ValueTask CloseAsync(long errorCode, CancellationToken cancellationToken = default);
}

/// <summary>
/// A single QUIC stream, modeled as a byte channel. A unidirectional stream is
/// write-only for the opener and read-only for the acceptor; a bidirectional stream
/// is both. <see cref="CompleteWrites"/> is the graceful half-close the peer sees as
/// end-of-stream; <see cref="Abort"/> is an abrupt reset.
/// </summary>
public interface IQuicStream : IAsyncDisposable
{
    /// <summary>The QUIC stream id.</summary>
    long Id { get; }

    /// <summary>Whether the stream was opened unidirectional or bidirectional.</summary>
    QuicStreamDirection Direction { get; }

    /// <summary>False on the send-only half of a unidirectional stream.</summary>
    bool CanRead { get; }

    /// <summary>False on the receive-only half of a unidirectional stream.</summary>
    bool CanWrite { get; }

    /// <summary>Reads the next bytes; returns 0 once the peer has completed its writes.</summary>
    ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default);

    /// <summary>Writes bytes; set <paramref name="completeWrites"/> to half-close after this write.</summary>
    ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, bool completeWrites = false,
        CancellationToken cancellationToken = default);

    /// <summary>Gracefully ends the send side; the peer reads end-of-stream.</summary>
    void CompleteWrites();

    /// <summary>Abruptly resets the stream with an application error code.</summary>
    void Abort(long errorCode);
}

/// <summary>Whether a stream carries data one way or both.</summary>
public enum QuicStreamDirection
{
    /// <summary>Opener writes, acceptor reads. MoQ subgroup/data streams are these.</summary>
    Unidirectional,

    /// <summary>Both peers read and write. MoQ's control stream is one.</summary>
    Bidirectional,
}

/// <summary>Everything a backend needs to bind a listening endpoint.</summary>
public sealed record QuicServerOptions
{
    /// <summary>The address and port to bind (port 0 lets the OS choose).</summary>
    public required IPEndPoint ListenEndPoint { get; init; }

    /// <summary>ALPN protocols to offer; a MoQ server offers its draft's token.</summary>
    public required IReadOnlyList<SslApplicationProtocol> ApplicationProtocols { get; init; }

    /// <summary>The server certificate presented in the TLS handshake (required for msquic).</summary>
    public X509Certificate2? ServerCertificate { get; init; }

    /// <summary>How long an idle connection is kept before QUIC closes it.</summary>
    public TimeSpan IdleTimeout { get; init; } = TimeSpan.FromSeconds(30);
}

/// <summary>Everything a backend needs to dial a peer.</summary>
public sealed record QuicClientOptions
{
    /// <summary>The peer to dial.</summary>
    public required EndPoint RemoteEndPoint { get; init; }

    /// <summary>ALPN protocols to offer; must overlap the server's.</summary>
    public required IReadOnlyList<SslApplicationProtocol> ApplicationProtocols { get; init; }

    /// <summary>The SNI host name; defaults to the remote address when null.</summary>
    public string? TargetHost { get; init; }

    /// <summary>Accept any server certificate (self-signed dev servers, interop tests).</summary>
    public bool AllowUntrustedCertificates { get; init; }

    /// <summary>How long an idle connection is kept before QUIC closes it.</summary>
    public TimeSpan IdleTimeout { get; init; } = TimeSpan.FromSeconds(30);
}
