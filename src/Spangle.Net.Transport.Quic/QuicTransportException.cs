namespace Spangle.Net.Transport.Quic;

/// <summary>Why a transport operation failed, backend-independently.</summary>
public enum QuicTransportError
{
    /// <summary>
    /// The dial failed: nothing is listening there, the host is unreachable, or the handshake
    /// (TLS, ALPN) was refused. <c>ConnectAsync</c> failures are always this.
    /// </summary>
    ConnectionRefused,

    /// <summary>The connection is gone: closed by the peer, idle-timed-out, or lost.</summary>
    ConnectionAborted,

    /// <summary>The peer reset this stream (or stopped reading it); the stream is over, the connection is not.</summary>
    StreamAborted,

    /// <summary>The operation was aborted on this side: the connection under it was closed locally.</summary>
    OperationAborted,
}

/// <summary>
/// The exception every transport-level failure surfaces as, whichever backend is underneath.
/// This is the abstraction's error contract: protocol code branches on <see cref="Error"/> —
/// "the stream died" versus "the connection died" is the difference between abandoning a group
/// and redialing a relay — and must not have to know msquic's exception types to do it.
/// Derives from <see cref="IOException"/> so code that treats any I/O failure alike keeps
/// working. (Cancellation still surfaces as <see cref="OperationCanceledException"/> and using
/// a disposed object as <see cref="ObjectDisposedException"/>, as everywhere in .NET.)
/// </summary>
public sealed class QuicTransportException : IOException
{
    /// <summary>Creates a transport failure with its backend-independent classification.</summary>
    public QuicTransportException(QuicTransportError error, string message, Exception? innerException = null)
        : base(message, innerException) =>
        Error = error;

    /// <summary>The exception convention's parameterless form; classified as ConnectionAborted.</summary>
    public QuicTransportException()
        : this(QuicTransportError.ConnectionAborted, "A QUIC transport operation failed.")
    {
    }

    /// <summary>The exception convention's message-only form; classified as ConnectionAborted.</summary>
    public QuicTransportException(string message)
        : this(QuicTransportError.ConnectionAborted, message)
    {
    }

    /// <summary>The exception convention's wrapping form; classified as ConnectionAborted.</summary>
    public QuicTransportException(string message, Exception innerException)
        : this(QuicTransportError.ConnectionAborted, message, innerException)
    {
    }

    /// <summary>What failed, in terms protocol code can branch on.</summary>
    public QuicTransportError Error { get; }
}
