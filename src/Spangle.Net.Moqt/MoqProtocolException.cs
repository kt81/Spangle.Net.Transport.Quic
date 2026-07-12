namespace Spangle.Net.Moqt;

/// <summary>Thrown when a MOQT wire message is malformed, truncated, or violates the protocol.</summary>
public sealed class MoqProtocolException : Exception
{
    /// <summary>Creates an exception with no message.</summary>
    public MoqProtocolException()
    {
    }

    /// <summary>Creates an exception with the given message.</summary>
    public MoqProtocolException(string message)
        : base(message)
    {
    }

    /// <summary>Creates an exception with the given message and inner exception.</summary>
    public MoqProtocolException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
