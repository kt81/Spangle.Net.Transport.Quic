namespace Spangle.Net.Moqt.Data;

/// <summary>
/// Bounds on what a data stream reader accepts from a peer. The wire format allows length
/// prefixes up to 2^62-1, and a reader that believed one would allocate whatever the peer
/// declares — before a single payload byte arrives, so QUIC flow control never gets a say.
/// The defaults sit far above any real media object while keeping a lying length from
/// becoming an allocation; a deployment expecting bigger objects raises them explicitly.
/// </summary>
public sealed record MoqReadLimits
{
    /// <summary>The limits used when a reader is not given any.</summary>
    public static MoqReadLimits Default { get; } = new();

    /// <summary>Largest object payload accepted off a data stream, in bytes.</summary>
    public int MaxObjectPayloadLength { get; init; } = 16 * 1024 * 1024;

    /// <summary>Largest Object Properties (extension headers) block accepted, in bytes.</summary>
    public int MaxPropertiesLength { get; init; } = 64 * 1024;
}
