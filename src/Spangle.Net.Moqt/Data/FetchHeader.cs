using System.Buffers;
using Spangle.Net.Moqt.Wire;
using Spangle.Net.Transport.Quic;

namespace Spangle.Net.Moqt.Data;

/// <summary>
/// The FETCH_HEADER that opens a fetch data stream (draft-18 §11.4.4). Unlike a subgroup stream,
/// which names its track and group up front, a fetch stream names only the request it answers —
/// every object on it belongs to the track that request asked for, and each object carries its
/// own Group and Object id (delta-encoded against the one before it).
/// </summary>
public readonly struct FetchHeader
{
    /// <summary>The unidirectional stream type that selects this header (draft-18 §3.4, Table 3).</summary>
    public const ulong StreamType = 0x5;

    /// <summary>Creates a header answering <paramref name="requestId"/>.</summary>
    public FetchHeader(ulong requestId) => RequestId = requestId;

    /// <summary>The Request ID of the FETCH this stream answers.</summary>
    public ulong RequestId { get; }

    /// <summary>Serializes the header into <paramref name="output"/>.</summary>
    public void WriteTo(IBufferWriter<byte> output)
    {
        ArgumentNullException.ThrowIfNull(output);
        var writer = new MoqWriter(output);
        writer.WriteVarInt(StreamType);
        writer.WriteVarInt(RequestId);
    }

    /// <summary>Reads a header off the start of a fetch stream.</summary>
    public static async ValueTask<FetchHeader> ReadAsync(IQuicStream stream,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(stream);

        ulong type = await StreamIo.ReadVarIntAsync(stream, cancellationToken).ConfigureAwait(false);
        if (type != StreamType)
        {
            throw new MoqProtocolException($"Expected a FETCH_HEADER (0x{StreamType:X}), got stream type 0x{type:X}.");
        }

        return await ReadAfterTypeAsync(stream, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Reads the header's body, for a caller that has already consumed the stream type.</summary>
    internal static async ValueTask<FetchHeader> ReadAfterTypeAsync(IQuicStream stream,
        CancellationToken cancellationToken)
    {
        ulong requestId = await StreamIo.ReadVarIntAsync(stream, cancellationToken).ConfigureAwait(false);
        return new FetchHeader(requestId);
    }
}
