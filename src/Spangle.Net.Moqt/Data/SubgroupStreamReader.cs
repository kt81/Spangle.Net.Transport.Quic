using Spangle.Net.Moqt.Wire;
using Spangle.Net.Transport.Quic;

namespace Spangle.Net.Moqt.Data;

/// <summary>
/// Reads a subgroup data stream (draft-18 §11.4.2): the SUBGROUP_HEADER first (via
/// <see cref="OpenAsync"/>), then objects until the stream FINs. Object IDs are
/// reconstructed from their deltas, and the Subgroup ID is resolved per the header's mode.
/// </summary>
public sealed class SubgroupStreamReader
{
    private readonly IQuicStream _stream;
    private ulong _previousObjectId;
    private ulong _resolvedSubgroupId;
    private bool _first = true;

    private SubgroupStreamReader(IQuicStream stream, SubgroupHeader header)
    {
        _stream = stream;
        Header = header;
    }

    /// <summary>The SUBGROUP_HEADER read from the front of the stream.</summary>
    public SubgroupHeader Header { get; }

    /// <summary>Reads the header and returns a reader positioned at the first object.</summary>
    public static async ValueTask<SubgroupStreamReader> OpenAsync(IQuicStream stream,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(stream);
        SubgroupHeader header = await SubgroupHeader.ReadAsync(stream, cancellationToken).ConfigureAwait(false);
        return new SubgroupStreamReader(stream, header);
    }

    /// <summary>Wraps a stream whose SUBGROUP_HEADER a caller has already read.</summary>
    internal static SubgroupStreamReader Create(IQuicStream stream, SubgroupHeader header) => new(stream, header);

    /// <summary>Reads the next object, or null once the stream has FIN'd at an object boundary.</summary>
    public async ValueTask<MoqObject?> ReadObjectAsync(CancellationToken cancellationToken = default)
    {
        int firstByte = await StreamIo.TryReadByteAsync(_stream, cancellationToken).ConfigureAwait(false);
        if (firstByte < 0)
        {
            return null; // clean end of the subgroup
        }

        ulong delta = await StreamIo.ReadVarIntAsync(_stream, firstByte, cancellationToken).ConfigureAwait(false);
        ulong objectId = _first ? delta : _previousObjectId + delta + 1;
        _previousObjectId = objectId;

        IReadOnlyList<MoqKeyValuePair> extensions = [];
        if (Header.HasProperties)
        {
            // Object Extension Headers: a byte-length-prefixed block of Key-Value-Pairs.
            ulong propertiesLength = await StreamIo.ReadVarIntAsync(_stream, cancellationToken).ConfigureAwait(false);
            if (propertiesLength > 0)
            {
                byte[] block = await StreamIo.ReadExactAsync(_stream, ToLength(propertiesLength), cancellationToken)
                    .ConfigureAwait(false);
                var reader = new MoqReader(block);
                extensions = KeyValuePairCodec.ReadList(ref reader);
            }
        }

        ulong payloadLength = await StreamIo.ReadVarIntAsync(_stream, cancellationToken).ConfigureAwait(false);
        MoqObjectStatus status;
        ReadOnlyMemory<byte> payload;
        if (payloadLength == 0)
        {
            ulong raw = await StreamIo.ReadVarIntAsync(_stream, cancellationToken).ConfigureAwait(false);
            status = raw switch
            {
                (ulong)MoqObjectStatus.Normal => MoqObjectStatus.Normal,
                (ulong)MoqObjectStatus.EndOfGroup => MoqObjectStatus.EndOfGroup,
                (ulong)MoqObjectStatus.EndOfTrack => MoqObjectStatus.EndOfTrack,
                _ => throw new MoqProtocolException($"0x{raw:X} is not a valid Object Status."),
            };
            payload = ReadOnlyMemory<byte>.Empty;
        }
        else
        {
            status = MoqObjectStatus.Normal;
            payload = await StreamIo.ReadExactAsync(_stream, ToLength(payloadLength), cancellationToken)
                .ConfigureAwait(false);
        }

        if (_first)
        {
            _resolvedSubgroupId = Header.SubgroupIdMode switch
            {
                SubgroupIdMode.Explicit => Header.SubgroupId,
                SubgroupIdMode.FirstObjectId => objectId,
                _ => 0,
            };
            _first = false;
        }

        return new MoqObject(Header.GroupId, objectId, _resolvedSubgroupId, Header.PublisherPriority, status, payload,
            extensions);
    }

    private static int ToLength(ulong value)
    {
        if (value > int.MaxValue)
        {
            throw new MoqProtocolException($"Length {value} exceeds the supported maximum.");
        }

        return (int)value;
    }
}
