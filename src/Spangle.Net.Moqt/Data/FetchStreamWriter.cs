using System.Buffers;
using Spangle.Net.Moqt.Wire;
using Spangle.Net.Transport.Quic;

namespace Spangle.Net.Moqt.Data;

/// <summary>
/// Writes a fetch data stream (draft-18 §11.4.4): the FETCH_HEADER once, then each object as a
/// Serialization Flags varint and only the fields the flags say are there. Where a subgroup
/// stream hoists the track, group and priority into its header, a fetch stream repeats nothing —
/// each object is coded against the one before it, and the flags say which fields that lets it
/// drop. The header is emitted lazily with the first entry (or on <see cref="CompleteAsync"/>),
/// which FINs the stream.
/// <para>
/// A FETCH has no Object Status field, so this writer rejects a non-Normal object: a publisher
/// says a stretch of the range has nothing to serve with <see cref="WriteEndOfRangeAsync"/>.
/// </para>
/// </summary>
public sealed class FetchStreamWriter
{
    private readonly IQuicStream _stream;
    private readonly FetchHeader _header;
    private readonly MoqGroupOrder _groupOrder;
    private bool _headerWritten;

    // The prior Group and Object id advance on every entry, an End of Range marker included, but
    // the prior Subgroup and Priority come from the last real object — a marker carries neither
    // (§11.4.4.2). Null means nothing has been written yet, which is what makes an entry "first".
    private ulong? _priorGroupId;
    private ulong? _priorObjectId;
    private ulong? _priorSubgroupId;
    private byte? _priorPriority;

    /// <summary>
    /// Creates a writer that emits <paramref name="header"/> before the first entry.
    /// <paramref name="groupOrder"/> must match the order the fetch was answered with, since it
    /// decides whether a Group ID Delta counts up or down.
    /// </summary>
    public FetchStreamWriter(IQuicStream stream, FetchHeader header,
        MoqGroupOrder groupOrder = MoqGroupOrder.Ascending)
    {
        ArgumentNullException.ThrowIfNull(stream);
        _stream = stream;
        _header = header;
        _groupOrder = groupOrder;
    }

    /// <summary>Appends one object to the fetch stream.</summary>
    public async ValueTask WriteObjectAsync(MoqObject moqObject, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(moqObject);
        if (moqObject.Status != MoqObjectStatus.Normal)
        {
            throw new ArgumentException(
                "A FETCH response carries no Object Status; report a range with nothing to serve " +
                "via WriteEndOfRangeAsync instead.", nameof(moqObject));
        }

        bool first = _priorGroupId is null;
        bool isDatagram = moqObject.Forwarding == MoqForwardingPreference.Datagram;

        ulong? groupIdDelta = ComputeGroupIdDelta(moqObject.GroupId, first);
        ulong? objectIdDelta = ComputeObjectIdDelta(moqObject.ObjectId, first, groupIdDelta.HasValue);
        FetchSubgroupMode subgroupMode = ChooseSubgroupMode(moqObject.SubgroupId, isDatagram);
        bool hasPriority = _priorPriority is null || moqObject.PublisherPriority != _priorPriority.Value;
        bool hasProperties = moqObject.Extensions.Count > 0;

        FetchObjectFlags flags = FetchObjectFlags.Compose(subgroupMode, objectIdDelta.HasValue,
            groupIdDelta.HasValue, hasPriority, hasProperties, isDatagram);

        var buffer = new ArrayBufferWriter<byte>();
        WriteHeaderIfPending(buffer);

        var writer = new MoqWriter(buffer);
        writer.WriteVarInt(flags.Value);
        if (groupIdDelta is { } group)
        {
            writer.WriteVarInt(group);
        }

        if (subgroupMode == FetchSubgroupMode.Explicit && !isDatagram)
        {
            writer.WriteVarInt(moqObject.SubgroupId);
        }

        if (objectIdDelta is { } objectId)
        {
            writer.WriteVarInt(objectId);
        }

        if (hasPriority)
        {
            writer.WriteByte(moqObject.PublisherPriority);
        }

        if (hasProperties)
        {
            // Object Properties: a byte-length-prefixed block of Key-Value-Pairs whose types are
            // delta-encoded, so the block must be in non-decreasing type order.
            var properties = new ArrayBufferWriter<byte>();
            KeyValuePairCodec.WriteList(new MoqWriter(properties), [.. moqObject.Extensions.OrderBy(p => p.Type)]);
            writer.WriteVarInt((ulong)properties.WrittenCount);
            buffer.Write(properties.WrittenSpan);
        }

        // The payload length closes the object. No status follows a zero length here: unlike a
        // subgroup stream, a FETCH has no Object Status field at all (§11.2.1.1).
        writer.WriteVarInt((ulong)moqObject.Payload.Length);
        if (!moqObject.Payload.IsEmpty)
        {
            buffer.Write(moqObject.Payload.Span);
        }

        _priorGroupId = moqObject.GroupId;
        _priorObjectId = moqObject.ObjectId;
        _priorPriority = moqObject.PublisherPriority;
        if (!isDatagram)
        {
            _priorSubgroupId = moqObject.SubgroupId;
        }

        await _stream.WriteAsync(buffer.WrittenMemory, completeWrites: false, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Appends an End of Range marker (draft-18 §11.4.4.2): every Location between the previous
    /// entry and <paramref name="location"/>, inclusive, has nothing to serve.
    /// </summary>
    public async ValueTask WriteEndOfRangeAsync(MoqLocation location, MoqFetchRangeKind kind,
        CancellationToken cancellationToken = default)
    {
        var buffer = new ArrayBufferWriter<byte>();
        WriteHeaderIfPending(buffer);

        // A marker is three fields and stops: flags, then an absolute Group and Object id. No
        // subgroup, priority, properties, or even a payload length follows.
        var writer = new MoqWriter(buffer);
        writer.WriteVarInt(kind == MoqFetchRangeKind.Unknown
            ? FetchObjectFlags.EndOfUnknownRange
            : FetchObjectFlags.EndOfNonExistentRange);
        writer.WriteVarInt(location.Group);
        writer.WriteVarInt(location.ObjectId);

        // A marker moves the Location an object may code against, but leaves the prior Subgroup
        // and Priority with the last real object, which is what a later object still references.
        _priorGroupId = location.Group;
        _priorObjectId = location.ObjectId;

        await _stream.WriteAsync(buffer.WrittenMemory, completeWrites: false, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Flushes the header if no entries were written, then FINs the stream.</summary>
    public async ValueTask CompleteAsync(CancellationToken cancellationToken = default)
    {
        if (!_headerWritten)
        {
            var buffer = new ArrayBufferWriter<byte>();
            WriteHeaderIfPending(buffer);
            await _stream.WriteAsync(buffer.WrittenMemory, completeWrites: false, cancellationToken)
                .ConfigureAwait(false);
        }

        _stream.CompleteWrites();
    }

    private void WriteHeaderIfPending(ArrayBufferWriter<byte> buffer)
    {
        if (_headerWritten)
        {
            return;
        }

        _header.WriteTo(buffer);
        _headerWritten = true;
    }

    /// <summary>
    /// The Group ID Delta field, or null to let the reader carry the prior Group ID forward. The
    /// first object's is its absolute Group ID; afterwards it counts the groups skipped, in
    /// whichever direction the fetch's group order runs.
    /// </summary>
    private ulong? ComputeGroupIdDelta(ulong groupId, bool first)
    {
        if (first)
        {
            return groupId;
        }

        ulong prior = _priorGroupId!.Value;
        if (groupId == prior)
        {
            return null;
        }

        if (_groupOrder == MoqGroupOrder.Descending)
        {
            if (groupId > prior)
            {
                throw new ArgumentException(
                    $"Group {groupId} follows {prior} in a descending fetch, which requires each Group ID to fall.",
                    nameof(groupId));
            }

            return prior - groupId - 1;
        }

        if (groupId < prior)
        {
            throw new ArgumentException(
                $"Group {groupId} follows {prior} in an ascending fetch, which requires each Group ID to rise.",
                nameof(groupId));
        }

        return groupId - prior - 1;
    }

    /// <summary>
    /// The Object ID Delta field, or null to let the reader take the prior Object ID plus one.
    /// It is the absolute Object ID whenever a Group ID Delta rides along — the id restarts with
    /// the group — and otherwise counts from the prior object within the same group.
    /// </summary>
    private ulong? ComputeObjectIdDelta(ulong objectId, bool first, bool groupChanged)
    {
        if (first || groupChanged)
        {
            return objectId;
        }

        ulong prior = _priorObjectId!.Value;
        if (objectId <= prior)
        {
            throw new ArgumentException(
                $"Object {objectId} follows {prior} in the same group, which requires Object IDs to rise.",
                nameof(objectId));
        }

        return objectId == prior + 1 ? null : objectId - prior;
    }

    /// <summary>
    /// The cheapest encoding of the Subgroup ID. The prior-object modes are unavailable until a
    /// real object has been written — referencing a prior that is not there is a protocol
    /// violation — so the first object falls to an explicit field, or to zero when it is zero.
    /// </summary>
    private FetchSubgroupMode ChooseSubgroupMode(ulong subgroupId, bool isDatagram)
    {
        if (isDatagram || subgroupId == 0)
        {
            return FetchSubgroupMode.Zero;
        }

        if (_priorSubgroupId is not { } prior)
        {
            return FetchSubgroupMode.Explicit;
        }

        if (subgroupId == prior)
        {
            return FetchSubgroupMode.PriorSubgroup;
        }

        return subgroupId == prior + 1 ? FetchSubgroupMode.PriorSubgroupPlusOne : FetchSubgroupMode.Explicit;
    }
}
