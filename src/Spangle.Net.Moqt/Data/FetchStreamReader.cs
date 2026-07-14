using Spangle.Net.Moqt.Wire;
using Spangle.Net.Transport.Quic;

namespace Spangle.Net.Moqt.Data;

/// <summary>
/// Reads a fetch data stream (draft-18 §11.4.4): the FETCH_HEADER first (via
/// <see cref="OpenAsync"/>), then entries until the stream FINs. Each object's Serialization
/// Flags say which fields it carries and which it inherits from the object before it, so the
/// reader rebuilds every Group, Object and Subgroup id from that running state — and rejects a
/// stream that references a prior object it never sent.
/// </summary>
public sealed class FetchStreamReader
{
    private readonly IQuicStream _stream;
    private readonly MoqGroupOrder _groupOrder;

    // Mirrors FetchStreamWriter's state: Group and Object advance on every entry, an End of Range
    // marker included, while Subgroup and Priority stay with the last real object (§11.4.4.2).
    private ulong? _priorGroupId;
    private ulong? _priorObjectId;
    private ulong? _priorSubgroupId;
    private byte? _priorPriority;

    private FetchStreamReader(IQuicStream stream, FetchHeader header, MoqGroupOrder groupOrder)
    {
        _stream = stream;
        _groupOrder = groupOrder;
        Header = header;
    }

    /// <summary>The FETCH_HEADER read from the front of the stream.</summary>
    public FetchHeader Header { get; }

    /// <summary>
    /// Reads the header and returns a reader positioned at the first entry.
    /// <paramref name="groupOrder"/> must match the order the fetch was answered with, since it
    /// decides whether a Group ID Delta counts up or down.
    /// </summary>
    public static async ValueTask<FetchStreamReader> OpenAsync(IQuicStream stream,
        MoqGroupOrder groupOrder = MoqGroupOrder.Ascending, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(stream);
        FetchHeader header = await FetchHeader.ReadAsync(stream, cancellationToken).ConfigureAwait(false);
        return new FetchStreamReader(stream, header, groupOrder);
    }

    /// <summary>Wraps a stream whose FETCH_HEADER a caller has already read.</summary>
    internal static FetchStreamReader Create(IQuicStream stream, FetchHeader header, MoqGroupOrder groupOrder) =>
        new(stream, header, groupOrder);

    /// <summary>Reads the next entry, or null once the stream has FIN'd at an entry boundary.</summary>
    public async ValueTask<MoqFetchEntry?> ReadEntryAsync(CancellationToken cancellationToken = default)
    {
        int firstByte = await StreamIo.TryReadByteAsync(_stream, cancellationToken).ConfigureAwait(false);
        if (firstByte < 0)
        {
            return null; // clean end of the fetch response
        }

        ulong rawFlags = await StreamIo.ReadVarIntAsync(_stream, firstByte, cancellationToken).ConfigureAwait(false);
        if (rawFlags is FetchObjectFlags.EndOfNonExistentRange or FetchObjectFlags.EndOfUnknownRange)
        {
            return await ReadEndOfRangeAsync(rawFlags, cancellationToken).ConfigureAwait(false);
        }

        FetchObjectFlags flags = FetchObjectFlags.Parse(rawFlags);
        bool first = _priorGroupId is null;
        if (first && (!flags.HasGroupIdDelta || !flags.HasObjectIdDelta))
        {
            throw new MoqProtocolException(
                "The first object on a FETCH stream must carry both a Group ID Delta and an Object ID Delta.");
        }

        // The fields arrive in exactly this order (Figure 27), and the Subgroup ID sits between the
        // two id deltas — so each read has to happen in sequence, not where it reads most naturally.
        ulong? groupIdField = flags.HasGroupIdDelta
            ? await StreamIo.ReadVarIntAsync(_stream, cancellationToken).ConfigureAwait(false)
            : null;
        ulong groupId = ResolveGroupId(groupIdField);

        ulong subgroupId = await ResolveSubgroupIdAsync(flags, cancellationToken).ConfigureAwait(false);

        ulong? objectIdField = flags.HasObjectIdDelta
            ? await StreamIo.ReadVarIntAsync(_stream, cancellationToken).ConfigureAwait(false)
            : null;
        ulong objectId = ResolveObjectId(objectIdField, flags.HasGroupIdDelta);

        byte priority;
        if (flags.HasPriority)
        {
            int raw = await StreamIo.TryReadByteAsync(_stream, cancellationToken).ConfigureAwait(false);
            if (raw < 0)
            {
                throw new MoqProtocolException("The fetch stream ended before the Publisher Priority.");
            }

            priority = (byte)raw;
        }
        else
        {
            priority = _priorPriority
                       ?? throw new MoqProtocolException("The first object on a FETCH stream must set its Priority.");
        }

        IReadOnlyList<MoqKeyValuePair> extensions = [];
        if (flags.HasProperties)
        {
            ulong propertiesLength = await StreamIo.ReadVarIntAsync(_stream, cancellationToken).ConfigureAwait(false);
            if (propertiesLength > 0)
            {
                byte[] block = await StreamIo.ReadExactAsync(_stream, ToLength(propertiesLength), cancellationToken)
                    .ConfigureAwait(false);
                var reader = new MoqReader(block);
                extensions = KeyValuePairCodec.ReadList(ref reader);
            }
        }

        // The payload length closes the object, and nothing follows a zero one: a FETCH has no
        // Object Status field (§11.2.1.1), so a zero-length object here is simply empty.
        ulong payloadLength = await StreamIo.ReadVarIntAsync(_stream, cancellationToken).ConfigureAwait(false);
        ReadOnlyMemory<byte> payload = payloadLength == 0
            ? ReadOnlyMemory<byte>.Empty
            : await StreamIo.ReadExactAsync(_stream, ToLength(payloadLength), cancellationToken).ConfigureAwait(false);

        _priorGroupId = groupId;
        _priorObjectId = objectId;
        _priorPriority = priority;
        if (!flags.IsDatagram)
        {
            _priorSubgroupId = subgroupId;
        }

        return new MoqFetchedObject(new MoqObject(groupId, objectId, subgroupId, priority, MoqObjectStatus.Normal,
            payload, extensions,
            flags.IsDatagram ? MoqForwardingPreference.Datagram : MoqForwardingPreference.Subgroup));
    }

    private async ValueTask<MoqFetchEntry> ReadEndOfRangeAsync(ulong rawFlags, CancellationToken cancellationToken)
    {
        // A marker carries an absolute Group and Object id — not deltas — and stops there.
        ulong group = await StreamIo.ReadVarIntAsync(_stream, cancellationToken).ConfigureAwait(false);
        ulong objectId = await StreamIo.ReadVarIntAsync(_stream, cancellationToken).ConfigureAwait(false);

        _priorGroupId = group;
        _priorObjectId = objectId;

        return new MoqFetchEndOfRange(new MoqLocation(group, objectId),
            rawFlags == FetchObjectFlags.EndOfUnknownRange ? MoqFetchRangeKind.Unknown : MoqFetchRangeKind.NonExistent);
    }

    /// <summary>
    /// The Group ID: absent field means the prior object's group, the first object's delta is its
    /// absolute id, and otherwise the delta counts groups skipped in the fetch's group order.
    /// </summary>
    private ulong ResolveGroupId(ulong? groupIdField)
    {
        if (groupIdField is not { } delta)
        {
            return _priorGroupId
                   ?? throw new MoqProtocolException("The first object on a FETCH stream must set its Group ID.");
        }

        if (_priorGroupId is not { } prior)
        {
            return delta;
        }

        if (delta == ulong.MaxValue)
        {
            throw new MoqProtocolException("The Group ID Delta overflows.");
        }

        if (_groupOrder == MoqGroupOrder.Descending)
        {
            if (prior < delta + 1)
            {
                throw new MoqProtocolException("The Group ID Delta falls below zero.");
            }

            return prior - (delta + 1);
        }

        if (prior > ulong.MaxValue - delta - 1)
        {
            throw new MoqProtocolException("The Group ID Delta overflows.");
        }

        return prior + delta + 1;
    }

    /// <summary>
    /// The Object ID: the field is the absolute id whenever a Group ID Delta rode along (the id
    /// restarts with the group), otherwise it counts from the prior object; absent, the id is the
    /// prior one plus one — even across a group boundary.
    /// </summary>
    private ulong ResolveObjectId(ulong? objectIdField, bool groupIdDeltaPresent)
    {
        if (objectIdField is { } field)
        {
            if (groupIdDeltaPresent)
            {
                return field;
            }

            ulong prior = _priorObjectId
                          ?? throw new MoqProtocolException(
                              "The first object on a FETCH stream cannot reference a prior Object ID.");
            if (field > ulong.MaxValue - prior)
            {
                throw new MoqProtocolException("The Object ID Delta overflows.");
            }

            return prior + field;
        }

        ulong previous = _priorObjectId
                         ?? throw new MoqProtocolException("The first object on a FETCH stream must set its Object ID.");
        if (previous == ulong.MaxValue)
        {
            throw new MoqProtocolException("The Object ID overflows.");
        }

        return previous + 1;
    }

    /// <summary>
    /// The Subgroup ID, per the flags' two low bits — reading the explicit field when they call
    /// for it. A datagram object has none, and the spec has the reader ignore the bits entirely.
    /// </summary>
    private async ValueTask<ulong> ResolveSubgroupIdAsync(FetchObjectFlags flags, CancellationToken cancellationToken)
    {
        if (flags.IsDatagram)
        {
            return 0;
        }

        switch (flags.SubgroupMode)
        {
            case FetchSubgroupMode.Zero:
                return 0;

            case FetchSubgroupMode.PriorSubgroup:
                return _priorSubgroupId
                       ?? throw new MoqProtocolException(
                           "The first object on a FETCH stream cannot reference a prior Subgroup ID.");

            case FetchSubgroupMode.PriorSubgroupPlusOne:
                ulong prior = _priorSubgroupId
                              ?? throw new MoqProtocolException(
                                  "The first object on a FETCH stream cannot reference a prior Subgroup ID.");
                if (prior == ulong.MaxValue)
                {
                    throw new MoqProtocolException("The Subgroup ID overflows.");
                }

                return prior + 1;

            default:
                return await StreamIo.ReadVarIntAsync(_stream, cancellationToken).ConfigureAwait(false);
        }
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
