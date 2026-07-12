using System.Buffers;
using Spangle.Net.Moqt.Wire;
using Spangle.Net.Transport.Quic;

namespace Spangle.Net.Moqt.Data;

/// <summary>How the Subgroup ID is conveyed in a <see cref="SubgroupHeader"/> (draft-18 §11.4.2).</summary>
public enum SubgroupIdMode
{
    /// <summary>0b00 — the Subgroup ID field is absent and the Subgroup ID is 0.</summary>
    Zero = 0,

    /// <summary>0b01 — absent; the Subgroup ID is the Object ID of the first object.</summary>
    FirstObjectId = 1,

    /// <summary>0b10 — the Subgroup ID is present in the header.</summary>
    Explicit = 2,
}

/// <summary>
/// The SUBGROUP_HEADER that opens a subgroup data stream (draft-18 §11.4.2). Its type is a
/// varint whose low bits select which fields follow: the PROPERTIES bit, the two-bit
/// SUBGROUP_ID_MODE, END_OF_GROUP, DEFAULT_PRIORITY (priority omitted, inherited), and
/// FIRST_OBJECT. Bit 4 (0x10) is always set.
/// </summary>
public readonly struct SubgroupHeader
{
    private const ulong TypeBase = 0x10;
    private const ulong PropertiesBit = 0x01;
    private const ulong SubgroupIdModeMask = 0x06;
    private const ulong EndOfGroupBit = 0x08;
    private const ulong TypeRangeBit = 0x10;
    private const ulong DefaultPriorityBit = 0x20;
    private const ulong FirstObjectBit = 0x40;

    /// <summary>The track this subgroup's objects belong to.</summary>
    public ulong TrackAlias { get; init; }

    /// <summary>The Group these objects belong to.</summary>
    public ulong GroupId { get; init; }

    /// <summary>How the Subgroup ID is conveyed.</summary>
    public SubgroupIdMode SubgroupIdMode { get; init; }

    /// <summary>The Subgroup ID (meaningful when <see cref="SubgroupIdMode"/> is Explicit).</summary>
    public ulong SubgroupId { get; init; }

    /// <summary>Whether every object on the stream carries a Properties field.</summary>
    public bool HasProperties { get; init; }

    /// <summary>Whether this subgroup contains the largest Object in the Group.</summary>
    public bool EndOfGroup { get; init; }

    /// <summary>Whether the first object here is the first published in the subgroup.</summary>
    public bool FirstObject { get; init; }

    /// <summary>Whether the priority is omitted and inherited from the subscription.</summary>
    public bool InheritPriority { get; init; }

    /// <summary>The publisher priority (present unless <see cref="InheritPriority"/>).</summary>
    public byte PublisherPriority { get; init; }

    /// <summary>The varint type value that encodes this header's field layout.</summary>
    public ulong Type =>
        TypeBase
        | (HasProperties ? PropertiesBit : 0)
        | ((ulong)SubgroupIdMode << 1)
        | (EndOfGroup ? EndOfGroupBit : 0)
        | (InheritPriority ? DefaultPriorityBit : 0)
        | (FirstObject ? FirstObjectBit : 0);

    /// <summary>Serializes the header into <paramref name="output"/>.</summary>
    public void WriteTo(IBufferWriter<byte> output)
    {
        ArgumentNullException.ThrowIfNull(output);
        var writer = new MoqWriter(output);
        writer.WriteVarInt(Type);
        writer.WriteVarInt(TrackAlias);
        writer.WriteVarInt(GroupId);
        if (SubgroupIdMode == SubgroupIdMode.Explicit)
        {
            writer.WriteVarInt(SubgroupId);
        }

        if (!InheritPriority)
        {
            Span<byte> priority = output.GetSpan(1);
            priority[0] = PublisherPriority;
            output.Advance(1);
        }
    }

    /// <summary>Reads a header off the start of a subgroup stream.</summary>
    public static async ValueTask<SubgroupHeader> ReadAsync(IQuicStream stream,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(stream);

        ulong type = await StreamIo.ReadVarIntAsync(stream, cancellationToken).ConfigureAwait(false);
        if ((type & TypeRangeBit) == 0 || type > 0x7F)
        {
            throw new MoqProtocolException($"0x{type:X} is not a valid SUBGROUP_HEADER type.");
        }

        var mode = (SubgroupIdMode)((type & SubgroupIdModeMask) >> 1);
        if ((int)mode == 3)
        {
            throw new MoqProtocolException("SUBGROUP_ID_MODE 0b11 is reserved.");
        }

        ulong trackAlias = await StreamIo.ReadVarIntAsync(stream, cancellationToken).ConfigureAwait(false);
        ulong groupId = await StreamIo.ReadVarIntAsync(stream, cancellationToken).ConfigureAwait(false);

        ulong subgroupId = 0;
        if (mode == SubgroupIdMode.Explicit)
        {
            subgroupId = await StreamIo.ReadVarIntAsync(stream, cancellationToken).ConfigureAwait(false);
        }

        bool inheritPriority = (type & DefaultPriorityBit) != 0;
        byte publisherPriority = 0;
        if (!inheritPriority)
        {
            int priority = await StreamIo.TryReadByteAsync(stream, cancellationToken).ConfigureAwait(false);
            if (priority < 0)
            {
                throw new MoqProtocolException("The subgroup stream ended before the Publisher Priority.");
            }

            publisherPriority = (byte)priority;
        }

        return new SubgroupHeader
        {
            TrackAlias = trackAlias,
            GroupId = groupId,
            SubgroupIdMode = mode,
            SubgroupId = subgroupId,
            HasProperties = (type & PropertiesBit) != 0,
            EndOfGroup = (type & EndOfGroupBit) != 0,
            FirstObject = (type & FirstObjectBit) != 0,
            InheritPriority = inheritPriority,
            PublisherPriority = publisherPriority,
        };
    }
}
