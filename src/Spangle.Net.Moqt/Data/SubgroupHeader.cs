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
/// The SUBGROUP_HEADER that opens a subgroup data stream (draft-18 §11.4.2). Its varint type
/// selects which fields follow; that bit layout lives in one place, <see cref="SubgroupHeaderType"/>,
/// which both the <see cref="Type"/> getter and <see cref="ReadAsync"/> go through.
/// </summary>
public readonly struct SubgroupHeader
{
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
        SubgroupHeaderType.Compose(HasProperties, SubgroupIdMode, EndOfGroup, InheritPriority, FirstObject).Value;

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

        ulong rawType = await StreamIo.ReadVarIntAsync(stream, cancellationToken).ConfigureAwait(false);
        SubgroupHeaderType type = SubgroupHeaderType.Parse(rawType);
        SubgroupIdMode mode = type.SubgroupIdMode;

        ulong trackAlias = await StreamIo.ReadVarIntAsync(stream, cancellationToken).ConfigureAwait(false);
        ulong groupId = await StreamIo.ReadVarIntAsync(stream, cancellationToken).ConfigureAwait(false);

        ulong subgroupId = 0;
        if (mode == SubgroupIdMode.Explicit)
        {
            subgroupId = await StreamIo.ReadVarIntAsync(stream, cancellationToken).ConfigureAwait(false);
        }

        // FIRST_OBJECT selects no field, so the priority byte follows the Subgroup ID directly.
        bool inheritPriority = type.InheritPriority;
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
            HasProperties = type.HasProperties,
            EndOfGroup = type.EndOfGroup,
            FirstObject = type.FirstObject,
            InheritPriority = inheritPriority,
            PublisherPriority = publisherPriority,
        };
    }
}
