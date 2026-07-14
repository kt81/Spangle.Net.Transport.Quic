namespace Spangle.Net.Moqt.Data;

/// <summary>How a fetch object's Subgroup ID is conveyed (draft-18 §11.4.4.1, Table 8).</summary>
internal enum FetchSubgroupMode
{
    /// <summary>0x00 — the Subgroup ID is zero and no field follows.</summary>
    Zero = 0x00,

    /// <summary>0x01 — the Subgroup ID is the prior object's.</summary>
    PriorSubgroup = 0x01,

    /// <summary>0x02 — the Subgroup ID is the prior object's plus one.</summary>
    PriorSubgroupPlusOne = 0x02,

    /// <summary>0x03 — the Subgroup ID field is present.</summary>
    Explicit = 0x03,
}

/// <summary>
/// The Serialization Flags that open every object on a FETCH stream (draft-18 §11.4.4.1). The
/// field is a varint: below 128 its bits select which of the object's fields follow, and two
/// values above that are the End of Range markers of Table 7. Every bit meaning lives here, so
/// <see cref="FetchStreamReader"/> and <see cref="FetchStreamWriter"/> cannot drift apart.
/// </summary>
internal readonly struct FetchObjectFlags
{
    /// <summary>Table 7 (0x8C): the objects up to this Location are known not to exist.</summary>
    public const ulong EndOfNonExistentRange = 0x8C;

    /// <summary>Table 7 (0x10C): the status of the objects up to this Location is unknown.</summary>
    public const ulong EndOfUnknownRange = 0x10C;

    // Any value at or above this is not a bit pattern; only the two Table 7 markers are legal.
    private const ulong FlagsExclusiveMax = 0x80;

    private const ulong SubgroupModeMask = 0x03;
    private const ulong ObjectIdDeltaBit = 0x04;
    private const ulong GroupIdDeltaBit = 0x08;
    private const ulong PriorityBit = 0x10;
    private const ulong PropertiesBit = 0x20;
    private const ulong DatagramBit = 0x40;

    private FetchObjectFlags(ulong value) => Value = value;

    /// <summary>The varint value these flags encode to.</summary>
    public ulong Value { get; }

    /// <summary>How the Subgroup ID is conveyed (meaningless when <see cref="IsDatagram"/>).</summary>
    public FetchSubgroupMode SubgroupMode => (FetchSubgroupMode)(Value & SubgroupModeMask);

    /// <summary>Whether an Object ID Delta field follows.</summary>
    public bool HasObjectIdDelta => (Value & ObjectIdDeltaBit) != 0;

    /// <summary>Whether a Group ID Delta field follows.</summary>
    public bool HasGroupIdDelta => (Value & GroupIdDeltaBit) != 0;

    /// <summary>Whether a Publisher Priority byte follows; otherwise the prior object's applies.</summary>
    public bool HasPriority => (Value & PriorityBit) != 0;

    /// <summary>Whether an Object Properties block follows.</summary>
    public bool HasProperties => (Value & PropertiesBit) != 0;

    /// <summary>Whether the object's forwarding preference is Datagram, so it has no Subgroup ID.</summary>
    public bool IsDatagram => (Value & DatagramBit) != 0;

    /// <summary>Builds the flags for one object.</summary>
    public static FetchObjectFlags Compose(FetchSubgroupMode subgroupMode, bool hasObjectIdDelta,
        bool hasGroupIdDelta, bool hasPriority, bool hasProperties, bool isDatagram)
    {
        // A datagram object has no Subgroup ID: the spec has the publisher zero the mode bits and
        // the subscriber ignore them, so never let a caller's mode leak into the wire here.
        ulong value = isDatagram ? DatagramBit : (ulong)subgroupMode;
        if (hasObjectIdDelta)
        {
            value |= ObjectIdDeltaBit;
        }

        if (hasGroupIdDelta)
        {
            value |= GroupIdDeltaBit;
        }

        if (hasPriority)
        {
            value |= PriorityBit;
        }

        if (hasProperties)
        {
            value |= PropertiesBit;
        }

        return new FetchObjectFlags(value);
    }

    /// <summary>
    /// Validates a Serialization Flags value that is not one of the Table 7 markers — the caller
    /// checks for those first, since they select a different field layout entirely.
    /// </summary>
    public static FetchObjectFlags Parse(ulong value)
    {
        if (value >= FlagsExclusiveMax)
        {
            // 0x80 is reserved, and every other value at or above it that is not an End of Range
            // marker is undefined; both are a protocol violation rather than something to skip.
            throw new MoqProtocolException(
                $"0x{value:X} is not a valid FETCH Serialization Flags value.");
        }

        return new FetchObjectFlags(value);
    }
}
