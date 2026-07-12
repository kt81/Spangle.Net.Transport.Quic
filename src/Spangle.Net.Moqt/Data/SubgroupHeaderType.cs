namespace Spangle.Net.Moqt.Data;

/// <summary>
/// The SUBGROUP_HEADER Type field (draft-18 §11.4.2), whose data-stream-type pattern is
/// <c>0b0XX1XXXX</c> (§3.4): bit 4 is always set, bit 7 is always clear, and the remaining
/// bits select which header fields follow. This type is the <em>single</em> definition of
/// that bit layout — both encoding (<see cref="Compose"/>) and decoding (the accessors and
/// <see cref="Parse"/>) derive from the one set of constants here, so revising the layout for
/// a later draft is a one-place change and read and write can never drift apart.
/// </summary>
internal readonly struct SubgroupHeaderType
{
    private const ulong Base = 0x10;            // bit 4 — always set (the "1" in 0b0XX1XXXX)
    private const ulong MaxValue = 0x7F;        // bit 7 — always clear
    private const ulong PropertiesBit = 0x01;   // bit 0 — every object carries a Properties field
    private const ulong SubgroupIdModeMask = 0x06; // bits 1-2 — how the Subgroup ID is conveyed
    private const int SubgroupIdModeShift = 1;
    private const ulong EndOfGroupBit = 0x08;   // bit 3 — this subgroup holds the group's largest Object
    private const ulong InheritPriorityBit = 0x20; // bit 5 — DEFAULT_PRIORITY: priority omitted, inherited
    private const ulong FirstObjectBit = 0x40;  // bit 6 — FIRST_OBJECT (semantic; selects no extra field)

    private SubgroupHeaderType(ulong value) => Value = value;

    /// <summary>The raw varint type value.</summary>
    public ulong Value { get; }

    /// <summary>Whether every object on the stream carries a Properties field.</summary>
    public bool HasProperties => (Value & PropertiesBit) != 0;

    /// <summary>How the Subgroup ID is conveyed.</summary>
    public SubgroupIdMode SubgroupIdMode => (SubgroupIdMode)((Value & SubgroupIdModeMask) >> SubgroupIdModeShift);

    /// <summary>Whether this subgroup contains the largest Object in the Group.</summary>
    public bool EndOfGroup => (Value & EndOfGroupBit) != 0;

    /// <summary>Whether the priority field is omitted and inherited from the subscription.</summary>
    public bool InheritPriority => (Value & InheritPriorityBit) != 0;

    /// <summary>
    /// The FIRST_OBJECT bit (§2.2): the stream's first object is the first ever published in
    /// the subgroup. This is a semantic assertion only — it does <em>not</em> add a field to
    /// the header, so nothing extra is read or written when it is set.
    /// </summary>
    public bool FirstObject => (Value & FirstObjectBit) != 0;

    /// <summary>Builds the type value from the header's field layout.</summary>
    public static SubgroupHeaderType Compose(bool hasProperties, SubgroupIdMode subgroupIdMode, bool endOfGroup,
        bool inheritPriority, bool firstObject)
    {
        ulong value = Base
            | (hasProperties ? PropertiesBit : 0)
            | ((ulong)subgroupIdMode << SubgroupIdModeShift)
            | (endOfGroup ? EndOfGroupBit : 0)
            | (inheritPriority ? InheritPriorityBit : 0)
            | (firstObject ? FirstObjectBit : 0);
        return new SubgroupHeaderType(value);
    }

    /// <summary>Validates a raw type value read off the wire and wraps it for decoding.</summary>
    public static SubgroupHeaderType Parse(ulong value)
    {
        if ((value & Base) == 0 || value > MaxValue)
        {
            throw new MoqProtocolException($"0x{value:X} is not a valid SUBGROUP_HEADER type.");
        }

        if (((value & SubgroupIdModeMask) >> SubgroupIdModeShift) == 3)
        {
            throw new MoqProtocolException("SUBGROUP_ID_MODE 0b11 is reserved.");
        }

        return new SubgroupHeaderType(value);
    }
}
