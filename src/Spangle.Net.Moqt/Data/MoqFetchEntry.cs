using System.Diagnostics.CodeAnalysis;

namespace Spangle.Net.Moqt.Data;

/// <summary>
/// One entry on a fetch stream. A fetch answers with a run of objects, but it may also state that
/// a stretch of the range it was asked for has nothing to send — so an entry is either an object
/// or an End of Range marker, and a caller pattern-matches on which.
/// </summary>
public abstract class MoqFetchEntry
{
    private protected MoqFetchEntry()
    {
    }
}

/// <summary>An object served by a fetch.</summary>
public sealed class MoqFetchedObject : MoqFetchEntry
{
    internal MoqFetchedObject(MoqObject moqObject) => Object = moqObject;

    /// <summary>The object. Its status is always Normal — a FETCH cannot carry an Object Status.</summary>
    [SuppressMessage("Naming", "CA1720:Identifier contains type name",
        Justification = "Object is the spec's own noun for the unit of media (draft-18 §11.2.1) and this " +
                        "library's vocabulary throughout; MoqObject already carries a prefix to clear the " +
                        "framework type, and renaming here would only obscure the mapping to the draft.")]
    public MoqObject Object { get; }
}

/// <summary>
/// An End of Range marker (draft-18 §11.4.4.2): every Location between the previous entry and
/// <see cref="Location"/>, inclusive, has nothing to serve. It takes the place of the Object
/// Status a subscription would use, which a FETCH has no field for.
/// </summary>
public sealed class MoqFetchEndOfRange : MoqFetchEntry
{
    internal MoqFetchEndOfRange(MoqLocation location, MoqFetchRangeKind kind)
    {
        Location = location;
        Kind = kind;
    }

    /// <summary>The last Location the marker covers.</summary>
    public MoqLocation Location { get; }

    /// <summary>Whether the covered objects are known absent or merely unknown.</summary>
    public MoqFetchRangeKind Kind { get; }
}

/// <summary>What an <see cref="MoqFetchEndOfRange"/> says about the range it covers.</summary>
public enum MoqFetchRangeKind
{
    /// <summary>The objects are known not to exist (Serialization Flags 0x8C).</summary>
    NonExistent,

    /// <summary>The publisher does not know whether the objects exist (Serialization Flags 0x10C).</summary>
    Unknown,
}
