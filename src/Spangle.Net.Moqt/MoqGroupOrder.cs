namespace Spangle.Net.Moqt;

/// <summary>
/// The order Groups are delivered in (draft-18 §10.2.8, the GROUP_ORDER parameter, type 0x22).
/// In a FETCH response it decides how a Group ID Delta resolves — ascending adds, descending
/// subtracts — so a reader that assumes the wrong one silently reconstructs the wrong Group IDs.
/// Omitted from a FETCH, the receiver uses <see cref="Ascending"/>.
/// </summary>
public enum MoqGroupOrder
{
    /// <summary>Group IDs increase (0x1). The default for a FETCH.</summary>
    Ascending = 0x1,

    /// <summary>Group IDs decrease (0x2).</summary>
    Descending = 0x2,
}
