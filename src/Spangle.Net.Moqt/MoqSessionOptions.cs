using Spangle.Net.Moqt.Data;

namespace Spangle.Net.Moqt;

/// <summary>
/// Per-session configuration, given to <see cref="MoqSession.ConnectAsync"/> /
/// <see cref="MoqSession.AcceptAsync"/>. This is where read bounds live: they are a property
/// of the peer relationship, not of any single accept call, so the session's demux loop
/// applies them to every stream it classifies and the facades never pass them by hand.
/// </summary>
public sealed record MoqSessionOptions
{
    /// <summary>The options used when a session is not given any.</summary>
    public static MoqSessionOptions Default { get; } = new();

    /// <summary>Bounds on what the data-plane readers accept from the peer.</summary>
    public MoqReadLimits ReadLimits { get; init; } = MoqReadLimits.Default;

    /// <summary>
    /// How many data streams the demux loop holds for a Track Alias (or FETCH request) nothing
    /// has claimed yet. A small buffer is needed because the claim races the data: a publisher
    /// may open the first subgroup stream the instant it sends SUBSCRIBE_OK, before the
    /// subscriber has parsed the alias out of it. Beyond the bound, unclaimed streams are
    /// disposed — an accepted stream holds inbound-stream credit, and a peer inventing aliases
    /// must not be able to pin them all.
    /// </summary>
    public int MaxUnclaimedStreams { get; init; } = 16;
}
