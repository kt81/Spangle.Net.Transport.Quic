using System.Diagnostics.CodeAnalysis;
using System.Runtime.InteropServices;
using Spangle.Net.Moqt.Wire;

namespace Spangle.Net.Moqt;

/// <summary>
/// A Location (draft-18 §1.4.2): a point in a track, addressed by Group and Object id. Used to
/// bound fetches and to report where a track currently ends.
/// </summary>
[StructLayout(LayoutKind.Auto)]
public readonly record struct MoqLocation(ulong Group, ulong ObjectId)
{
    internal void WriteTo(MoqWriter writer)
    {
        writer.WriteVarInt(Group);
        writer.WriteVarInt(ObjectId);
    }

    internal static MoqLocation Read(ref MoqReader reader)
    {
        ulong group = reader.ReadVarInt();
        ulong objectId = reader.ReadVarInt();
        return new MoqLocation(group, objectId);
    }
}

/// <summary>
/// A Redirect (draft-18 §10.6.1): where a rejected request should be retried — a connect URI plus
/// the track it applies to. Carried by <see cref="Messages.RequestErrorMessage"/>.
/// </summary>
[SuppressMessage("Design", "CA1054:URI-like parameters should not be strings",
    Justification = "The wire field is a length-prefixed string that need not be a well-formed URI; " +
                    "System.Uri cannot round-trip what the peer actually sent.")]
[SuppressMessage("Design", "CA1056:URI-like properties should not be strings",
    Justification = "The wire field is a length-prefixed string that need not be a well-formed URI; " +
                    "System.Uri cannot round-trip what the peer actually sent.")]
public sealed class MoqRedirect
{
    /// <summary>Creates a redirect to <paramref name="connectUri"/> for <paramref name="track"/>.</summary>
    public MoqRedirect(string connectUri, FullTrackName track)
    {
        ArgumentNullException.ThrowIfNull(connectUri);
        ArgumentNullException.ThrowIfNull(track);
        ConnectUri = connectUri;
        Track = track;
    }

    /// <summary>The URI to reconnect to.</summary>
    public string ConnectUri { get; }

    /// <summary>The track the redirect applies to.</summary>
    public FullTrackName Track { get; }

    internal void WriteTo(MoqWriter writer)
    {
        writer.WriteString(ConnectUri);
        Track.WriteTo(writer);
    }

    internal static MoqRedirect Read(ref MoqReader reader)
    {
        string uri = reader.ReadString();
        FullTrackName track = FullTrackName.Read(ref reader);
        return new MoqRedirect(uri, track);
    }
}
