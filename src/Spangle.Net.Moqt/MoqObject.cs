using Spangle.Net.Moqt.Wire;

namespace Spangle.Net.Moqt;

/// <summary>
/// A MOQT Object (draft-18 §2.1, §11.2.1): the atomic unit of media on a track. It is
/// addressed by Group and Object id within a Subgroup, carries the publisher's priority and
/// a status, an opaque payload the media layer fills, and optional Object Properties. This is
/// the type the Spangle bridge produces (egress) and consumes (ingest) at the package boundary.
/// </summary>
public sealed class MoqObject
{
    /// <summary>Creates an object with the given coordinates, status, payload, and properties.</summary>
    public MoqObject(ulong groupId, ulong objectId, ulong subgroupId, byte publisherPriority,
        MoqObjectStatus status, ReadOnlyMemory<byte> payload,
        IReadOnlyList<MoqKeyValuePair>? properties = null,
        MoqForwardingPreference forwarding = MoqForwardingPreference.Subgroup)
    {
        if (status != MoqObjectStatus.Normal && !payload.IsEmpty)
        {
            throw new ArgumentException("A non-normal object must have an empty payload.", nameof(payload));
        }

        GroupId = groupId;
        ObjectId = objectId;
        SubgroupId = subgroupId;
        PublisherPriority = publisherPriority;
        Status = status;
        Payload = payload;
        Properties = properties ?? [];
        Forwarding = forwarding;
    }

    /// <summary>The id of the Object's Group within the track.</summary>
    public ulong GroupId { get; }

    /// <summary>The order of the object within its group.</summary>
    public ulong ObjectId { get; }

    /// <summary>The id of the Object's Subgroup within the group.</summary>
    public ulong SubgroupId { get; }

    /// <summary>The publisher's 8-bit delivery priority for the object.</summary>
    public byte PublisherPriority { get; }

    /// <summary>Whether the object is normal or marks the end of a group/track.</summary>
    public MoqObjectStatus Status { get; }

    /// <summary>The opaque payload; empty for any non-normal status.</summary>
    public ReadOnlyMemory<byte> Payload { get; }

    /// <summary>
    /// The Object Properties as Key-Value-Pairs — where a media mapping carries its per-frame
    /// metadata (draft-cenzano-moq-media-interop puts the media type, timestamps and codec
    /// extradata here; older drafts called these Extension Headers, and LOC still does). Only
    /// written when the subgroup header's Properties bit is set; empty otherwise.
    /// </summary>
    public IReadOnlyList<MoqKeyValuePair> Properties { get; }

    /// <summary>
    /// How the publisher sends this object (draft-18 §11.2.1). It is a property of the individual
    /// object, not of the track, and a <see cref="MoqForwardingPreference.Datagram"/> object has no
    /// Subgroup ID — so <see cref="SubgroupId"/> is meaningless for one.
    /// </summary>
    public MoqForwardingPreference Forwarding { get; }

    /// <summary>A normal object carrying a payload and optional Object Properties.</summary>
    public static MoqObject Normal(ulong groupId, ulong objectId, ulong subgroupId, byte publisherPriority,
        ReadOnlyMemory<byte> payload, IReadOnlyList<MoqKeyValuePair>? properties = null) =>
        new(groupId, objectId, subgroupId, publisherPriority, MoqObjectStatus.Normal, payload, properties);
}

/// <summary>
/// How a publisher sends an Object (draft-18 §11.2.1). This is per-object and can vary within a
/// track; in a subscription an Object MUST be sent according to its preference.
/// </summary>
public enum MoqForwardingPreference
{
    /// <summary>Sent on a subgroup stream. Every object on a SUBGROUP_HEADER stream is this.</summary>
    Subgroup,

    /// <summary>Sent in a datagram, so the object has no Subgroup ID.</summary>
    Datagram,
}

/// <summary>
/// The Object Status (draft-18 §11.2.1.1). Only carried when the payload length is zero;
/// a non-zero-length object is implicitly <see cref="Normal"/>.
/// </summary>
public enum MoqObjectStatus
{
    /// <summary>A normal object (0x0).</summary>
    Normal = 0x0,

    /// <summary>No objects at or after this Object ID exist in the group (0x3).</summary>
    EndOfGroup = 0x3,

    /// <summary>No objects at or after this location exist in the track (0x4).</summary>
    EndOfTrack = 0x4,
}
