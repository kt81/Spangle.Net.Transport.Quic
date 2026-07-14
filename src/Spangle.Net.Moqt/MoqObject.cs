using Spangle.Net.Moqt.Wire;

namespace Spangle.Net.Moqt;

/// <summary>
/// A MOQT Object (draft-18 §2.1, §11.2.1): the atomic unit of media on a track. It is
/// addressed by Group and Object id within a Subgroup, carries the publisher's priority and
/// a status, an opaque payload the media layer fills, and optional Extension Headers. This is
/// the type the Spangle bridge produces (egress) and consumes (ingest) at the package boundary.
/// </summary>
public sealed class MoqObject
{
    /// <summary>Creates an object with the given coordinates, status, payload, and extensions.</summary>
    public MoqObject(ulong groupId, ulong objectId, ulong subgroupId, byte publisherPriority,
        MoqObjectStatus status, ReadOnlyMemory<byte> payload,
        IReadOnlyList<MoqKeyValuePair>? extensions = null)
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
        Extensions = extensions ?? [];
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
    /// The object's Extension Headers as Key-Value-Pairs — where a media mapping carries its
    /// per-frame metadata (draft-cenzano-moq-media-interop puts the media type, timestamps and
    /// codec extradata here). Only written when the subgroup header's Properties bit is set;
    /// empty otherwise.
    /// </summary>
    public IReadOnlyList<MoqKeyValuePair> Extensions { get; }

    /// <summary>A normal object carrying a payload and optional extension headers.</summary>
    public static MoqObject Normal(ulong groupId, ulong objectId, ulong subgroupId, byte publisherPriority,
        ReadOnlyMemory<byte> payload, IReadOnlyList<MoqKeyValuePair>? extensions = null) =>
        new(groupId, objectId, subgroupId, publisherPriority, MoqObjectStatus.Normal, payload, extensions);
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
