using Spangle.Net.Moqt.Wire;

namespace Spangle.Net.Moqt.Messages;

/// <summary>
/// The PUBLISH request (draft-18 §10.10, type 0x1D): a publisher pushes a track at a peer rather
/// than waiting to be asked — the mirror of SUBSCRIBE. Unlike SUBSCRIBE the publisher assigns the
/// Track Alias here, since it is the one offering. The peer answers with a REQUEST_OK (the draft's
/// "PUBLISH_OK") to accept, or REQUEST_ERROR to decline.
/// </summary>
public sealed class PublishMessage
{
    /// <summary>Creates a PUBLISH offering <paramref name="track"/> under <paramref name="trackAlias"/>.</summary>
    public PublishMessage(ulong requestId, FullTrackName track, ulong trackAlias,
        IReadOnlyList<MoqKeyValuePair>? parameters = null, IReadOnlyList<MoqKeyValuePair>? trackProperties = null)
    {
        ArgumentNullException.ThrowIfNull(track);
        RequestId = requestId;
        Track = track;
        TrackAlias = trackAlias;
        Parameters = parameters ?? [];
        TrackProperties = trackProperties ?? [];
    }

    /// <summary>The request id, unique on the session (draft-18 §10.1).</summary>
    public ulong RequestId { get; }

    /// <summary>The track being offered.</summary>
    public FullTrackName Track { get; }

    /// <summary>The alias the publisher assigns; the data plane tags objects with it.</summary>
    public ulong TrackAlias { get; }

    /// <summary>Request parameters (Key-Value-Pairs).</summary>
    public IReadOnlyList<MoqKeyValuePair> Parameters { get; }

    /// <summary>Track Properties — the trailing field, spanning the rest of the message.</summary>
    public IReadOnlyList<MoqKeyValuePair> TrackProperties { get; }

    /// <summary>Writes the PUBLISH payload.</summary>
    public void EncodePayload(MoqWriter writer)
    {
        ArgumentNullException.ThrowIfNull(writer);
        writer.WriteVarInt(RequestId);
        Track.WriteTo(writer);
        writer.WriteVarInt(TrackAlias);
        KeyValuePairCodec.WriteCounted(writer, [.. Parameters.OrderBy(p => p.Type)]);
        KeyValuePairCodec.WriteList(writer, [.. TrackProperties.OrderBy(p => p.Type)]);
    }

    /// <summary>Parses a PUBLISH payload.</summary>
    public static PublishMessage DecodePayload(ReadOnlySpan<byte> payload)
    {
        var reader = new MoqReader(payload);
        ulong requestId = reader.ReadVarInt();
        FullTrackName track = FullTrackName.Read(ref reader);
        ulong trackAlias = reader.ReadVarInt();
        int parameterCount = reader.ReadVarIntAsInt32();
        IReadOnlyList<MoqKeyValuePair> parameters = KeyValuePairCodec.ReadCounted(ref reader, parameterCount);
        IReadOnlyList<MoqKeyValuePair> trackProperties = KeyValuePairCodec.ReadList(ref reader);
        return new PublishMessage(requestId, track, trackAlias, parameters, trackProperties);
    }
}

/// <summary>
/// The PUBLISH_DONE message (draft-18 §10.11, type 0xB): a publisher tells a subscriber it has
/// stopped, why, and how many data streams it opened — so the subscriber knows when it has seen
/// everything rather than guessing.
/// </summary>
public sealed class PublishDoneMessage
{
    /// <summary>Creates a PUBLISH_DONE.</summary>
    public PublishDoneMessage(ulong statusCode, ulong streamCount, string errorReason = "")
    {
        ArgumentNullException.ThrowIfNull(errorReason);
        StatusCode = statusCode;
        StreamCount = streamCount;
        ErrorReason = errorReason;
    }

    /// <summary>Why the publisher stopped.</summary>
    public ulong StatusCode { get; }

    /// <summary>How many data streams this subscription opened.</summary>
    public ulong StreamCount { get; }

    /// <summary>A human-readable reason phrase.</summary>
    public string ErrorReason { get; }

    /// <summary>Writes the PUBLISH_DONE payload.</summary>
    public void EncodePayload(MoqWriter writer)
    {
        ArgumentNullException.ThrowIfNull(writer);
        writer.WriteVarInt(StatusCode);
        writer.WriteVarInt(StreamCount);
        writer.WriteString(ErrorReason);
    }

    /// <summary>Parses a PUBLISH_DONE payload.</summary>
    public static PublishDoneMessage DecodePayload(ReadOnlySpan<byte> payload)
    {
        var reader = new MoqReader(payload);
        ulong statusCode = reader.ReadVarInt();
        ulong streamCount = reader.ReadVarInt();
        string reason = reader.ReadString();
        return new PublishDoneMessage(statusCode, streamCount, reason);
    }
}

/// <summary>
/// The PUBLISH_BLOCKED message (draft-18 §10.20, type 0xF): a publisher would like to PUBLISH a
/// track but cannot, because the peer's request limits leave it no room. Names the track it is
/// blocked on. (draft-19 renames this to PUBLISH_SKIPPED.)
/// </summary>
public sealed class PublishBlockedMessage
{
    /// <summary>Creates a PUBLISH_BLOCKED naming the track that could not be published.</summary>
    public PublishBlockedMessage(TrackNamespace namespaceSuffix, ReadOnlyMemory<byte> trackName)
    {
        ArgumentNullException.ThrowIfNull(namespaceSuffix);
        NamespaceSuffix = namespaceSuffix;
        TrackName = trackName;
    }

    /// <summary>The namespace fields beyond the subscribed prefix.</summary>
    public TrackNamespace NamespaceSuffix { get; }

    /// <summary>The track name (opaque bytes).</summary>
    public ReadOnlyMemory<byte> TrackName { get; }

    /// <summary>Writes the PUBLISH_BLOCKED payload.</summary>
    public void EncodePayload(MoqWriter writer)
    {
        ArgumentNullException.ThrowIfNull(writer);
        NamespaceSuffix.WriteTo(writer);
        writer.WriteBytes(TrackName.Span);
    }

    /// <summary>Parses a PUBLISH_BLOCKED payload.</summary>
    public static PublishBlockedMessage DecodePayload(ReadOnlySpan<byte> payload)
    {
        var reader = new MoqReader(payload);
        TrackNamespace suffix = TrackNamespace.Read(ref reader);
        byte[] name = reader.ReadBytes().ToArray();
        return new PublishBlockedMessage(suffix, name);
    }
}
