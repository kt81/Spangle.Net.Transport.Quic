using Spangle.Net.Moqt.Wire;

namespace Spangle.Net.Moqt.Messages;

/// <summary>How a FETCH names the objects it wants (draft-18 §10.12).</summary>
public enum MoqFetchType : ulong
{
    /// <summary>0x1 — an explicit track and Location range.</summary>
    Standalone = 0x1,

    /// <summary>0x2 — relative to an existing subscription, to fill in what came before it.</summary>
    Joining = 0x2,
}

/// <summary>
/// The FETCH request (draft-18 §10.12, type 0x16): asks for objects that already exist, rather
/// than subscribing to what comes next — catch-up and VOD. A <em>standalone</em> fetch names a
/// track and a Location range outright; a <em>joining</em> fetch names an existing subscription
/// and fills in the objects before it, so a late subscriber can start at a keyframe without
/// racing the live edge. The peer answers <see cref="FetchOkMessage"/> or
/// <see cref="RequestErrorMessage"/>.
/// </summary>
public sealed class FetchMessage
{
    private FetchMessage(ulong requestId, MoqFetchType fetchType, FullTrackName? track, MoqLocation startLocation,
        MoqLocation endLocation, ulong joiningRequestId, ulong joiningStart,
        IReadOnlyList<MoqKeyValuePair> parameters)
    {
        RequestId = requestId;
        FetchType = fetchType;
        Track = track;
        StartLocation = startLocation;
        EndLocation = endLocation;
        JoiningRequestId = joiningRequestId;
        JoiningStart = joiningStart;
        Parameters = parameters;
    }

    /// <summary>The request id, unique on the session (draft-18 §10.1).</summary>
    public ulong RequestId { get; }

    /// <summary>Which of the two forms this is.</summary>
    public MoqFetchType FetchType { get; }

    /// <summary>The track — standalone fetches only.</summary>
    public FullTrackName? Track { get; }

    /// <summary>Where the range begins — standalone fetches only.</summary>
    public MoqLocation StartLocation { get; }

    /// <summary>Where the range ends — standalone fetches only.</summary>
    public MoqLocation EndLocation { get; }

    /// <summary>The subscription to fill in ahead of — joining fetches only.</summary>
    public ulong JoiningRequestId { get; }

    /// <summary>How far back from that subscription to start — joining fetches only.</summary>
    public ulong JoiningStart { get; }

    /// <summary>Request parameters (Key-Value-Pairs).</summary>
    public IReadOnlyList<MoqKeyValuePair> Parameters { get; }

    /// <summary>Creates a standalone FETCH for an explicit track and Location range.</summary>
    public static FetchMessage Standalone(ulong requestId, FullTrackName track, MoqLocation start, MoqLocation end,
        IReadOnlyList<MoqKeyValuePair>? parameters = null)
    {
        ArgumentNullException.ThrowIfNull(track);
        return new FetchMessage(requestId, MoqFetchType.Standalone, track, start, end, 0, 0, parameters ?? []);
    }

    /// <summary>Creates a joining FETCH that fills in the objects before an existing subscription.</summary>
    public static FetchMessage Joining(ulong requestId, ulong joiningRequestId, ulong joiningStart,
        IReadOnlyList<MoqKeyValuePair>? parameters = null) =>
        new(requestId, MoqFetchType.Joining, null, default, default, joiningRequestId, joiningStart,
            parameters ?? []);

    /// <summary>Writes the FETCH payload.</summary>
    public void EncodePayload(MoqWriter writer)
    {
        ArgumentNullException.ThrowIfNull(writer);
        writer.WriteVarInt(RequestId);
        writer.WriteVarInt((ulong)FetchType);

        if (FetchType == MoqFetchType.Standalone)
        {
            Track!.WriteTo(writer);
            StartLocation.WriteTo(writer);
            EndLocation.WriteTo(writer);
        }
        else
        {
            writer.WriteVarInt(JoiningRequestId);
            writer.WriteVarInt(JoiningStart);
        }

        KeyValuePairCodec.WriteCounted(writer, [.. Parameters.OrderBy(p => p.Type)]);
    }

    /// <summary>Parses a FETCH payload; the Fetch Type selects which of the two bodies follows.</summary>
    public static FetchMessage DecodePayload(ReadOnlySpan<byte> payload)
    {
        var reader = new MoqReader(payload);
        ulong requestId = reader.ReadVarInt();
        ulong rawType = reader.ReadVarInt();

        FullTrackName? track = null;
        MoqLocation start = default;
        MoqLocation end = default;
        ulong joiningRequestId = 0;
        ulong joiningStart = 0;

        switch (rawType)
        {
            case (ulong)MoqFetchType.Standalone:
                track = FullTrackName.Read(ref reader);
                start = MoqLocation.Read(ref reader);
                end = MoqLocation.Read(ref reader);
                break;
            case (ulong)MoqFetchType.Joining:
                joiningRequestId = reader.ReadVarInt();
                joiningStart = reader.ReadVarInt();
                break;
            default:
                throw new MoqProtocolException($"0x{rawType:X} is not a valid Fetch Type.");
        }

        int parameterCount = reader.ReadVarIntAsInt32();
        IReadOnlyList<MoqKeyValuePair> parameters = KeyValuePairCodec.ReadCounted(ref reader, parameterCount);
        return new FetchMessage(requestId, (MoqFetchType)rawType, track, start, end, joiningRequestId, joiningStart,
            parameters);
    }
}

/// <summary>
/// The FETCH_OK reply (draft-18 §10.13, type 0x18): the fetch is accepted, and this says where the
/// fetched range actually ends and whether that is the end of the track — so the requester knows
/// what is coming without waiting to find out.
/// </summary>
public sealed class FetchOkMessage
{
    /// <summary>Creates a FETCH_OK.</summary>
    public FetchOkMessage(bool endOfTrack, MoqLocation endLocation, IReadOnlyList<MoqKeyValuePair>? parameters = null,
        IReadOnlyList<MoqKeyValuePair>? trackProperties = null)
    {
        EndOfTrack = endOfTrack;
        EndLocation = endLocation;
        Parameters = parameters ?? [];
        TrackProperties = trackProperties ?? [];
    }

    /// <summary>Whether the fetched range reaches the end of the track.</summary>
    public bool EndOfTrack { get; }

    /// <summary>Where the fetched range ends.</summary>
    public MoqLocation EndLocation { get; }

    /// <summary>Response parameters (Key-Value-Pairs).</summary>
    public IReadOnlyList<MoqKeyValuePair> Parameters { get; }

    /// <summary>Track Properties — the trailing field, spanning the rest of the message.</summary>
    public IReadOnlyList<MoqKeyValuePair> TrackProperties { get; }

    /// <summary>Writes the FETCH_OK payload. End Of Track is a single byte, not a varint.</summary>
    public void EncodePayload(MoqWriter writer)
    {
        ArgumentNullException.ThrowIfNull(writer);
        writer.WriteByte(EndOfTrack ? (byte)1 : (byte)0);
        EndLocation.WriteTo(writer);
        KeyValuePairCodec.WriteCounted(writer, [.. Parameters.OrderBy(p => p.Type)]);
        KeyValuePairCodec.WriteList(writer, [.. TrackProperties.OrderBy(p => p.Type)]);
    }

    /// <summary>Parses a FETCH_OK payload.</summary>
    public static FetchOkMessage DecodePayload(ReadOnlySpan<byte> payload)
    {
        var reader = new MoqReader(payload);
        bool endOfTrack = reader.ReadByte() != 0;
        MoqLocation endLocation = MoqLocation.Read(ref reader);
        int parameterCount = reader.ReadVarIntAsInt32();
        IReadOnlyList<MoqKeyValuePair> parameters = KeyValuePairCodec.ReadCounted(ref reader, parameterCount);
        IReadOnlyList<MoqKeyValuePair> trackProperties = KeyValuePairCodec.ReadList(ref reader);
        return new FetchOkMessage(endOfTrack, endLocation, parameters, trackProperties);
    }
}
