using Spangle.Net.Moqt.Wire;

namespace Spangle.Net.Moqt.Messages;

/// <summary>
/// The TRACK_STATUS request (draft-18 §10.14, type 0xD): asks what a track's current state is
/// without subscribing to it — the receiver treats it exactly like a SUBSCRIBE except that it
/// creates no subscription state and sends no Objects, answering with a REQUEST_OK (the draft's
/// "TRACK_STATUS_OK") whose Track Properties carry the answer.
/// <para>
/// The wire format is identical to <see cref="SubscribeMessage"/>; only the type differs (and
/// delivery-related parameters such as SUBSCRIBER_PRIORITY are left out, since nothing is
/// delivered). It is the first and only message on its bidirectional stream.
/// </para>
/// </summary>
public sealed class TrackStatusMessage
{
    /// <summary>Creates a TRACK_STATUS asking about <paramref name="track"/>.</summary>
    public TrackStatusMessage(ulong requestId, FullTrackName track,
        IReadOnlyList<MoqKeyValuePair>? parameters = null)
    {
        ArgumentNullException.ThrowIfNull(track);
        RequestId = requestId;
        Track = track;
        Parameters = parameters ?? [];
    }

    /// <summary>The request id, unique on the session (draft-18 §10.1).</summary>
    public ulong RequestId { get; }

    /// <summary>The track being asked about.</summary>
    public FullTrackName Track { get; }

    /// <summary>Request parameters; delivery-related ones do not belong here.</summary>
    public IReadOnlyList<MoqKeyValuePair> Parameters { get; }

    /// <summary>Writes the TRACK_STATUS payload.</summary>
    public void EncodePayload(MoqWriter writer)
    {
        ArgumentNullException.ThrowIfNull(writer);
        writer.WriteVarInt(RequestId);
        Track.WriteTo(writer);
        KeyValuePairCodec.WriteCounted(writer, [.. Parameters.OrderBy(p => p.Type)]);
    }

    /// <summary>Parses a TRACK_STATUS payload.</summary>
    public static TrackStatusMessage DecodePayload(ReadOnlySpan<byte> payload)
    {
        var reader = new MoqReader(payload);
        ulong requestId = reader.ReadVarInt();
        FullTrackName track = FullTrackName.Read(ref reader);
        int parameterCount = reader.ReadVarIntAsInt32();
        IReadOnlyList<MoqKeyValuePair> parameters = KeyValuePairCodec.ReadCounted(ref reader, parameterCount);
        return new TrackStatusMessage(requestId, track, parameters);
    }
}
