using Spangle.Net.Moqt.Wire;

namespace Spangle.Net.Moqt.Messages;

/// <summary>
/// The SUBSCRIBE request (draft-18 §10.7, type 0x3): a subscriber asks for a track by name.
/// Sent as the first message on a bidirectional request stream. The Track Alias is assigned
/// by the publisher in the <see cref="SubscribeOkMessage"/> response, not here.
/// </summary>
public sealed class SubscribeMessage
{
    /// <summary>Creates a SUBSCRIBE for <paramref name="track"/>.</summary>
    public SubscribeMessage(ulong requestId, FullTrackName track, IReadOnlyList<MoqKeyValuePair>? parameters = null)
    {
        ArgumentNullException.ThrowIfNull(track);
        RequestId = requestId;
        Track = track;
        Parameters = parameters ?? [];
    }

    /// <summary>The request id, unique on the session (draft-18 §10.1).</summary>
    public ulong RequestId { get; }

    /// <summary>The track being subscribed to.</summary>
    public FullTrackName Track { get; }

    /// <summary>Subscribe parameters (priority, group order, filter, ... as Key-Value-Pairs).</summary>
    public IReadOnlyList<MoqKeyValuePair> Parameters { get; }

    /// <summary>Writes the SUBSCRIBE payload.</summary>
    public void EncodePayload(MoqWriter writer)
    {
        ArgumentNullException.ThrowIfNull(writer);
        writer.WriteVarInt(RequestId);
        Track.WriteTo(writer);
        KeyValuePairCodec.WriteCounted(writer, [.. Parameters.OrderBy(p => p.Type)]);
    }

    /// <summary>Parses a SUBSCRIBE payload.</summary>
    public static SubscribeMessage DecodePayload(ReadOnlySpan<byte> payload)
    {
        var reader = new MoqReader(payload);
        ulong requestId = reader.ReadVarInt();
        FullTrackName track = FullTrackName.Read(ref reader);
        int parameterCount = reader.ReadVarIntAsInt32();
        IReadOnlyList<MoqKeyValuePair> parameters = KeyValuePairCodec.ReadCounted(ref reader, parameterCount);
        return new SubscribeMessage(requestId, track, parameters);
    }
}
