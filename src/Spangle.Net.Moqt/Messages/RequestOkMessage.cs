using Spangle.Net.Moqt.Wire;

namespace Spangle.Net.Moqt.Messages;

/// <summary>
/// The REQUEST_OK reply (draft-18 §10.5, type 0x7): the generic success answer to PUBLISH,
/// REQUEST_UPDATE, TRACK_STATUS, SUBSCRIBE_NAMESPACE, SUBSCRIBE_TRACKS and PUBLISH_NAMESPACE.
/// The draft calls it PUBLISH_OK, TRACK_STATUS_OK, PUBLISH_NAMESPACE_OK … depending on what it
/// answers, but all of them are this message. It rides the request's own bidirectional stream, so
/// the Request ID is implicit. SUBSCRIBE and FETCH are the exceptions: they have their own
/// <see cref="SubscribeOkMessage"/> and <see cref="FetchOkMessage"/>.
/// </summary>
public sealed class RequestOkMessage
{
    /// <summary>Creates a REQUEST_OK with the given parameters and track properties.</summary>
    public RequestOkMessage(IReadOnlyList<MoqKeyValuePair>? parameters = null,
        IReadOnlyList<MoqKeyValuePair>? trackProperties = null)
    {
        Parameters = parameters ?? [];
        TrackProperties = trackProperties ?? [];
    }

    /// <summary>Response parameters (Key-Value-Pairs).</summary>
    public IReadOnlyList<MoqKeyValuePair> Parameters { get; }

    /// <summary>
    /// Track Properties — the trailing field, spanning the rest of the message. Populated in a
    /// TRACK_STATUS_OK; empty in the other REQUEST_OK flavours.
    /// </summary>
    public IReadOnlyList<MoqKeyValuePair> TrackProperties { get; }

    /// <summary>Writes the REQUEST_OK payload.</summary>
    public void EncodePayload(MoqWriter writer)
    {
        ArgumentNullException.ThrowIfNull(writer);
        KeyValuePairCodec.WriteCounted(writer, [.. Parameters.OrderBy(p => p.Type)]);
        KeyValuePairCodec.WriteList(writer, [.. TrackProperties.OrderBy(p => p.Type)]);
    }

    /// <summary>Parses a REQUEST_OK payload.</summary>
    public static RequestOkMessage DecodePayload(ReadOnlySpan<byte> payload)
    {
        var reader = new MoqReader(payload);
        int parameterCount = reader.ReadVarIntAsInt32();
        IReadOnlyList<MoqKeyValuePair> parameters = KeyValuePairCodec.ReadCounted(ref reader, parameterCount);
        IReadOnlyList<MoqKeyValuePair> trackProperties = KeyValuePairCodec.ReadList(ref reader);
        return new RequestOkMessage(parameters, trackProperties);
    }
}
