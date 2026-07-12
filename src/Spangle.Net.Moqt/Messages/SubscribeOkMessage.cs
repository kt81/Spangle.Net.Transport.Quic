using Spangle.Net.Moqt.Wire;

namespace Spangle.Net.Moqt.Messages;

/// <summary>
/// The SUBSCRIBE_OK response (draft-18 §10.8, type 0x4): the publisher accepts a subscription
/// and assigns the Track Alias the subscriber will match against incoming subgroup streams.
/// Sent back on the same request stream, so the Request ID is implicit.
/// </summary>
public sealed class SubscribeOkMessage
{
    /// <summary>Creates a SUBSCRIBE_OK assigning <paramref name="trackAlias"/>.</summary>
    public SubscribeOkMessage(ulong trackAlias, IReadOnlyList<MoqKeyValuePair>? parameters = null,
        IReadOnlyList<MoqKeyValuePair>? trackProperties = null)
    {
        TrackAlias = trackAlias;
        Parameters = parameters ?? [];
        TrackProperties = trackProperties ?? [];
    }

    /// <summary>The Track Alias identifying this track on the data plane.</summary>
    public ulong TrackAlias { get; }

    /// <summary>Response parameters (Key-Value-Pairs).</summary>
    public IReadOnlyList<MoqKeyValuePair> Parameters { get; }

    /// <summary>Track Properties — the trailing field, spanning the rest of the message.</summary>
    public IReadOnlyList<MoqKeyValuePair> TrackProperties { get; }

    /// <summary>Writes the SUBSCRIBE_OK payload.</summary>
    public void EncodePayload(MoqWriter writer)
    {
        ArgumentNullException.ThrowIfNull(writer);
        writer.WriteVarInt(TrackAlias);
        KeyValuePairCodec.WriteCounted(writer, [.. Parameters.OrderBy(p => p.Type)]);
        // Track Properties are the final field; their length is the remaining message bytes.
        KeyValuePairCodec.WriteList(writer, [.. TrackProperties.OrderBy(p => p.Type)]);
    }

    /// <summary>Parses a SUBSCRIBE_OK payload.</summary>
    public static SubscribeOkMessage DecodePayload(ReadOnlySpan<byte> payload)
    {
        var reader = new MoqReader(payload);
        ulong trackAlias = reader.ReadVarInt();
        int parameterCount = reader.ReadVarIntAsInt32();
        IReadOnlyList<MoqKeyValuePair> parameters = KeyValuePairCodec.ReadCounted(ref reader, parameterCount);
        IReadOnlyList<MoqKeyValuePair> trackProperties = KeyValuePairCodec.ReadList(ref reader);
        return new SubscribeOkMessage(trackAlias, parameters, trackProperties);
    }
}
