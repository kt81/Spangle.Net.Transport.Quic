using Spangle.Net.Moqt.Wire;

namespace Spangle.Net.Moqt.Messages;

/// <summary>
/// The PUBLISH_NAMESPACE request (draft-18 §10.15, type 0x6): a publisher tells a peer (an
/// upstream relay) that it can serve tracks under a Track Namespace. The peer acknowledges with
/// REQUEST_OK (<see cref="MoqControlMessageType.RequestOk"/>) carrying the same Request ID, and
/// may then SUBSCRIBE to tracks in that namespace. Sent on the control stream.
/// </summary>
public sealed class PublishNamespaceMessage
{
    /// <summary>Creates a PUBLISH_NAMESPACE for <paramref name="namespace"/>.</summary>
    public PublishNamespaceMessage(ulong requestId, TrackNamespace @namespace,
        IReadOnlyList<MoqKeyValuePair>? parameters = null)
    {
        ArgumentNullException.ThrowIfNull(@namespace);
        RequestId = requestId;
        Namespace = @namespace;
        Parameters = parameters ?? [];
    }

    /// <summary>The request id, unique on the session (draft-18 §10.1).</summary>
    public ulong RequestId { get; }

    /// <summary>The Track Namespace being announced.</summary>
    public TrackNamespace Namespace { get; }

    /// <summary>Announce parameters as Key-Value-Pairs (often empty).</summary>
    public IReadOnlyList<MoqKeyValuePair> Parameters { get; }

    /// <summary>Writes the PUBLISH_NAMESPACE payload.</summary>
    public void EncodePayload(MoqWriter writer)
    {
        ArgumentNullException.ThrowIfNull(writer);
        writer.WriteVarInt(RequestId);
        Namespace.WriteTo(writer);
        KeyValuePairCodec.WriteCounted(writer, [.. Parameters.OrderBy(p => p.Type)]);
    }

    /// <summary>Parses a PUBLISH_NAMESPACE payload.</summary>
    public static PublishNamespaceMessage DecodePayload(ReadOnlySpan<byte> payload)
    {
        var reader = new MoqReader(payload);
        ulong requestId = reader.ReadVarInt();
        TrackNamespace @namespace = TrackNamespace.Read(ref reader);
        int parameterCount = reader.ReadVarIntAsInt32();
        IReadOnlyList<MoqKeyValuePair> parameters = KeyValuePairCodec.ReadCounted(ref reader, parameterCount);
        return new PublishNamespaceMessage(requestId, @namespace, parameters);
    }
}
