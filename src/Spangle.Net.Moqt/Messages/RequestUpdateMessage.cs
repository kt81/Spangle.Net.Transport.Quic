using Spangle.Net.Moqt.Wire;

namespace Spangle.Net.Moqt.Messages;

/// <summary>
/// The REQUEST_UPDATE message (draft-18 §10.9, type 0x2): changes an in-flight request in place —
/// a subscription's priority, forward state or filter, or a namespace subscription's prefix —
/// without tearing it down. Sent on the request's own bidirectional stream; the peer answers with
/// <see cref="RequestOkMessage"/> or <see cref="RequestErrorMessage"/>.
/// </summary>
public sealed class RequestUpdateMessage
{
    /// <summary>Creates a REQUEST_UPDATE carrying the parameters to change.</summary>
    public RequestUpdateMessage(ulong requestId, IReadOnlyList<MoqKeyValuePair>? parameters = null)
    {
        RequestId = requestId;
        Parameters = parameters ?? [];
    }

    /// <summary>The request being updated.</summary>
    public ulong RequestId { get; }

    /// <summary>The parameters to change; those omitted keep their current value.</summary>
    public IReadOnlyList<MoqKeyValuePair> Parameters { get; }

    /// <summary>Writes the REQUEST_UPDATE payload.</summary>
    public void EncodePayload(MoqWriter writer)
    {
        ArgumentNullException.ThrowIfNull(writer);
        writer.WriteVarInt(RequestId);
        KeyValuePairCodec.WriteCounted(writer, [.. Parameters.OrderBy(p => p.Type)]);
    }

    /// <summary>Parses a REQUEST_UPDATE payload.</summary>
    public static RequestUpdateMessage DecodePayload(ReadOnlySpan<byte> payload)
    {
        var reader = new MoqReader(payload);
        ulong requestId = reader.ReadVarInt();
        int parameterCount = reader.ReadVarIntAsInt32();
        IReadOnlyList<MoqKeyValuePair> parameters = KeyValuePairCodec.ReadCounted(ref reader, parameterCount);
        return new RequestUpdateMessage(requestId, parameters);
    }
}
