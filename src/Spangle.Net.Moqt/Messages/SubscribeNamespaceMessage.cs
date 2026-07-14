using Spangle.Net.Moqt.Wire;

namespace Spangle.Net.Moqt.Messages;

/// <summary>
/// The SUBSCRIBE_NAMESPACE request (draft-18 §10.18, type 0x50): asks to be told about every
/// namespace beginning with a prefix, so a subscriber can discover what a publisher offers. The
/// peer answers REQUEST_OK, then streams <see cref="NamespaceMessage"/> and
/// <see cref="NamespaceDoneMessage"/> on the same request stream as namespaces come and go.
/// </summary>
public sealed class SubscribeNamespaceMessage
{
    /// <summary>Creates a SUBSCRIBE_NAMESPACE for <paramref name="namespacePrefix"/>.</summary>
    public SubscribeNamespaceMessage(ulong requestId, TrackNamespace namespacePrefix,
        IReadOnlyList<MoqKeyValuePair>? parameters = null)
    {
        ArgumentNullException.ThrowIfNull(namespacePrefix);
        RequestId = requestId;
        NamespacePrefix = namespacePrefix;
        Parameters = parameters ?? [];
    }

    /// <summary>The request id, unique on the session (draft-18 §10.1).</summary>
    public ulong RequestId { get; }

    /// <summary>The prefix every announced namespace must begin with.</summary>
    public TrackNamespace NamespacePrefix { get; }

    /// <summary>Request parameters (Key-Value-Pairs).</summary>
    public IReadOnlyList<MoqKeyValuePair> Parameters { get; }

    /// <summary>Writes the SUBSCRIBE_NAMESPACE payload.</summary>
    public void EncodePayload(MoqWriter writer)
    {
        ArgumentNullException.ThrowIfNull(writer);
        writer.WriteVarInt(RequestId);
        NamespacePrefix.WriteTo(writer);
        KeyValuePairCodec.WriteCounted(writer, [.. Parameters.OrderBy(p => p.Type)]);
    }

    /// <summary>Parses a SUBSCRIBE_NAMESPACE payload.</summary>
    public static SubscribeNamespaceMessage DecodePayload(ReadOnlySpan<byte> payload)
    {
        var reader = new MoqReader(payload);
        ulong requestId = reader.ReadVarInt();
        TrackNamespace prefix = TrackNamespace.Read(ref reader);
        int parameterCount = reader.ReadVarIntAsInt32();
        IReadOnlyList<MoqKeyValuePair> parameters = KeyValuePairCodec.ReadCounted(ref reader, parameterCount);
        return new SubscribeNamespaceMessage(requestId, prefix, parameters);
    }
}

/// <summary>
/// The SUBSCRIBE_TRACKS request (draft-18 §10.19, type 0x51): the track-level counterpart of
/// <see cref="SubscribeNamespaceMessage"/> — subscribe to every track under a namespace prefix,
/// rather than naming each one. Same wire shape as SUBSCRIBE_NAMESPACE.
/// </summary>
public sealed class SubscribeTracksMessage
{
    /// <summary>Creates a SUBSCRIBE_TRACKS for <paramref name="namespacePrefix"/>.</summary>
    public SubscribeTracksMessage(ulong requestId, TrackNamespace namespacePrefix,
        IReadOnlyList<MoqKeyValuePair>? parameters = null)
    {
        ArgumentNullException.ThrowIfNull(namespacePrefix);
        RequestId = requestId;
        NamespacePrefix = namespacePrefix;
        Parameters = parameters ?? [];
    }

    /// <summary>The request id, unique on the session (draft-18 §10.1).</summary>
    public ulong RequestId { get; }

    /// <summary>The prefix every matching track's namespace must begin with.</summary>
    public TrackNamespace NamespacePrefix { get; }

    /// <summary>Request parameters (Key-Value-Pairs).</summary>
    public IReadOnlyList<MoqKeyValuePair> Parameters { get; }

    /// <summary>Writes the SUBSCRIBE_TRACKS payload.</summary>
    public void EncodePayload(MoqWriter writer)
    {
        ArgumentNullException.ThrowIfNull(writer);
        writer.WriteVarInt(RequestId);
        NamespacePrefix.WriteTo(writer);
        KeyValuePairCodec.WriteCounted(writer, [.. Parameters.OrderBy(p => p.Type)]);
    }

    /// <summary>Parses a SUBSCRIBE_TRACKS payload.</summary>
    public static SubscribeTracksMessage DecodePayload(ReadOnlySpan<byte> payload)
    {
        var reader = new MoqReader(payload);
        ulong requestId = reader.ReadVarInt();
        TrackNamespace prefix = TrackNamespace.Read(ref reader);
        int parameterCount = reader.ReadVarIntAsInt32();
        IReadOnlyList<MoqKeyValuePair> parameters = KeyValuePairCodec.ReadCounted(ref reader, parameterCount);
        return new SubscribeTracksMessage(requestId, prefix, parameters);
    }
}
