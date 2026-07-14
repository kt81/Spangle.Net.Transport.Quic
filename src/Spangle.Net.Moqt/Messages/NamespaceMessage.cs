using Spangle.Net.Moqt.Wire;

namespace Spangle.Net.Moqt.Messages;

/// <summary>
/// The NAMESPACE message (draft-18 §10.16, type 0x8): a publisher tells a namespace subscriber
/// that a namespace matching its prefix now exists. Sent on the SUBSCRIBE_NAMESPACE request's own
/// bidirectional stream, so the Request ID is implicit and only the part of the namespace beyond
/// the subscribed prefix travels.
/// </summary>
public sealed class NamespaceMessage
{
    /// <summary>Creates a NAMESPACE announcing <paramref name="namespaceSuffix"/>.</summary>
    public NamespaceMessage(TrackNamespace namespaceSuffix)
    {
        ArgumentNullException.ThrowIfNull(namespaceSuffix);
        NamespaceSuffix = namespaceSuffix;
    }

    /// <summary>The namespace fields beyond the subscribed prefix.</summary>
    public TrackNamespace NamespaceSuffix { get; }

    /// <summary>Writes the NAMESPACE payload.</summary>
    public void EncodePayload(MoqWriter writer)
    {
        ArgumentNullException.ThrowIfNull(writer);
        NamespaceSuffix.WriteTo(writer);
    }

    /// <summary>Parses a NAMESPACE payload.</summary>
    public static NamespaceMessage DecodePayload(ReadOnlySpan<byte> payload)
    {
        var reader = new MoqReader(payload);
        return new NamespaceMessage(TrackNamespace.Read(ref reader));
    }
}

/// <summary>
/// The NAMESPACE_DONE message (draft-18 §10.17, type 0xE): the mirror of
/// <see cref="NamespaceMessage"/> — a previously announced namespace is gone.
/// </summary>
public sealed class NamespaceDoneMessage
{
    /// <summary>Creates a NAMESPACE_DONE for <paramref name="namespaceSuffix"/>.</summary>
    public NamespaceDoneMessage(TrackNamespace namespaceSuffix)
    {
        ArgumentNullException.ThrowIfNull(namespaceSuffix);
        NamespaceSuffix = namespaceSuffix;
    }

    /// <summary>The namespace fields beyond the subscribed prefix.</summary>
    public TrackNamespace NamespaceSuffix { get; }

    /// <summary>Writes the NAMESPACE_DONE payload.</summary>
    public void EncodePayload(MoqWriter writer)
    {
        ArgumentNullException.ThrowIfNull(writer);
        NamespaceSuffix.WriteTo(writer);
    }

    /// <summary>Parses a NAMESPACE_DONE payload.</summary>
    public static NamespaceDoneMessage DecodePayload(ReadOnlySpan<byte> payload)
    {
        var reader = new MoqReader(payload);
        return new NamespaceDoneMessage(TrackNamespace.Read(ref reader));
    }
}
