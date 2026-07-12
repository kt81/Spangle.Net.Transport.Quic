using System.Linq;
using Spangle.Net.Moqt.Wire;

namespace Spangle.Net.Moqt.Messages;

/// <summary>
/// The SETUP message (draft-18 §10.3): the first message each endpoint sends on its control
/// stream. Its payload is a sequence of Setup Options (Key-Value-Pairs); the version itself
/// is negotiated by QUIC ALPN in draft ≥ 15, so it is not carried here.
/// </summary>
public sealed class SetupMessage
{
    /// <summary>Creates a SETUP with no options.</summary>
    public SetupMessage()
        : this([])
    {
    }

    /// <summary>Creates a SETUP carrying the given Setup Options.</summary>
    public SetupMessage(IReadOnlyList<MoqKeyValuePair> options)
    {
        ArgumentNullException.ThrowIfNull(options);
        Options = options;
    }

    /// <summary>The Setup Options, keyed by <see cref="MoqSetupOption"/> type codes.</summary>
    public IReadOnlyList<MoqKeyValuePair> Options { get; }

    /// <summary>Writes the SETUP payload (the Setup Options) into <paramref name="writer"/>.</summary>
    public void EncodePayload(MoqWriter writer)
    {
        ArgumentNullException.ThrowIfNull(writer);
        // Key-Value-Pairs are delta-encoded and so must be non-decreasing by type; ordering
        // Setup Options is free (they carry no positional meaning).
        IReadOnlyList<MoqKeyValuePair> ordered = Options.OrderBy(option => option.Type).ToList();
        KeyValuePairCodec.WriteList(writer, ordered);
    }

    /// <summary>Parses a SETUP payload (already sliced to the message length) into its options.</summary>
    public static SetupMessage DecodePayload(ReadOnlySpan<byte> payload)
    {
        var reader = new MoqReader(payload);
        return new SetupMessage(KeyValuePairCodec.ReadList(ref reader));
    }
}
