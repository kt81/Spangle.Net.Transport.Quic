namespace Spangle.Net.Moqt.Wire;

/// <summary>
/// A MOQT Key-Value-Pair (draft-18 §1.4.3): the even/odd type rule decides the value's
/// shape — an even type carries a single varint, an odd type carries a length-prefixed byte
/// string (max 2^16-1 bytes). Used for Setup Options and message parameters.
/// </summary>
public readonly struct MoqKeyValuePair
{
    private readonly byte[]? _bytes;

    private MoqKeyValuePair(ulong type, ulong varintValue, byte[]? bytes)
    {
        Type = type;
        VarintValue = varintValue;
        _bytes = bytes;
    }

    /// <summary>The pair's type; its parity selects the value serialization.</summary>
    public ulong Type { get; }

    /// <summary>The value when <see cref="IsBytes"/> is false (even type).</summary>
    public ulong VarintValue { get; }

    /// <summary>Whether the value is a byte string (odd type) rather than a varint.</summary>
    public bool IsBytes => (Type & 1UL) == 1UL;

    /// <summary>The value when <see cref="IsBytes"/> is true (odd type).</summary>
    public ReadOnlySpan<byte> Bytes => _bytes;

    /// <summary>An even-typed pair carrying a single varint value.</summary>
    public static MoqKeyValuePair Varint(ulong type, ulong value)
    {
        if ((type & 1UL) != 0UL)
        {
            throw new ArgumentException("An even type is required for a varint value.", nameof(type));
        }

        return new MoqKeyValuePair(type, value, null);
    }

    /// <summary>An odd-typed pair carrying a length-prefixed byte string.</summary>
    public static MoqKeyValuePair FromBytes(ulong type, ReadOnlySpan<byte> value)
    {
        if ((type & 1UL) == 0UL)
        {
            throw new ArgumentException("An odd type is required for a byte value.", nameof(type));
        }

        if (value.Length > ushort.MaxValue)
        {
            throw new ArgumentException("A Key-Value-Pair value may not exceed 2^16-1 bytes.", nameof(value));
        }

        return new MoqKeyValuePair(type, 0, value.ToArray());
    }
}

/// <summary>
/// Reads and writes sequences of <see cref="MoqKeyValuePair"/>. Types are delta-encoded
/// against the previous type (draft-18 §1.4.3), so a sequence must be in non-decreasing
/// type order; the reader reconstructs the absolute types by running sum.
/// </summary>
public static class KeyValuePairCodec
{
    /// <summary>Writes the pairs, delta-encoding each type against the previous.</summary>
    public static void WriteList(MoqWriter writer, IReadOnlyList<MoqKeyValuePair> pairs)
    {
        ArgumentNullException.ThrowIfNull(writer);
        ArgumentNullException.ThrowIfNull(pairs);

        ulong previousType = 0;
        foreach (MoqKeyValuePair pair in pairs)
        {
            if (pair.Type < previousType)
            {
                throw new ArgumentException("Key-Value-Pairs must be in non-decreasing type order.", nameof(pairs));
            }

            writer.WriteVarInt(pair.Type - previousType);
            previousType = pair.Type;

            if (pair.IsBytes)
            {
                writer.WriteBytes(pair.Bytes);
            }
            else
            {
                writer.WriteVarInt(pair.VarintValue);
            }
        }
    }

    /// <summary>Reads pairs until the reader is exhausted (the caller bounds the buffer).</summary>
    public static IReadOnlyList<MoqKeyValuePair> ReadList(ref MoqReader reader)
    {
        var pairs = new List<MoqKeyValuePair>();
        ulong previousType = 0;
        while (!reader.End)
        {
            ulong type = previousType + reader.ReadVarInt();
            previousType = type;

            pairs.Add((type & 1UL) == 0UL
                ? MoqKeyValuePair.Varint(type, reader.ReadVarInt())
                : MoqKeyValuePair.FromBytes(type, reader.ReadBytes()));
        }

        return pairs;
    }
}
