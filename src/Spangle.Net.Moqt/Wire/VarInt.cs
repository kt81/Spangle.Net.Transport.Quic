namespace Spangle.Net.Moqt.Wire;

/// <summary>
/// The QUIC variable-length integer (RFC 9000, Section 16), which MOQT uses for nearly
/// every field. The two most-significant bits of the first byte select a 1-, 2-, 4-, or
/// 8-byte big-endian encoding; the remaining 62 bits carry the value. This encoding is
/// stable across every MoQ draft, so it is the one wire primitive that never needs revising.
/// </summary>
public static class VarInt
{
    /// <summary>The largest value representable: 2^62 - 1.</summary>
    public const ulong MaxValue = (1UL << 62) - 1;

    /// <summary>The number of bytes <paramref name="value"/> encodes to (1, 2, 4, or 8).</summary>
    public static int GetLength(ulong value) => value switch
    {
        <= 0x3F => 1,
        <= 0x3FFF => 2,
        <= 0x3FFF_FFFF => 4,
        <= MaxValue => 8,
        _ => throw new ArgumentOutOfRangeException(nameof(value), value,
            "Value exceeds the 62-bit range of a QUIC variable-length integer."),
    };

    /// <summary>
    /// Reads one variable-length integer from the start of <paramref name="source"/>.
    /// Returns false (without throwing) if the buffer is too short to hold the full encoding.
    /// </summary>
    public static bool TryRead(ReadOnlySpan<byte> source, out ulong value, out int bytesRead)
    {
        value = 0;
        bytesRead = 0;
        if (source.IsEmpty)
        {
            return false;
        }

        int length = 1 << (source[0] >> 6); // top two bits: 0->1, 1->2, 2->4, 3->8
        if (source.Length < length)
        {
            return false;
        }

        ulong result = (ulong)(source[0] & 0x3F);
        for (int i = 1; i < length; i++)
        {
            result = (result << 8) | source[i];
        }

        value = result;
        bytesRead = length;
        return true;
    }

    /// <summary>
    /// Writes <paramref name="value"/> in its minimal encoding to the start of
    /// <paramref name="destination"/> and returns the number of bytes written.
    /// </summary>
    public static int Write(Span<byte> destination, ulong value)
    {
        int length = GetLength(value);
        if (destination.Length < length)
        {
            throw new ArgumentException("Destination is too small for the encoded value.", nameof(destination));
        }

        for (int i = length - 1; i >= 0; i--)
        {
            destination[i] = (byte)(value & 0xFF);
            value >>= 8;
        }

        // 1->0b00, 2->0b01, 4->0b10, 8->0b11 in the two most-significant bits.
        int prefix = System.Numerics.BitOperations.TrailingZeroCount(length);
        destination[0] |= (byte)(prefix << 6);
        return length;
    }
}
