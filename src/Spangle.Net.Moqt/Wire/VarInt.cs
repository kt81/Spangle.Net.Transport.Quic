using System.Buffers.Binary;
using System.Numerics;

namespace Spangle.Net.Moqt.Wire;

/// <summary>
/// The MOQT variable-length integer (draft-ietf-moq-transport-18 §1.4.2), which is distinct
/// from the QUIC RFC 9000 varint: the count of leading one-bits in the first byte selects the
/// encoded length, UTF-8 style. <c>0xxxxxxx</c> is 1 byte (7-bit value), <c>10xxxxxx…</c> is 2
/// bytes (14-bit), <c>110xxxxx…</c> is 3 bytes (21-bit), and so on to <c>11111110…</c> at 8
/// bytes (56-bit) and <c>11111111</c> + 8 bytes at 9 bytes (a full 64-bit value). The value
/// occupies the bits after the prefix, big-endian. The encoder emits the minimal form; the
/// decoder also accepts non-minimal encodings (as the spec allows).
/// </summary>
public static class VarInt
{
    /// <summary>The largest value representable: the full 64-bit range (a 9-byte encoding).</summary>
    public const ulong MaxValue = ulong.MaxValue;

    /// <summary>
    /// The encoded length (1–9) a var-int occupies, read from its first byte: one more than the
    /// number of leading one-bits (with <c>0xFF</c> selecting the 9-byte form). The one place
    /// this decode lives, so the wire reader, the control-message framer, and the stream reader
    /// all agree.
    /// </summary>
    public static int GetEncodedLength(byte firstByte)
    {
        int leadingOnes = BitOperations.LeadingZeroCount((uint)(byte)~firstByte) - 24;
        return Math.Min(leadingOnes + 1, 9);
    }

    /// <summary>The number of bytes <paramref name="value"/> encodes to in its minimal form (1–9).</summary>
    public static int GetLength(ulong value) => value switch
    {
        <= 0x7FUL => 1,
        <= 0x3FFFUL => 2,
        <= 0x1F_FFFFUL => 3,
        <= 0xFFF_FFFFUL => 4,
        <= 0x7_FFFF_FFFFUL => 5,
        <= 0x3FF_FFFF_FFFFUL => 6,
        <= 0x1_FFFF_FFFF_FFFFUL => 7,
        <= 0xFF_FFFF_FFFF_FFFFUL => 8,
        _ => 9,
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

        int length = GetEncodedLength(source[0]);
        if (source.Length < length)
        {
            return false;
        }

        ulong result;
        if (length == 9)
        {
            // 0xFF prefix byte carries no value bits; the value is the next 8 bytes big-endian.
            result = BinaryPrimitives.ReadUInt64BigEndian(source[1..9]);
        }
        else
        {
            // The first byte holds (8 - length) value bits below its length prefix.
            result = (ulong)(source[0] & ((1 << (8 - length)) - 1));
            for (int i = 1; i < length; i++)
            {
                result = (result << 8) | source[i];
            }
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

        if (length == 9)
        {
            destination[0] = 0xFF;
            BinaryPrimitives.WriteUInt64BigEndian(destination[1..9], value);
            return 9;
        }

        // Lay the value big-endian across `length` bytes; its top `length` bits are zero (the
        // value fits in 7*length bits), leaving room for the prefix.
        for (int i = length - 1; i >= 0; i--)
        {
            destination[i] = (byte)(value & 0xFF);
            value >>= 8;
        }

        // OR in the prefix: (length - 1) leading one-bits, i.e. 0xFF shifted left by (9 - length).
        destination[0] |= (byte)(0xFF << (9 - length));
        return length;
    }
}
