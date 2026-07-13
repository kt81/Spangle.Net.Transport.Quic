using Spangle.Net.Moqt.Wire;

namespace Spangle.Net.Moqt.Tests;

/// <summary>
/// The MOQT variable-length integer codec (draft-18 §1.4.2): the UTF-8-style leading-one-bit
/// length prefix, distinct from the QUIC RFC 9000 varint. Pinned to worked encodings (including
/// the SETUP frame type 0x2F00 -> AF 00 that the reference relay emits), the length-class
/// boundaries, and the non-minimal-decode rule.
/// </summary>
public class VarIntTests
{
    [Theory]
    [InlineData(37UL, new byte[] { 0x25 })]                                     // 1 byte
    [InlineData(100UL, new byte[] { 0x64 })]                                    // 1 byte
    [InlineData(0x2F00UL, new byte[] { 0xAF, 0x00 })]                           // SETUP frame type, 2 bytes
    [InlineData(0x3FFFUL, new byte[] { 0xBF, 0xFF })]                           // max 2-byte
    [InlineData(0x4000UL, new byte[] { 0xC0, 0x40, 0x00 })]                     // min 3-byte
    [InlineData(0x123456UL, new byte[] { 0xD2, 0x34, 0x56 })]                   // 3 bytes
    [InlineData(0xABCDEFUL, new byte[] { 0xE0, 0xAB, 0xCD, 0xEF })]             // 4 bytes
    [InlineData(0xFF_FFFF_FFFF_FFFFUL,
        new byte[] { 0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF })]         // max 8-byte
    [InlineData(0x0100_0000_0000_0000UL,
        new byte[] { 0xFF, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 })]   // min 9-byte
    [InlineData(ulong.MaxValue,
        new byte[] { 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF })]   // max 9-byte
    public void Write_ProducesTheMoqEncoding(ulong value, byte[] expected)
    {
        var buffer = new byte[9];
        int written = VarInt.Write(buffer, value);
        written.Should().Be(expected.Length);
        buffer.AsSpan(0, written).ToArray().Should().Equal(expected);
    }

    [Theory]
    [InlineData(new byte[] { 0x25 }, 37UL, 1)]
    [InlineData(new byte[] { 0xAF, 0x00 }, 0x2F00UL, 2)]
    [InlineData(new byte[] { 0xC0, 0x40, 0x00 }, 0x4000UL, 3)]
    [InlineData(new byte[] { 0xE0, 0xAB, 0xCD, 0xEF }, 0xABCDEFUL, 4)]
    [InlineData(new byte[] { 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF }, ulong.MaxValue, 9)]
    // A minimal 37 is one byte; the two-byte 0x80 0x25 is a non-minimal encoding decoding honors.
    [InlineData(new byte[] { 0x80, 0x25 }, 37UL, 2)]
    public void TryRead_DecodesMoqSamplesAndNonMinimal(byte[] source, ulong expected, int expectedLength)
    {
        VarInt.TryRead(source, out ulong value, out int bytesRead).Should().BeTrue();
        value.Should().Be(expected);
        bytesRead.Should().Be(expectedLength);
    }

    [Theory]
    [InlineData(0UL, 1)]
    [InlineData(0x7FUL, 1)]
    [InlineData(0x80UL, 2)]
    [InlineData(0x3FFFUL, 2)]
    [InlineData(0x4000UL, 3)]
    [InlineData(0x1F_FFFFUL, 3)]
    [InlineData(0x20_0000UL, 4)]
    [InlineData(0xFFF_FFFFUL, 4)]
    [InlineData(0x1000_0000UL, 5)]
    [InlineData(0x7_FFFF_FFFFUL, 5)]
    [InlineData(0x8_0000_0000UL, 6)]
    [InlineData(0x3FF_FFFF_FFFFUL, 6)]
    [InlineData(0x400_0000_0000UL, 7)]
    [InlineData(0x1_FFFF_FFFF_FFFFUL, 7)]
    [InlineData(0x2_0000_0000_0000UL, 8)]
    [InlineData(0xFF_FFFF_FFFF_FFFFUL, 8)]
    [InlineData(0x100_0000_0000_0000UL, 9)]
    [InlineData(ulong.MaxValue, 9)]
    public void GetLength_PicksTheMinimalClassAtEachBoundary(ulong value, int expectedLength)
    {
        VarInt.GetLength(value).Should().Be(expectedLength);
    }

    [Theory]
    [InlineData(0UL)]
    [InlineData(1UL)]
    [InlineData(0x7FUL)]
    [InlineData(0x80UL)]
    [InlineData(0x3FFFUL)]
    [InlineData(0x4000UL)]
    [InlineData(0xFFF_FFFFUL)]
    [InlineData(0x1000_0000UL)]
    [InlineData(0xFF_FFFF_FFFF_FFFFUL)]
    [InlineData(0x100_0000_0000_0000UL)]
    [InlineData(ulong.MaxValue)]
    public void WriteThenRead_RoundTrips(ulong value)
    {
        var buffer = new byte[9];
        int written = VarInt.Write(buffer, value);
        VarInt.TryRead(buffer.AsSpan(0, written), out ulong decoded, out int read).Should().BeTrue();
        decoded.Should().Be(value);
        read.Should().Be(written);
    }

    [Fact]
    public void TryRead_TruncatedMultiByte_ReturnsFalse()
    {
        // A first byte of 0xC0 announces a 3-byte encoding, but only two bytes are present.
        VarInt.TryRead(new byte[] { 0xC0, 0x40 }, out _, out _).Should().BeFalse();
    }

    [Fact]
    public void TryRead_EmptyBuffer_ReturnsFalse()
    {
        VarInt.TryRead(ReadOnlySpan<byte>.Empty, out _, out _).Should().BeFalse();
    }
}
