using Spangle.Net.Moqt.Wire;

namespace Spangle.Net.Moqt.Tests;

/// <summary>
/// The QUIC variable-length integer codec, pinned to the exact sample encodings in
/// RFC 9000 Appendix A.1 plus the length-class boundaries and the non-minimal-decode rule.
/// </summary>
public class VarIntTests
{
    [Theory]
    // The four worked examples from RFC 9000, Appendix A.1.
    [InlineData(151288809941952652UL, new byte[] { 0xc2, 0x19, 0x7c, 0x5e, 0xff, 0x14, 0xe8, 0x8c })]
    [InlineData(494878333UL, new byte[] { 0x9d, 0x7f, 0x3e, 0x7d })]
    [InlineData(15293UL, new byte[] { 0x7b, 0xbd })]
    [InlineData(37UL, new byte[] { 0x25 })]
    public void Write_ProducesTheRfc9000Encoding(ulong value, byte[] expected)
    {
        var buffer = new byte[8];
        int written = VarInt.Write(buffer, value);
        written.Should().Be(expected.Length);
        buffer.AsSpan(0, written).ToArray().Should().Equal(expected);
    }

    [Theory]
    [InlineData(new byte[] { 0xc2, 0x19, 0x7c, 0x5e, 0xff, 0x14, 0xe8, 0x8c }, 151288809941952652UL, 8)]
    [InlineData(new byte[] { 0x9d, 0x7f, 0x3e, 0x7d }, 494878333UL, 4)]
    [InlineData(new byte[] { 0x7b, 0xbd }, 15293UL, 2)]
    [InlineData(new byte[] { 0x25 }, 37UL, 1)]
    // RFC 9000 also shows 37 encoded non-minimally in two bytes as 0x40 0x25; decoding honors it.
    [InlineData(new byte[] { 0x40, 0x25 }, 37UL, 2)]
    public void TryRead_DecodesTheRfc9000Samples(byte[] source, ulong expected, int expectedLength)
    {
        VarInt.TryRead(source, out ulong value, out int bytesRead).Should().BeTrue();
        value.Should().Be(expected);
        bytesRead.Should().Be(expectedLength);
    }

    [Theory]
    [InlineData(0UL, 1)]
    [InlineData(0x3FUL, 1)]
    [InlineData(0x40UL, 2)]
    [InlineData(0x3FFFUL, 2)]
    [InlineData(0x4000UL, 4)]
    [InlineData(0x3FFF_FFFFUL, 4)]
    [InlineData(0x4000_0000UL, 8)]
    [InlineData(VarInt.MaxValue, 8)]
    public void GetLength_PicksTheMinimalClassAtEachBoundary(ulong value, int expectedLength)
    {
        VarInt.GetLength(value).Should().Be(expectedLength);
    }

    [Theory]
    [InlineData(0UL)]
    [InlineData(1UL)]
    [InlineData(63UL)]
    [InlineData(64UL)]
    [InlineData(16383UL)]
    [InlineData(16384UL)]
    [InlineData(1073741823UL)]
    [InlineData(1073741824UL)]
    [InlineData(VarInt.MaxValue)]
    public void WriteThenRead_RoundTrips(ulong value)
    {
        var buffer = new byte[8];
        int written = VarInt.Write(buffer, value);
        VarInt.TryRead(buffer.AsSpan(0, written), out ulong decoded, out int read).Should().BeTrue();
        decoded.Should().Be(value);
        read.Should().Be(written);
    }

    [Fact]
    public void Write_AboveMaxValue_Throws()
    {
        var buffer = new byte[8];
        Action act = () => VarInt.Write(buffer, VarInt.MaxValue + 1);
        act.Should().Throw<ArgumentOutOfRangeException>();
    }

    [Fact]
    public void TryRead_TruncatedMultiByte_ReturnsFalse()
    {
        // A first byte of 0x9d announces a 4-byte encoding, but only two bytes are present.
        VarInt.TryRead(new byte[] { 0x9d, 0x7f }, out _, out _).Should().BeFalse();
    }

    [Fact]
    public void TryRead_EmptyBuffer_ReturnsFalse()
    {
        VarInt.TryRead(ReadOnlySpan<byte>.Empty, out _, out _).Should().BeFalse();
    }
}
