using System.Buffers;
using Spangle.Net.Moqt.Wire;

namespace Spangle.Net.Moqt.Tests;

/// <summary>
/// The Key-Value-Pair codec (draft-18 §1.4.3): even types carry a varint, odd types carry
/// length-prefixed bytes, and types are delta-encoded so a sequence must be non-decreasing.
/// </summary>
public class KeyValuePairTests
{
    private static IReadOnlyList<MoqKeyValuePair> RoundTrip(IReadOnlyList<MoqKeyValuePair> pairs)
    {
        var output = new ArrayBufferWriter<byte>();
        KeyValuePairCodec.WriteList(new MoqWriter(output), pairs);
        var reader = new MoqReader(output.WrittenSpan);
        return KeyValuePairCodec.ReadList(ref reader);
    }

    [Fact]
    public void EvenType_CarriesAVarint()
    {
        IReadOnlyList<MoqKeyValuePair> decoded = RoundTrip([MoqKeyValuePair.Varint(0x04, 4096)]);
        decoded.Should().HaveCount(1);
        decoded[0].Type.Should().Be(0x04UL);
        decoded[0].IsBytes.Should().BeFalse();
        decoded[0].VarintValue.Should().Be(4096UL);
    }

    [Fact]
    public void OddType_CarriesLengthPrefixedBytes()
    {
        byte[] value = [0x01, 0x02, 0x03];
        IReadOnlyList<MoqKeyValuePair> decoded = RoundTrip([MoqKeyValuePair.FromBytes(0x05, value)]);
        decoded.Should().HaveCount(1);
        decoded[0].Type.Should().Be(0x05UL);
        decoded[0].IsBytes.Should().BeTrue();
        decoded[0].Bytes.ToArray().Should().Equal(value);
    }

    [Fact]
    public void MixedSequence_DeltaEncodesAndReconstructsTypes()
    {
        IReadOnlyList<MoqKeyValuePair> decoded = RoundTrip(
        [
            MoqKeyValuePair.Varint(0x04, 10),
            MoqKeyValuePair.FromBytes(0x05, [0xAA]),
            MoqKeyValuePair.Varint(0x08, 20),
            MoqKeyValuePair.FromBytes(0x09, [0xBB, 0xCC]),
        ]);

        decoded.Select(p => p.Type).Should().Equal(0x04UL, 0x05UL, 0x08UL, 0x09UL);
        decoded[0].VarintValue.Should().Be(10UL);
        decoded[1].Bytes.ToArray().Should().Equal(new byte[] { 0xAA });
        decoded[2].VarintValue.Should().Be(20UL);
        decoded[3].Bytes.ToArray().Should().Equal(new byte[] { 0xBB, 0xCC });
    }

    [Fact]
    public void RepeatedType_EncodesAsZeroDelta()
    {
        // Two pairs of the same even type -> second delta is 0; both must survive.
        IReadOnlyList<MoqKeyValuePair> decoded = RoundTrip(
        [
            MoqKeyValuePair.Varint(0x06, 1),
            MoqKeyValuePair.Varint(0x06, 2),
        ]);
        decoded.Select(p => p.VarintValue).Should().Equal(1UL, 2UL);
    }

    [Fact]
    public void DecreasingType_IsRejected()
    {
        var output = new ArrayBufferWriter<byte>();
        Action act = () => KeyValuePairCodec.WriteList(new MoqWriter(output),
            [MoqKeyValuePair.Varint(0x08, 1), MoqKeyValuePair.Varint(0x04, 2)]);
        act.Should().Throw<ArgumentException>();
    }

    [Fact]
    public void Varint_WithOddType_IsRejected()
    {
        Action act = () => MoqKeyValuePair.Varint(0x05, 1);
        act.Should().Throw<ArgumentException>();
    }

    [Fact]
    public void FromBytes_WithEvenType_IsRejected()
    {
        Action act = () => MoqKeyValuePair.FromBytes(0x04, [0x00]);
        act.Should().Throw<ArgumentException>();
    }
}
