using System.Buffers;
using Spangle.Net.Moqt.Wire;

namespace Spangle.Net.Moqt.Tests;

/// <summary>
/// The message reader/writer built on the varint codec: length-prefixed byte strings and
/// UTF-8 strings round-trip, positions advance correctly, and truncation is rejected.
/// </summary>
public class MoqReaderWriterTests
{
    [Fact]
    public void VarInts_RoundTripInSequence()
    {
        var output = new ArrayBufferWriter<byte>();
        var writer = new MoqWriter(output);
        writer.WriteVarInt(1);
        writer.WriteVarInt(300);
        writer.WriteVarInt(VarInt.MaxValue);

        var reader = new MoqReader(output.WrittenSpan);
        reader.ReadVarInt().Should().Be(1UL);
        reader.ReadVarInt().Should().Be(300UL);
        reader.ReadVarInt().Should().Be(VarInt.MaxValue);
        reader.End.Should().BeTrue();
    }

    [Fact]
    public void Bytes_RoundTripWithLengthPrefix()
    {
        byte[] payload = [0xDE, 0xAD, 0xBE, 0xEF];
        var output = new ArrayBufferWriter<byte>();
        new MoqWriter(output).WriteBytes(payload);

        var reader = new MoqReader(output.WrittenSpan);
        reader.ReadBytes().ToArray().Should().Equal(payload);
        reader.End.Should().BeTrue();
    }

    [Fact]
    public void Strings_RoundTripAsUtf8()
    {
        var output = new ArrayBufferWriter<byte>();
        var writer = new MoqWriter(output);
        writer.WriteString("live/カメラ1"); // multi-byte UTF-8 exercises byte-count vs char-count
        writer.WriteString(string.Empty);

        var reader = new MoqReader(output.WrittenSpan);
        reader.ReadString().Should().Be("live/カメラ1");
        reader.ReadString().Should().Be(string.Empty);
        reader.End.Should().BeTrue();
    }

    [Fact]
    public void MixedFields_RoundTripAndTrackPosition()
    {
        var output = new ArrayBufferWriter<byte>();
        var writer = new MoqWriter(output);
        writer.WriteVarInt(0x0B);
        writer.WriteString("ns");
        writer.WriteVarInt(42);

        var reader = new MoqReader(output.WrittenSpan);
        reader.ReadVarInt().Should().Be(0x0BUL);
        reader.ReadString().Should().Be("ns");
        int before = reader.Position;
        reader.ReadVarIntAsInt32().Should().Be(42);
        reader.Position.Should().BeGreaterThan(before);
        reader.Remaining.Should().Be(0);
    }

    [Fact]
    public void ReadToEnd_ReturnsTheTrailingPayload()
    {
        var output = new ArrayBufferWriter<byte>();
        var writer = new MoqWriter(output);
        writer.WriteVarInt(7);
        output.Write(new byte[] { 1, 2, 3 });

        var reader = new MoqReader(output.WrittenSpan);
        reader.ReadVarInt().Should().Be(7UL);
        reader.ReadToEnd().ToArray().Should().Equal(1, 2, 3);
        reader.End.Should().BeTrue();
    }

    [Fact]
    public void ReadBytes_LengthPastEnd_Throws()
    {
        // Claims 10 bytes follow, but the buffer holds only 2.
        byte[] buffer = [0x0A, 0x01, 0x02];
        try
        {
            var reader = new MoqReader(buffer);
            reader.ReadBytes();
            Assert.Fail("expected MoqProtocolException");
        }
        catch (MoqProtocolException)
        {
        }
    }

    [Fact]
    public void ReadVarInt_TruncatedBuffer_Throws()
    {
        byte[] buffer = [0xC0, 0x40]; // announces a 3-byte varint, only 2 present
        try
        {
            var reader = new MoqReader(buffer);
            reader.ReadVarInt();
            Assert.Fail("expected MoqProtocolException");
        }
        catch (MoqProtocolException)
        {
        }
    }
}
