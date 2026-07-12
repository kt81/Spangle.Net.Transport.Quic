using System.Buffers;
using System.Text;
using Spangle.Net.Moqt.Messages;
using Spangle.Net.Moqt.Wire;

namespace Spangle.Net.Moqt.Tests;

/// <summary>SUBSCRIBE / SUBSCRIBE_OK and the Track Name they carry round-trip through the codec.</summary>
public class SubscribeMessageTests
{
    private static ReadOnlySpan<byte> Encode(Action<MoqWriter> write, out byte[] bytes)
    {
        var buffer = new ArrayBufferWriter<byte>();
        write(new MoqWriter(buffer));
        bytes = buffer.WrittenSpan.ToArray();
        return bytes;
    }

    [Fact]
    public void Subscribe_RoundTrips()
    {
        var subscribe = new SubscribeMessage(
            requestId: 9,
            track: FullTrackName.FromStrings(["sports", "live"], "camera-1"),
            parameters: [MoqKeyValuePair.Varint(MoqSetupOption.MaxRequestUpdates, 3)]);

        Encode(subscribe.EncodePayload, out byte[] bytes);
        SubscribeMessage decoded = SubscribeMessage.DecodePayload(bytes);

        decoded.RequestId.Should().Be(9UL);
        decoded.Track.Namespace.ToStrings().Should().Equal("sports", "live");
        decoded.Track.NameAsString.Should().Be("camera-1");
        decoded.Parameters.Should().ContainSingle()
            .Which.VarintValue.Should().Be(3UL);
    }

    [Fact]
    public void SubscribeOk_RoundTrips_WithTrackAliasAndTrailingProperties()
    {
        var ok = new SubscribeOkMessage(
            trackAlias: 55,
            parameters: [MoqKeyValuePair.Varint(0x20, 128)],
            trackProperties: [MoqKeyValuePair.FromBytes(0x21, Encoding.UTF8.GetBytes("p"))]);

        Encode(ok.EncodePayload, out byte[] bytes);
        SubscribeOkMessage decoded = SubscribeOkMessage.DecodePayload(bytes);

        decoded.TrackAlias.Should().Be(55UL);
        decoded.Parameters.Should().ContainSingle().Which.VarintValue.Should().Be(128UL);
        decoded.TrackProperties.Should().ContainSingle().Which.Type.Should().Be(0x21UL);
    }

    [Fact]
    public void Namespace_RejectsMoreThan32Fields()
    {
        Action act = () => _ = new TrackNamespace([.. Enumerable.Range(0, 33).Select(_ => new byte[] { 0 })]);
        act.Should().Throw<ArgumentException>();
    }

    [Fact]
    public void EmptyNamespace_RoundTrips()
    {
        var subscribe = new SubscribeMessage(1, new FullTrackName(new TrackNamespace([]), Encoding.UTF8.GetBytes("t")));
        Encode(subscribe.EncodePayload, out byte[] bytes);
        SubscribeMessage decoded = SubscribeMessage.DecodePayload(bytes);
        decoded.Track.Namespace.Fields.Should().BeEmpty();
        decoded.Track.NameAsString.Should().Be("t");
    }
}
