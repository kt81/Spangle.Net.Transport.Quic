using System.Buffers;
using System.Text;
using Spangle.Net.Moqt.Messages;
using Spangle.Net.Moqt.Wire;

namespace Spangle.Net.Moqt.Tests;

/// <summary>The SETUP payload (Setup Options) round-trips through encode/decode.</summary>
public class SetupMessageTests
{
    [Fact]
    public void Options_RoundTripThroughThePayload()
    {
        var setup = new SetupMessage(
        [
            MoqKeyValuePair.Varint(MoqSetupOption.MaxAuthTokenCacheSize, 8192),
            MoqKeyValuePair.FromBytes(MoqSetupOption.MoqtImplementation, Encoding.UTF8.GetBytes("spangle/0.1")),
            MoqKeyValuePair.Varint(MoqSetupOption.MaxRequestUpdates, 16),
        ]);

        var buffer = new ArrayBufferWriter<byte>();
        setup.EncodePayload(new MoqWriter(buffer));
        SetupMessage decoded = SetupMessage.DecodePayload(buffer.WrittenSpan);

        decoded.Options.Should().HaveCount(3);
        decoded.Options.Single(o => o.Type == MoqSetupOption.MaxAuthTokenCacheSize).VarintValue.Should().Be(8192UL);
        decoded.Options.Single(o => o.Type == MoqSetupOption.MaxRequestUpdates).VarintValue.Should().Be(16UL);
        Encoding.UTF8.GetString(decoded.Options.Single(o => o.Type == MoqSetupOption.MoqtImplementation).Bytes)
            .Should().Be("spangle/0.1");
    }

    [Fact]
    public void EmptySetup_RoundTripsToNoOptions()
    {
        var buffer = new ArrayBufferWriter<byte>();
        new SetupMessage().EncodePayload(new MoqWriter(buffer));
        SetupMessage.DecodePayload(buffer.WrittenSpan).Options.Should().BeEmpty();
    }
}
