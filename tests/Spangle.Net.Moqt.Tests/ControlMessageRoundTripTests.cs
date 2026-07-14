using System.Buffers;
using System.Text;
using Spangle.Net.Moqt.Messages;
using Spangle.Net.Moqt.Wire;

namespace Spangle.Net.Moqt.Tests;

/// <summary>
/// Every control message in draft-18 §10 Table: encode then decode, so each field's order, width
/// and optionality is pinned. The messages that end in a trailing field (Track Properties, an
/// optional Redirect or Request ID) are the ones most likely to drift, so each is covered both
/// with and without that field present.
/// </summary>
public class ControlMessageRoundTripTests
{
    private static byte[] Encode(Action<MoqWriter> write)
    {
        var buffer = new ArrayBufferWriter<byte>();
        write(new MoqWriter(buffer));
        return buffer.WrittenSpan.ToArray();
    }

    private static MoqKeyValuePair[] SampleParams() =>
    [
        MoqKeyValuePair.Varint(0x02, 42),
        MoqKeyValuePair.FromBytes(0x03, "token"u8.ToArray()),
    ];

    [Fact]
    public void GoAway_RoundTrips_WithAndWithoutRequestId()
    {
        // On the control stream: a server offering migration, no Request ID.
        GoAwayMessage session = GoAwayMessage.DecodePayload(
            Encode(new GoAwayMessage("moqt://relay.example/new", 5_000).EncodePayload));
        session.NewSessionUri.Should().Be("moqt://relay.example/new");
        session.Timeout.Should().Be(5_000UL);
        session.RequestId.Should().BeNull();

        // On a request stream: migrating one request, so a Request ID trails.
        GoAwayMessage perRequest = GoAwayMessage.DecodePayload(
            Encode(new GoAwayMessage(string.Empty, 0, requestId: 9).EncodePayload));
        perRequest.NewSessionUri.Should().BeEmpty("a client MUST send a zero-length URI");
        perRequest.RequestId.Should().Be(9UL);
    }

    [Fact]
    public void RequestOk_RoundTrips_WithTrackProperties()
    {
        var message = new RequestOkMessage(SampleParams(), [MoqKeyValuePair.Varint(0x04, 7)]);
        RequestOkMessage decoded = RequestOkMessage.DecodePayload(Encode(message.EncodePayload));

        decoded.Parameters.Should().HaveCount(2);
        decoded.Parameters[0].VarintValue.Should().Be(42UL);
        Encoding.UTF8.GetString(decoded.Parameters[1].Bytes).Should().Be("token");
        decoded.TrackProperties.Should().ContainSingle().Which.VarintValue.Should().Be(7UL);
    }

    [Fact]
    public void RequestError_RoundTrips_WithAndWithoutRedirect()
    {
        RequestErrorMessage plain = RequestErrorMessage.DecodePayload(
            Encode(new RequestErrorMessage(0x3, 1_000, "not supported").EncodePayload));
        plain.ErrorCode.Should().Be(0x3UL);
        plain.RetryInterval.Should().Be(1_000UL);
        plain.ErrorReason.Should().Be("not supported");
        plain.Redirect.Should().BeNull();

        var redirect = new MoqRedirect("moqt://other.example/moq", FullTrackName.FromStrings(["live"], "cam"));
        RequestErrorMessage redirected = RequestErrorMessage.DecodePayload(
            Encode(new RequestErrorMessage(0x1, 0, "go elsewhere", redirect).EncodePayload));
        redirected.Redirect.Should().NotBeNull();
        redirected.Redirect!.ConnectUri.Should().Be("moqt://other.example/moq");
        redirected.Redirect.Track.NameAsString.Should().Be("cam");
        redirected.Redirect.Track.Namespace.ToStrings().Should().Equal("live");
    }

    [Fact]
    public void RequestUpdate_RoundTrips()
    {
        RequestUpdateMessage decoded = RequestUpdateMessage.DecodePayload(
            Encode(new RequestUpdateMessage(4, SampleParams()).EncodePayload));
        decoded.RequestId.Should().Be(4UL);
        decoded.Parameters.Should().HaveCount(2);
    }

    [Fact]
    public void Publish_RoundTrips()
    {
        FullTrackName track = FullTrackName.FromStrings(["live", "demo"], "video0");
        PublishMessage decoded = PublishMessage.DecodePayload(
            Encode(new PublishMessage(2, track, trackAlias: 11, SampleParams(),
                [MoqKeyValuePair.Varint(0x06, 3)]).EncodePayload));

        decoded.RequestId.Should().Be(2UL);
        decoded.Track.Namespace.ToStrings().Should().Equal("live", "demo");
        decoded.Track.NameAsString.Should().Be("video0");
        decoded.TrackAlias.Should().Be(11UL, "the publisher assigns the alias in PUBLISH");
        decoded.Parameters.Should().HaveCount(2);
        decoded.TrackProperties.Should().ContainSingle();
    }

    [Fact]
    public void PublishDone_RoundTrips()
    {
        PublishDoneMessage decoded = PublishDoneMessage.DecodePayload(
            Encode(new PublishDoneMessage(statusCode: 0x2, streamCount: 17, "track ended").EncodePayload));
        decoded.StatusCode.Should().Be(0x2UL);
        decoded.StreamCount.Should().Be(17UL);
        decoded.ErrorReason.Should().Be("track ended");
    }

    [Fact]
    public void PublishBlocked_RoundTrips()
    {
        PublishBlockedMessage decoded = PublishBlockedMessage.DecodePayload(
            Encode(new PublishBlockedMessage(TrackNamespace.FromStrings("live", "demo"),
                Encoding.UTF8.GetBytes("video0")).EncodePayload));
        decoded.NamespaceSuffix.ToStrings().Should().Equal("live", "demo");
        Encoding.UTF8.GetString(decoded.TrackName.Span).Should().Be("video0");
    }

    [Fact]
    public void TrackStatus_RoundTrips_AndMatchesSubscribeOnTheWire()
    {
        FullTrackName track = FullTrackName.FromStrings(["live"], "cam");
        byte[] trackStatus = Encode(new TrackStatusMessage(1, track, SampleParams()).EncodePayload);

        TrackStatusMessage decoded = TrackStatusMessage.DecodePayload(trackStatus);
        decoded.RequestId.Should().Be(1UL);
        decoded.Track.NameAsString.Should().Be("cam");
        decoded.Parameters.Should().HaveCount(2);

        // draft-18 §10.14: "identical to the SUBSCRIBE message" — only the type code differs.
        byte[] subscribe = Encode(new SubscribeMessage(1, track, SampleParams()).EncodePayload);
        trackStatus.Should().Equal(subscribe);
    }

    [Fact]
    public void Fetch_Standalone_RoundTrips()
    {
        FullTrackName track = FullTrackName.FromStrings(["vod"], "movie");
        FetchMessage decoded = FetchMessage.DecodePayload(
            Encode(FetchMessage.Standalone(5, track, new MoqLocation(1, 0), new MoqLocation(9, 30),
                SampleParams()).EncodePayload));

        decoded.FetchType.Should().Be(MoqFetchType.Standalone);
        decoded.RequestId.Should().Be(5UL);
        decoded.Track!.NameAsString.Should().Be("movie");
        decoded.StartLocation.Should().Be(new MoqLocation(1, 0));
        decoded.EndLocation.Should().Be(new MoqLocation(9, 30));
        decoded.Parameters.Should().HaveCount(2);
    }

    [Fact]
    public void Fetch_Joining_RoundTrips()
    {
        FetchMessage decoded = FetchMessage.DecodePayload(
            Encode(FetchMessage.Joining(6, joiningRequestId: 3, joiningStart: 2).EncodePayload));

        decoded.FetchType.Should().Be(MoqFetchType.Joining);
        decoded.JoiningRequestId.Should().Be(3UL);
        decoded.JoiningStart.Should().Be(2UL);
        decoded.Track.Should().BeNull("a joining fetch names a subscription, not a track");
    }

    [Fact]
    public void Fetch_UnknownType_Throws()
    {
        // Fetch Type 0x3 is not defined; the reader must reject rather than mis-parse the body.
        byte[] payload = Encode(writer =>
        {
            writer.WriteVarInt(1); // request id
            writer.WriteVarInt(3); // bogus fetch type
        });

        Action act = () => FetchMessage.DecodePayload(payload);
        act.Should().Throw<MoqProtocolException>().WithMessage("*Fetch Type*");
    }

    [Fact]
    public void FetchOk_RoundTrips()
    {
        FetchOkMessage decoded = FetchOkMessage.DecodePayload(
            Encode(new FetchOkMessage(endOfTrack: true, new MoqLocation(9, 30), SampleParams(),
                [MoqKeyValuePair.Varint(0x08, 1)]).EncodePayload));

        decoded.EndOfTrack.Should().BeTrue();
        decoded.EndLocation.Should().Be(new MoqLocation(9, 30));
        decoded.Parameters.Should().HaveCount(2);
        decoded.TrackProperties.Should().ContainSingle();
    }

    [Fact]
    public void Namespace_And_NamespaceDone_RoundTrip()
    {
        NamespaceMessage ns = NamespaceMessage.DecodePayload(
            Encode(new NamespaceMessage(TrackNamespace.FromStrings("live", "room7")).EncodePayload));
        ns.NamespaceSuffix.ToStrings().Should().Equal("live", "room7");

        NamespaceDoneMessage done = NamespaceDoneMessage.DecodePayload(
            Encode(new NamespaceDoneMessage(TrackNamespace.FromStrings("live", "room7")).EncodePayload));
        done.NamespaceSuffix.ToStrings().Should().Equal("live", "room7");
    }

    [Fact]
    public void SubscribeNamespace_And_SubscribeTracks_RoundTrip()
    {
        SubscribeNamespaceMessage ns = SubscribeNamespaceMessage.DecodePayload(
            Encode(new SubscribeNamespaceMessage(7, TrackNamespace.FromStrings("live"), SampleParams())
                .EncodePayload));
        ns.RequestId.Should().Be(7UL);
        ns.NamespacePrefix.ToStrings().Should().Equal("live");
        ns.Parameters.Should().HaveCount(2);

        SubscribeTracksMessage tracks = SubscribeTracksMessage.DecodePayload(
            Encode(new SubscribeTracksMessage(8, TrackNamespace.FromStrings("live"), SampleParams()).EncodePayload));
        tracks.RequestId.Should().Be(8UL);
        tracks.NamespacePrefix.ToStrings().Should().Equal("live");
    }
}
