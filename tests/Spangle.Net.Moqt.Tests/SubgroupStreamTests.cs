using System.Net;
using System.Net.Security;
using System.Text;
using Spangle.Net.Moqt.Data;
using Spangle.Net.Moqt.Wire;
using Spangle.Net.Transport.Quic;
using Spangle.Net.Transport.Quic.InMemory;

namespace Spangle.Net.Moqt.Tests;

/// <summary>
/// The subgroup data plane (draft-18 §11.4.2): the SUBGROUP_HEADER type-bit encoding, and a
/// full write→read of objects over a stream — delta-encoded Object IDs (sequential and
/// gapped), payloads, and a zero-length status object. Runs over the in-memory transport.
/// </summary>
public class SubgroupStreamTests
{
    private static readonly SslApplicationProtocol Alpn = new(MoqtConstants.Alpn);

    [Fact]
    public void Type_EncodesTheFieldPresenceBits()
    {
        // base 0x10 | explicit subgroup id (0b10 -> 0x04) | end-of-group (0x08) | first-object (0x40)
        new SubgroupHeader { SubgroupIdMode = SubgroupIdMode.Explicit, EndOfGroup = true, FirstObject = true }
            .Type.Should().Be(0x5CUL);

        // base 0x10 | properties (0x01) | subgroup-id mode 0 | default/inherit priority (0x20)
        new SubgroupHeader { SubgroupIdMode = SubgroupIdMode.Zero, HasProperties = true, InheritPriority = true }
            .Type.Should().Be(0x31UL);
    }

    [Fact]
    public async Task SubgroupStream_FirstObjectAndInheritedPriority_RoundTripWithNoExtraFields()
    {
        // FIRST_OBJECT is a semantic bit (draft-18 §2.2) that adds no header field, and
        // DEFAULT_PRIORITY omits the priority byte. With both set, the object must still parse
        // — proving the reader consumes exactly the fields the type selects and none extra.
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        CancellationToken ct = cts.Token;

        var transport = new InMemoryQuicTransport();
        await using IQuicServer server = await transport.ListenAsync(new QuicServerOptions
        {
            ListenEndPoint = new IPEndPoint(IPAddress.Loopback, 0),
            ApplicationProtocols = [Alpn],
        }, ct);

        ValueTask<IQuicConnection> acceptTask = server.AcceptConnectionAsync(ct);
        await using IQuicConnection clientConn = await transport.ConnectAsync(new QuicClientOptions
        {
            RemoteEndPoint = server.LocalEndPoint,
            ApplicationProtocols = [Alpn],
        }, ct);
        await using IQuicConnection serverConn = await acceptTask;

        var header = new SubgroupHeader
        {
            TrackAlias = 9,
            GroupId = 4,
            SubgroupIdMode = SubgroupIdMode.Explicit,
            SubgroupId = 2,
            FirstObject = true,
            InheritPriority = true, // priority byte omitted
        };

        await using IQuicStream outbound = await clientConn.OpenStreamAsync(QuicStreamDirection.Unidirectional, ct);
        var writer = new SubgroupStreamWriter(outbound, header);
        await writer.WriteObjectAsync(MoqObject.Normal(4, 0, 2, 0, Encoding.UTF8.GetBytes("x")), ct);
        await writer.CompleteAsync(ct);

        await using IQuicStream inbound = await serverConn.AcceptStreamAsync(ct);
        var reader = await SubgroupStreamReader.OpenAsync(inbound, ct);

        reader.Header.FirstObject.Should().BeTrue();
        reader.Header.InheritPriority.Should().BeTrue();
        reader.Header.TrackAlias.Should().Be(9UL);
        reader.Header.SubgroupId.Should().Be(2UL);

        MoqObject? first = await reader.ReadObjectAsync(ct);
        first.Should().NotBeNull();
        first!.ObjectId.Should().Be(0UL);
        Encoding.UTF8.GetString(first.Payload.Span).Should().Be("x");
        (await reader.ReadObjectAsync(ct)).Should().BeNull(); // clean end, nothing miscounted
    }

    [Fact]
    public async Task SubgroupStream_ObjectExtensionHeaders_RoundTrip()
    {
        // With the Properties bit set, every object carries a byte-length-prefixed block of
        // Key-Value-Pairs. This is where a media mapping puts its per-frame metadata — the shape
        // used here mirrors draft-cenzano-moq-media-interop: an even-typed varint (media type)
        // plus odd-typed byte strings (a packed metadata blob and the codec extradata).
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        CancellationToken ct = cts.Token;

        var transport = new InMemoryQuicTransport();
        await using IQuicServer server = await transport.ListenAsync(new QuicServerOptions
        {
            ListenEndPoint = new IPEndPoint(IPAddress.Loopback, 0),
            ApplicationProtocols = [Alpn],
        }, ct);

        ValueTask<IQuicConnection> acceptTask = server.AcceptConnectionAsync(ct);
        await using IQuicConnection clientConn = await transport.ConnectAsync(new QuicClientOptions
        {
            RemoteEndPoint = server.LocalEndPoint,
            ApplicationProtocols = [Alpn],
        }, ct);
        await using IQuicConnection serverConn = await acceptTask;

        var header = new SubgroupHeader
        {
            TrackAlias = 7,
            GroupId = 1,
            SubgroupIdMode = SubgroupIdMode.Explicit,
            SubgroupId = 0,
            HasProperties = true,
            PublisherPriority = 10,
        };
        header.Type.Should().Be(0x15UL, "base 0x10 | properties 0x01 | explicit subgroup id 0x04");

        MoqKeyValuePair[] extensions =
        [
            MoqKeyValuePair.Varint(0x0A, 0x00),                       // media type (even -> varint)
            MoqKeyValuePair.FromBytes(0x0D, [0x01, 0x64, 0x00, 0x1F]), // extradata (odd -> bytes)
            MoqKeyValuePair.FromBytes(0x15, [0x00, 0x2A, 0x2A]),       // packed metadata (odd -> bytes)
        ];

        await using IQuicStream outbound = await clientConn.OpenStreamAsync(QuicStreamDirection.Unidirectional, ct);
        var writer = new SubgroupStreamWriter(outbound, header);
        await writer.WriteObjectAsync(
            MoqObject.Normal(1, 0, 0, 10, Encoding.UTF8.GetBytes("frame0"), extensions), ct);
        // An object with no extensions on the same stream still writes an (empty) block.
        await writer.WriteObjectAsync(MoqObject.Normal(1, 1, 0, 10, Encoding.UTF8.GetBytes("frame1")), ct);
        await writer.CompleteAsync(ct);

        await using IQuicStream inbound = await serverConn.AcceptStreamAsync(ct);
        var reader = await SubgroupStreamReader.OpenAsync(inbound, ct);
        reader.Header.HasProperties.Should().BeTrue();

        MoqObject? first = await reader.ReadObjectAsync(ct);
        first.Should().NotBeNull();
        Encoding.UTF8.GetString(first!.Payload.Span).Should().Be("frame0");
        first.Extensions.Should().HaveCount(3);
        first.Extensions[0].Type.Should().Be(0x0AUL);
        first.Extensions[0].IsBytes.Should().BeFalse();
        first.Extensions[0].VarintValue.Should().Be(0x00UL);
        first.Extensions[1].Type.Should().Be(0x0DUL);
        first.Extensions[1].Bytes.ToArray().Should().Equal([0x01, 0x64, 0x00, 0x1F]);
        first.Extensions[2].Type.Should().Be(0x15UL);
        first.Extensions[2].Bytes.ToArray().Should().Equal([0x00, 0x2A, 0x2A]);

        MoqObject? second = await reader.ReadObjectAsync(ct);
        second.Should().NotBeNull();
        Encoding.UTF8.GetString(second!.Payload.Span).Should().Be("frame1");
        second.Extensions.Should().BeEmpty("an empty block round-trips as no extensions");

        (await reader.ReadObjectAsync(ct)).Should().BeNull();
    }

    [Fact]
    public async Task SubgroupStream_RoundTripsHeaderAndObjects()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        CancellationToken ct = cts.Token;

        var transport = new InMemoryQuicTransport();
        await using IQuicServer server = await transport.ListenAsync(new QuicServerOptions
        {
            ListenEndPoint = new IPEndPoint(IPAddress.Loopback, 0),
            ApplicationProtocols = [Alpn],
        }, ct);

        ValueTask<IQuicConnection> acceptTask = server.AcceptConnectionAsync(ct);
        await using IQuicConnection clientConn = await transport.ConnectAsync(new QuicClientOptions
        {
            RemoteEndPoint = server.LocalEndPoint,
            ApplicationProtocols = [Alpn],
        }, ct);
        await using IQuicConnection serverConn = await acceptTask;

        var header = new SubgroupHeader
        {
            TrackAlias = 7,
            GroupId = 3,
            SubgroupIdMode = SubgroupIdMode.Explicit,
            SubgroupId = 1,
            PublisherPriority = 128,
        };

        await using IQuicStream outbound = await clientConn.OpenStreamAsync(QuicStreamDirection.Unidirectional, ct);
        var writer = new SubgroupStreamWriter(outbound, header);
        await writer.WriteObjectAsync(MoqObject.Normal(3, 0, 1, 128, Encoding.UTF8.GetBytes("a")), ct);
        await writer.WriteObjectAsync(MoqObject.Normal(3, 1, 1, 128, Encoding.UTF8.GetBytes("bb")), ct); // sequential
        await writer.WriteObjectAsync(MoqObject.Normal(3, 5, 1, 128, Encoding.UTF8.GetBytes("ccc")), ct); // gap
        await writer.WriteObjectAsync(
            new MoqObject(3, 6, 1, 128, MoqObjectStatus.EndOfGroup, ReadOnlyMemory<byte>.Empty), ct);
        await writer.CompleteAsync(ct);

        await using IQuicStream inbound = await serverConn.AcceptStreamAsync(ct);
        var reader = await SubgroupStreamReader.OpenAsync(inbound, ct);

        reader.Header.TrackAlias.Should().Be(7UL);
        reader.Header.GroupId.Should().Be(3UL);
        reader.Header.SubgroupId.Should().Be(1UL);
        reader.Header.PublisherPriority.Should().Be((byte)128);

        var objects = new List<MoqObject>();
        while (await reader.ReadObjectAsync(ct) is { } moqObject)
        {
            objects.Add(moqObject);
        }

        objects.Select(o => o.ObjectId).Should().Equal(0UL, 1UL, 5UL, 6UL);
        objects.Should().OnlyContain(o => o.SubgroupId == 1UL && o.GroupId == 3UL);
        Encoding.UTF8.GetString(objects[0].Payload.Span).Should().Be("a");
        Encoding.UTF8.GetString(objects[1].Payload.Span).Should().Be("bb");
        Encoding.UTF8.GetString(objects[2].Payload.Span).Should().Be("ccc");
        objects[3].Status.Should().Be(MoqObjectStatus.EndOfGroup);
        objects[3].Payload.IsEmpty.Should().BeTrue();
    }
}
