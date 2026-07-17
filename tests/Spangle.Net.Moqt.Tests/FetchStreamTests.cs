using System.Net;
using System.Net.Security;
using System.Text;
using Spangle.Net.Moqt.Data;
using Spangle.Net.Moqt.Wire;
using Spangle.Net.Transport.Quic;
using Spangle.Net.Transport.Quic.InMemory;

namespace Spangle.Net.Moqt.Tests;

/// <summary>
/// The fetch data plane (draft-18 §11.4.4). A fetch stream hoists nothing into its header, so
/// every object codes itself against the one before it and a Serialization Flags varint says
/// which fields that let it drop — which makes the running state, not any single field, the
/// thing most likely to be wrong. The encoding tests here pin exact bytes rather than only
/// round-tripping, because a writer and reader that share a misreading round-trip green.
/// </summary>
public class FetchStreamTests
{
    private static readonly SslApplicationProtocol Alpn = new(MoqtConstants.Alpn);

    private sealed class Pair : IAsyncDisposable
    {
        public required IQuicServer Server { get; init; }
        public required IQuicConnection Client { get; init; }
        public required IQuicConnection Peer { get; init; }

        public async ValueTask DisposeAsync()
        {
            await Client.DisposeAsync();
            await Peer.DisposeAsync();
            await Server.DisposeAsync();
        }
    }

    private static async Task<Pair> ConnectAsync(CancellationToken ct)
    {
        var transport = new InMemoryQuicTransport();
        IQuicServer server = await transport.ListenAsync(new QuicServerOptions
        {
            ListenEndPoint = new IPEndPoint(IPAddress.Loopback, 0),
            ApplicationProtocols = [Alpn],
        }, ct);

        ValueTask<IQuicConnection> acceptTask = server.AcceptConnectionAsync(ct);
        IQuicConnection client = await transport.ConnectAsync(new QuicClientOptions
        {
            RemoteEndPoint = server.LocalEndPoint,
            ApplicationProtocols = [Alpn],
        }, ct);

        return new Pair { Server = server, Client = client, Peer = await acceptTask };
    }

    private static async Task<byte[]> DrainAsync(IQuicStream stream, CancellationToken ct)
    {
        var all = new List<byte>();
        var buffer = new byte[256];
        int read;
        while ((read = await stream.ReadAsync(buffer, ct)) > 0)
        {
            all.AddRange(buffer.AsSpan(0, read).ToArray());
        }

        return [.. all];
    }

    [Fact]
    public async Task FetchStream_EncodesTheExactBytesTheSpecDescribes()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        CancellationToken ct = cts.Token;
        await using Pair pair = await ConnectAsync(ct);

        await using IQuicStream outbound = await pair.Client.OpenStreamAsync(QuicStreamDirection.Unidirectional, ct);
        var writer = new FetchStreamWriter(outbound, new FetchHeader(4));

        // First object: the spec makes it carry absolute Group and Object ids, and an explicit
        // priority, since it has no prior object to code against.
        await writer.WriteObjectAsync(MoqObject.Normal(7, 3, 0, 128, Encoding.UTF8.GetBytes("hi")), ct);

        // Second: same group, next object id, same subgroup and priority — every field falls away
        // and the flags go to zero. This is what the delta encoding is for.
        await writer.WriteObjectAsync(MoqObject.Normal(7, 4, 0, 128, Encoding.UTF8.GetBytes("yo")), ct);

        // Third: zero length. A subgroup stream would follow the zero with an Object Status
        // varint; a FETCH has no such field (§11.2.1.1), so the object ends at the length.
        await writer.WriteObjectAsync(MoqObject.Normal(7, 5, 0, 128, ReadOnlyMemory<byte>.Empty), ct);
        await writer.CompleteAsync(ct);

        await using IQuicStream inbound = await pair.Peer.AcceptStreamAsync(ct);
        byte[] wire = await DrainAsync(inbound, ct);

        wire.Should().Equal(
        [
            0x05, 0x04,                          // FETCH_HEADER: stream type 0x5, Request ID 4
            0x1C, 0x07, 0x03, 0x80, 0x02, 0x68, 0x69, // flags 0x1C (group+object+priority), g7 o3 p128 len2 "hi"
            0x00, 0x02, 0x79, 0x6F,              // flags 0x00 (inherit everything), len 2, "yo"
            0x00, 0x00,                          // flags 0x00, len 0 — and nothing after it
        ]);
    }

    [Fact]
    public async Task FetchStream_ZeroLengthObject_CarriesNoStatus()
    {
        // Pinned on its own because this is where we knowingly diverge from the reference relay,
        // which shares one writer between subgroup and fetch streams and so emits a status varint
        // after a zero length in both. The spec (§11.2.1.1) says a FETCH has no Object Status
        // field at all, and the independent C implementation agrees; a status here would desync
        // the reader by one varint on the very next object.
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        CancellationToken ct = cts.Token;
        await using Pair pair = await ConnectAsync(ct);

        await using IQuicStream outbound = await pair.Client.OpenStreamAsync(QuicStreamDirection.Unidirectional, ct);
        var writer = new FetchStreamWriter(outbound, new FetchHeader(0));
        await writer.WriteObjectAsync(MoqObject.Normal(0, 0, 0, 1, ReadOnlyMemory<byte>.Empty), ct);
        await writer.CompleteAsync(ct);

        await using IQuicStream inbound = await pair.Peer.AcceptStreamAsync(ct);
        byte[] wire = await DrainAsync(inbound, ct);

        wire.Should().Equal([0x05, 0x00, 0x1C, 0x00, 0x00, 0x01, 0x00],
            "the object ends at its zero payload length");
    }

    [Fact]
    public async Task FetchStream_RejectsAnObjectStatus()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        CancellationToken ct = cts.Token;
        await using Pair pair = await ConnectAsync(ct);

        await using IQuicStream outbound = await pair.Client.OpenStreamAsync(QuicStreamDirection.Unidirectional, ct);
        var writer = new FetchStreamWriter(outbound, new FetchHeader(1));

        // There is no field to put it in, so failing loudly beats silently dropping it.
        Func<Task> act = async () => await writer.WriteObjectAsync(
            new MoqObject(1, 0, 0, 1, MoqObjectStatus.EndOfGroup, ReadOnlyMemory<byte>.Empty), ct);
        await act.Should().ThrowAsync<ArgumentException>().WithMessage("*Object Status*");
    }

    [Fact]
    public async Task FetchStream_RoundTripsAcrossGroupsSubgroupsAndPriorities()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        CancellationToken ct = cts.Token;
        await using Pair pair = await ConnectAsync(ct);

        await using IQuicStream outbound = await pair.Client.OpenStreamAsync(QuicStreamDirection.Unidirectional, ct);
        var writer = new FetchStreamWriter(outbound, new FetchHeader(11));

        MoqKeyValuePair[] extensions = [MoqKeyValuePair.Varint(0x0A, 1), MoqKeyValuePair.FromBytes(0x0D, [0xAB])];

        await writer.WriteObjectAsync(MoqObject.Normal(2, 0, 5, 10, Encoding.UTF8.GetBytes("a")), ct);
        await writer.WriteObjectAsync(MoqObject.Normal(2, 1, 5, 10, Encoding.UTF8.GetBytes("b")), ct);   // all inherited
        await writer.WriteObjectAsync(MoqObject.Normal(2, 9, 6, 10, Encoding.UTF8.GetBytes("c")), ct);   // id gap, subgroup+1
        await writer.WriteObjectAsync(MoqObject.Normal(2, 10, 0, 99, Encoding.UTF8.GetBytes("d")), ct);  // subgroup 0, new priority
        await writer.WriteObjectAsync(
            MoqObject.Normal(5, 0, 5, 99, Encoding.UTF8.GetBytes("e"), extensions), ct);                 // group jump + properties
        await writer.CompleteAsync(ct);

        await using IQuicStream inbound = await pair.Peer.AcceptStreamAsync(ct);
        var reader = await FetchStreamReader.OpenAsync(inbound, cancellationToken: ct);
        reader.Header.RequestId.Should().Be(11UL);

        var objects = new List<MoqObject>();
        while (await reader.ReadEntryAsync(ct) is { } entry)
        {
            objects.Add(entry.Should().BeOfType<MoqFetchedObject>().Subject.Object);
        }

        objects.Select(o => o.GroupId).Should().Equal([2UL, 2UL, 2UL, 2UL, 5UL], "the group jump resolves via its delta");
        objects.Select(o => o.ObjectId).Should().Equal([0UL, 1UL, 9UL, 10UL, 0UL], "ids restart with a new group");
        objects.Select(o => o.SubgroupId).Should().Equal([5UL, 5UL, 6UL, 0UL, 5UL]);
        objects.Select(o => o.PublisherPriority).Should().Equal([(byte)10, (byte)10, (byte)10, (byte)99, (byte)99]);
        objects.Select(o => Encoding.UTF8.GetString(o.Payload.Span)).Should().Equal(["a", "b", "c", "d", "e"]);

        objects[0].Extensions.Should().BeEmpty();
        objects[4].Extensions.Should().HaveCount(2);
        objects[4].Extensions[0].VarintValue.Should().Be(1UL);
        objects[4].Extensions[1].Bytes.ToArray().Should().Equal([0xAB]);
    }

    [Fact]
    public async Task FetchStream_DescendingGroupOrder_ResolvesDeltasDownward()
    {
        // The same Group ID Delta means the opposite thing under a descending fetch, and nothing
        // on the wire says which — the order comes from the request. A reader told the wrong one
        // reconstructs wrong ids without any parse error, so both ends must agree explicitly.
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        CancellationToken ct = cts.Token;
        await using Pair pair = await ConnectAsync(ct);

        await using IQuicStream outbound = await pair.Client.OpenStreamAsync(QuicStreamDirection.Unidirectional, ct);
        var writer = new FetchStreamWriter(outbound, new FetchHeader(2), MoqGroupOrder.Descending);
        await writer.WriteObjectAsync(MoqObject.Normal(9, 0, 0, 1, Encoding.UTF8.GetBytes("x")), ct);
        await writer.WriteObjectAsync(MoqObject.Normal(8, 0, 0, 1, Encoding.UTF8.GetBytes("y")), ct);
        await writer.WriteObjectAsync(MoqObject.Normal(4, 0, 0, 1, Encoding.UTF8.GetBytes("z")), ct);
        await writer.CompleteAsync(ct);

        await using IQuicStream inbound = await pair.Peer.AcceptStreamAsync(ct);
        var reader = await FetchStreamReader.OpenAsync(inbound, MoqGroupOrder.Descending, cancellationToken: ct);

        var groups = new List<ulong>();
        while (await reader.ReadEntryAsync(ct) is MoqFetchedObject fetched)
        {
            groups.Add(fetched.Object.GroupId);
        }

        groups.Should().Equal([9UL, 8UL, 4UL]);
    }

    [Fact]
    public async Task FetchStream_AscendingWriter_RejectsAFallingGroup()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        CancellationToken ct = cts.Token;
        await using Pair pair = await ConnectAsync(ct);

        await using IQuicStream outbound = await pair.Client.OpenStreamAsync(QuicStreamDirection.Unidirectional, ct);
        var writer = new FetchStreamWriter(outbound, new FetchHeader(3));
        await writer.WriteObjectAsync(MoqObject.Normal(5, 0, 0, 1, Encoding.UTF8.GetBytes("x")), ct);

        Func<Task> act = async () =>
            await writer.WriteObjectAsync(MoqObject.Normal(4, 0, 0, 1, Encoding.UTF8.GetBytes("y")), ct);
        await act.Should().ThrowAsync<ArgumentException>().WithMessage("*ascending*");
    }

    [Fact]
    public async Task FetchStream_EndOfRangeMarkers_RoundTripAndAnchorTheObjectsAfterThem()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        CancellationToken ct = cts.Token;
        await using Pair pair = await ConnectAsync(ct);

        await using IQuicStream outbound = await pair.Client.OpenStreamAsync(QuicStreamDirection.Unidirectional, ct);
        var writer = new FetchStreamWriter(outbound, new FetchHeader(6));
        await writer.WriteObjectAsync(MoqObject.Normal(1, 0, 3, 7, Encoding.UTF8.GetBytes("a")), ct);
        await writer.WriteEndOfRangeAsync(new MoqLocation(1, 4), MoqFetchRangeKind.NonExistent, ct);

        // The marker moved the Location, so this object's id follows the marker's, while its
        // subgroup and priority still come from the object before the marker (§11.4.4.2).
        await writer.WriteObjectAsync(MoqObject.Normal(1, 5, 3, 7, Encoding.UTF8.GetBytes("b")), ct);
        await writer.WriteEndOfRangeAsync(new MoqLocation(2, 0), MoqFetchRangeKind.Unknown, ct);
        await writer.CompleteAsync(ct);

        await using IQuicStream inbound = await pair.Peer.AcceptStreamAsync(ct);
        var reader = await FetchStreamReader.OpenAsync(inbound, cancellationToken: ct);

        var entries = new List<MoqFetchEntry>();
        while (await reader.ReadEntryAsync(ct) is { } entry)
        {
            entries.Add(entry);
        }

        entries.Should().HaveCount(4);

        entries[0].Should().BeOfType<MoqFetchedObject>().Subject.Object.ObjectId.Should().Be(0UL);

        MoqFetchEndOfRange gap = entries[1].Should().BeOfType<MoqFetchEndOfRange>().Subject;
        gap.Kind.Should().Be(MoqFetchRangeKind.NonExistent);
        gap.Location.Should().Be(new MoqLocation(1, 4));

        MoqObject after = entries[2].Should().BeOfType<MoqFetchedObject>().Subject.Object;
        after.ObjectId.Should().Be(5UL, "the object id counts on from the marker's Location");
        after.SubgroupId.Should().Be(3UL, "a marker carries no subgroup, so the prior object's stands");
        after.PublisherPriority.Should().Be((byte)7, "a marker carries no priority either");

        MoqFetchEndOfRange unknown = entries[3].Should().BeOfType<MoqFetchEndOfRange>().Subject;
        unknown.Kind.Should().Be(MoqFetchRangeKind.Unknown);
        unknown.Location.Should().Be(new MoqLocation(2, 0));
    }

    [Fact]
    public async Task FetchStream_EndOfRangeMarker_EncodesTheTable7Values()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        CancellationToken ct = cts.Token;
        await using Pair pair = await ConnectAsync(ct);

        await using IQuicStream outbound = await pair.Client.OpenStreamAsync(QuicStreamDirection.Unidirectional, ct);
        var writer = new FetchStreamWriter(outbound, new FetchHeader(0));
        await writer.WriteEndOfRangeAsync(new MoqLocation(9, 2), MoqFetchRangeKind.NonExistent, ct);
        await writer.WriteEndOfRangeAsync(new MoqLocation(9, 3), MoqFetchRangeKind.Unknown, ct);
        await writer.CompleteAsync(ct);

        await using IQuicStream inbound = await pair.Peer.AcceptStreamAsync(ct);
        byte[] wire = await DrainAsync(inbound, ct);

        // 0x8C and 0x10C both exceed a one-byte var-int, and their Group/Object ids are absolute,
        // not deltas — a marker is three fields and stops, with no payload length to follow.
        wire.Should().Equal(
        [
            0x05, 0x00,             // FETCH_HEADER
            0x80, 0x8C, 0x09, 0x02, // End of Non-Existent Range (0x8C), group 9, object 2
            0x81, 0x0C, 0x09, 0x03, // End of Unknown Range (0x10C), group 9, object 3
        ]);
    }

    [Theory]
    // The first object has no prior to inherit from, so each of these is a protocol violation
    // rather than something to guess at: flags 0x00 drops both id deltas, 0x0C keeps the ids but
    // drops the priority, and 0x1D references a prior subgroup that was never sent.
    [InlineData(new byte[] { 0x05, 0x00, 0x00, 0x00 }, "Group ID Delta")]
    [InlineData(new byte[] { 0x05, 0x00, 0x0C, 0x01, 0x01, 0x00 }, "Priority")]
    [InlineData(new byte[] { 0x05, 0x00, 0x1D, 0x01, 0x01, 0x02, 0x00 }, "Subgroup")]
    public async Task FetchStream_FirstObjectReferencingAPriorObject_IsRejected(byte[] wire, string expected)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        CancellationToken ct = cts.Token;
        await using Pair pair = await ConnectAsync(ct);

        await using IQuicStream outbound = await pair.Client.OpenStreamAsync(QuicStreamDirection.Unidirectional, ct);
        await outbound.WriteAsync(wire, completeWrites: true, ct);

        await using IQuicStream inbound = await pair.Peer.AcceptStreamAsync(ct);
        var reader = await FetchStreamReader.OpenAsync(inbound, cancellationToken: ct);

        Func<Task> act = async () => await reader.ReadEntryAsync(ct);
        await act.Should().ThrowAsync<MoqProtocolException>().WithMessage($"*{expected}*");
    }

    [Fact]
    public async Task FetchStream_ReservedSerializationFlags_AreRejected()
    {
        // 0x80 is reserved, and no value at or above it other than the two Table 7 markers is
        // defined. Skipping one would mean parsing the rest of the stream as garbage.
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        CancellationToken ct = cts.Token;
        await using Pair pair = await ConnectAsync(ct);

        await using IQuicStream outbound = await pair.Client.OpenStreamAsync(QuicStreamDirection.Unidirectional, ct);
        // 0x40 0x80 is the two-byte var-int for 0x80.
        await outbound.WriteAsync(new byte[] { 0x05, 0x00, 0x80, 0x80 }, completeWrites: true, ct);

        await using IQuicStream inbound = await pair.Peer.AcceptStreamAsync(ct);
        var reader = await FetchStreamReader.OpenAsync(inbound, cancellationToken: ct);

        Func<Task> act = async () => await reader.ReadEntryAsync(ct);
        await act.Should().ThrowAsync<MoqProtocolException>().WithMessage("*Serialization Flags*");
    }

    [Fact]
    public async Task FetchStream_DatagramObject_HasNoSubgroup()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        CancellationToken ct = cts.Token;
        await using Pair pair = await ConnectAsync(ct);

        await using IQuicStream outbound = await pair.Client.OpenStreamAsync(QuicStreamDirection.Unidirectional, ct);
        var writer = new FetchStreamWriter(outbound, new FetchHeader(0));
        await writer.WriteObjectAsync(new MoqObject(3, 0, 0, 1, MoqObjectStatus.Normal,
            Encoding.UTF8.GetBytes("d"), null, MoqForwardingPreference.Datagram), ct);
        await writer.CompleteAsync(ct);

        await using IQuicStream inbound = await pair.Peer.AcceptStreamAsync(ct);
        var reader = await FetchStreamReader.OpenAsync(inbound, cancellationToken: ct);

        MoqObject fetched = (await reader.ReadEntryAsync(ct)).Should().BeOfType<MoqFetchedObject>().Subject.Object;
        fetched.Forwarding.Should().Be(MoqForwardingPreference.Datagram);
        fetched.GroupId.Should().Be(3UL);
    }

    [Fact]
    public async Task StreamRouter_ClassifiesAFetchStream()
    {
        // Both data streams lead with a var-int type, and only that tells them apart: a fetch
        // stream's 0x05 has bit 4 clear, so it can never be mistaken for a SUBGROUP_HEADER.
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        CancellationToken ct = cts.Token;
        await using Pair pair = await ConnectAsync(ct);

        await using IQuicStream outbound = await pair.Client.OpenStreamAsync(QuicStreamDirection.Unidirectional, ct);
        var writer = new FetchStreamWriter(outbound, new FetchHeader(42));
        await writer.WriteObjectAsync(MoqObject.Normal(1, 0, 0, 1, Encoding.UTF8.GetBytes("p")), ct);
        await writer.CompleteAsync(ct);

        MoqIncomingStream incoming = await MoqStreamRouter.AcceptAsync(pair.Peer, cancellationToken: ct);

        MoqFetchStream fetchStream = incoming.Should().BeOfType<MoqFetchStream>().Subject;
        fetchStream.Header.RequestId.Should().Be(42UL);

        FetchStreamReader reader = fetchStream.OpenReader();
        MoqObject fetched = (await reader.ReadEntryAsync(ct)).Should().BeOfType<MoqFetchedObject>().Subject.Object;
        Encoding.UTF8.GetString(fetched.Payload.Span).Should().Be("p");

        fetchStream.Invoking(f => f.OpenReader()).Should().Throw<InvalidOperationException>();
    }
}
