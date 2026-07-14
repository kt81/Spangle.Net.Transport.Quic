using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using Spangle.Net.Moqt.Messages;
using Spangle.Net.Moqt.Wire;
using Spangle.Net.Transport.Quic;

namespace Spangle.Net.Moqt;

/// <summary>
/// The subscriber side of a MOQT session, built on an established <see cref="MoqSession"/>: send
/// SUBSCRIBE for a track and receive its objects as they arrive on subgroup streams. This is the
/// native (non-browser, raw-QUIC) counterpart of a browser player — the verifier the Spangle
/// egress is driven against before an external relay is involved, and the entry point for a
/// future MoQ ingest path.
/// <para>
/// Scope (first cut): one SUBSCRIBE at a time, matched to its Track Alias; no SUBSCRIBE_UPDATE,
/// FETCH, or multi-track demux. The session owns the connection lifetime.
/// </para>
/// </summary>
public sealed class MoqSubscriber
{
    // draft-18 §10.7: the SUBSCRIPTION_FILTER Subscribe Parameter (key 0x21, odd → a
    // length-prefixed value) is mandatory; its value is a LocationType varint (2 = Largest
    // Object = "from the latest object", the live-edge filter) plus, for absolute filters,
    // a start location and end group. Live playback needs only the Largest Object form.
    private const ulong SubscriptionFilterKey = 0x21;
    private const ulong LargestObjectFilter = 2;

    private readonly MoqSession _session;
    private ulong _nextRequestId; // client-initiated request IDs are even (draft-18 §10.1)

    private MoqSubscriber(MoqSession session) => _session = session;

    /// <summary>Wraps an established session as a subscriber.</summary>
    public static MoqSubscriber Create(MoqSession session)
    {
        ArgumentNullException.ThrowIfNull(session);
        return new MoqSubscriber(session);
    }

    /// <summary>
    /// Sends SUBSCRIBE for <paramref name="track"/> on a new request stream and awaits SUBSCRIBE_OK,
    /// returning a subscription bound to the assigned Track Alias. Read its objects with
    /// <see cref="MoqSubscription.ReadObjectsAsync"/>.
    /// </summary>
    [SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope",
        Justification = "The request stream is owned by the returned MoqSubscription and disposed there.")]
    public async Task<MoqSubscription> SubscribeAsync(FullTrackName track,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(track);

        IQuicStream request = await _session.Connection
            .OpenStreamAsync(QuicStreamDirection.Bidirectional, cancellationToken).ConfigureAwait(false);

        ulong requestId = _nextRequestId;
        _nextRequestId += 2;

        // Mandatory SUBSCRIPTION_FILTER, set to Largest Object (subscribe from the live edge).
        var filterValue = new ArrayBufferWriter<byte>();
        new MoqWriter(filterValue).WriteVarInt(LargestObjectFilter);
        var filter = MoqKeyValuePair.FromBytes(SubscriptionFilterKey, filterValue.WrittenSpan);

        var payload = new ArrayBufferWriter<byte>();
        new SubscribeMessage(requestId, track, [filter]).EncodePayload(new MoqWriter(payload));
        var frame = new ArrayBufferWriter<byte>();
        ControlMessage.Write(frame, MoqControlMessageType.Subscribe, payload.WrittenSpan);
        await request.WriteAsync(frame.WrittenMemory, completeWrites: false, cancellationToken)
            .ConfigureAwait(false);

        (ulong type, byte[] okPayload) = await ControlMessage.ReadAsync(request, cancellationToken)
            .ConfigureAwait(false);
        if (type != MoqControlMessageType.SubscribeOk)
        {
            request.Abort(0);
            throw new MoqProtocolException(
                $"Expected SUBSCRIBE_OK (0x{MoqControlMessageType.SubscribeOk:X}) after SUBSCRIBE, got 0x{type:X}.");
        }

        SubscribeOkMessage ok = SubscribeOkMessage.DecodePayload(okPayload);
        return new MoqSubscription(_session.Connection, request, ok.TrackAlias);
    }
}

/// <summary>
/// An accepted subscription: its Track Alias plus the object stream. Objects are delivered on
/// unidirectional subgroup streams the publisher opens; this type accepts them, filters by alias,
/// and yields each object in arrival order.
/// </summary>
public sealed class MoqSubscription : IAsyncDisposable
{
    private readonly IQuicConnection _connection;
    private readonly IQuicStream _request;

    internal MoqSubscription(IQuicConnection connection, IQuicStream request, ulong trackAlias)
    {
        _connection = connection;
        _request = request;
        TrackAlias = trackAlias;
    }

    /// <summary>The Track Alias the publisher assigned; subgroup streams carrying it belong here.</summary>
    public ulong TrackAlias { get; }

    /// <summary>
    /// Yields the track's objects as they arrive, group after group, until cancellation. Streams
    /// carrying a different alias are skipped. The enumerator does not end on its own (a live track
    /// has no end); the caller stops by breaking the enumeration or cancelling the token.
    /// </summary>
    public async IAsyncEnumerable<MoqObject> ReadObjectsAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            MoqIncomingStream incoming;
            try
            {
                incoming = await MoqStreamRouter.AcceptAsync(_connection, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                yield break;
            }

            if (incoming is not MoqSubgroupStream subgroup || subgroup.Reader.Header.TrackAlias != TrackAlias)
            {
                continue; // control/request streams and other tracks are not ours
            }

            while (await subgroup.Reader.ReadObjectAsync(cancellationToken).ConfigureAwait(false) is { } moqObject)
            {
                yield return moqObject;
            }

            await subgroup.Stream.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync() => _request.DisposeAsync();
}
