using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Threading.Channels;
using Spangle.Net.Moqt.Messages;
using Spangle.Net.Moqt.Wire;
using Spangle.Net.Transport.Quic;

namespace Spangle.Net.Moqt;

/// <summary>
/// The subscriber side of a MOQT session, built on an established <see cref="MoqSession"/>: send
/// SUBSCRIBE for a track and receive its objects as they arrive on subgroup streams. This is the
/// native (non-browser, raw-QUIC) counterpart of a browser player — the verifier the Spangle
/// egress is driven against before an external relay is involved, and the entry point for the
/// MoQ ingest path.
/// <para>
/// The session's demux loop (<see cref="MoqSession.RunAsync"/>) must be running: it is what
/// accepts the subgroup streams and routes them to each subscription by Track Alias. Any number
/// of concurrent subscriptions can share the session; each reads only its own streams.
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
    /// <see cref="MoqSubscription.ReadObjectsAsync"/>. The session's demux loop must be running.
    /// </summary>
    [SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope",
        Justification = "The request stream is owned by the returned MoqSubscription and disposed there.")]
    public async Task<MoqSubscription> SubscribeAsync(FullTrackName track,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(track);

        IQuicStream request = await _session.Connection
            .OpenStreamAsync(QuicStreamDirection.Bidirectional, cancellationToken).ConfigureAwait(false);

        // Subscribes may come from any thread — concurrent subscriptions are the point of the
        // session demux — so the id step is atomic.
        ulong requestId = Interlocked.Add(ref _nextRequestId, 2) - 2;

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
            await request.DisposeAsync().ConfigureAwait(false);
            throw new MoqProtocolException(
                $"Expected SUBSCRIBE_OK (0x{MoqControlMessageType.SubscribeOk:X}) after SUBSCRIBE, got 0x{type:X}.");
        }

        SubscribeOkMessage ok = SubscribeOkMessage.DecodePayload(okPayload);
        return new MoqSubscription(_session, request, ok.TrackAlias);
    }
}

/// <summary>
/// An accepted subscription: its Track Alias plus the object stream. Objects are delivered on
/// unidirectional subgroup streams the publisher opens; the session's demux loop routes the ones
/// carrying this subscription's alias here, and <see cref="ReadObjectsAsync"/> yields each object
/// in arrival order.
/// </summary>
public sealed class MoqSubscription : IAsyncDisposable
{
    private readonly MoqSession _session;
    private readonly IQuicStream _request;
    private readonly ChannelReader<MoqSubgroupStream> _subgroups;

    internal MoqSubscription(MoqSession session, IQuicStream request, ulong trackAlias)
    {
        _session = session;
        _request = request;
        TrackAlias = trackAlias;
        _subgroups = session.ClaimSubgroups(trackAlias);
    }

    /// <summary>The Track Alias the publisher assigned; subgroup streams carrying it belong here.</summary>
    public ulong TrackAlias { get; }

    /// <summary>
    /// Yields the track's objects as they arrive, group after group. The enumeration ends when
    /// the session's demux loop ends (the session died or was cancelled) — a live track has no
    /// end of its own — or when the caller breaks out or cancels the token.
    /// </summary>
    public async IAsyncEnumerable<MoqObject> ReadObjectsAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (MoqSubgroupStream subgroup in _subgroups.ReadAllAsync(cancellationToken)
                           .ConfigureAwait(false))
        {
            try
            {
                while (await subgroup.Reader.ReadObjectAsync(cancellationToken).ConfigureAwait(false) is
                       { } moqObject)
                {
                    yield return moqObject;
                }
            }
            finally
            {
                await subgroup.Stream.DisposeAsync().ConfigureAwait(false);
            }
        }
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        await _session.ReleaseSubgroupsAsync(TrackAlias).ConfigureAwait(false);
        await _request.DisposeAsync().ConfigureAwait(false);
    }
}
