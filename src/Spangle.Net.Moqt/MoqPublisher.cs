using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using System.Threading.Channels;
using Spangle.Net.Moqt.Data;
using Spangle.Net.Moqt.Messages;
using Spangle.Net.Moqt.Wire;
using Spangle.Net.Transport.Quic;

namespace Spangle.Net.Moqt;

/// <summary>
/// The publisher side of a MOQT session, built on an established <see cref="MoqSession"/>:
/// declare the tracks it offers, run the request-stream demux loop (answering SUBSCRIBE with
/// SUBSCRIBE_OK and a freshly assigned Track Alias), and stream each track's objects on
/// unidirectional subgroup streams. This is the "the bridge just calls it" surface the Spangle
/// media bridge drives for egress — it never touches a varint.
/// <para>
/// Scope: SUBSCRIBE-driven pull, fanned out to any number of concurrent subscribers per track —
/// each SUBSCRIBE gets its own Track Alias and its own subgroup streams, and every group a track
/// publishes reaches all of them. No PUBLISH push (§10.10), and no SUBSCRIBE_ERROR beyond the
/// "does not exist" reply. The session owns the connection lifetime; this facade does not dispose
/// it.
/// </para>
/// </summary>
public sealed class MoqPublisher
{
    private readonly MoqSession _session;
    private readonly Dictionary<string, MoqPublishedTrack> _tracks = new(StringComparer.Ordinal);
    private readonly List<IQuicStream> _announcements = [];
    private ulong _nextAlias = 1;
    private ulong _nextRequestId; // client-initiated request IDs (draft-18 §10.1)
    private Exception? _stopped;

    private MoqPublisher(MoqSession session) => _session = session;

    /// <summary>Wraps an established session as a publisher.</summary>
    public static MoqPublisher Create(MoqSession session)
    {
        ArgumentNullException.ThrowIfNull(session);
        return new MoqPublisher(session);
    }

    /// <summary>
    /// Announces a Track Namespace to an upstream peer (a relay) with PUBLISH_NAMESPACE and awaits
    /// its REQUEST_OK. In draft-18 each request runs on its own bidirectional stream, so this opens
    /// one, sends PUBLISH_NAMESPACE, and reads the reply on the same stream. The stream is kept
    /// open for the life of the announcement. After this returns the relay may SUBSCRIBE to tracks
    /// in the namespace; declare those with <see cref="PublishTrack"/> and answer subscriptions
    /// with <see cref="RunAsync"/>.
    /// </summary>
    [SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope",
        Justification = "The request stream stays open while the namespace is announced; it is held in _announcements.")]
    public async Task AnnounceNamespaceAsync(TrackNamespace @namespace, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(@namespace);
        // Announces may come from any thread, unlike subscriptions (which the session's
        // dispatch loop serializes), so the id step is atomic.
        ulong requestId = Interlocked.Add(ref _nextRequestId, 2) - 2;

        IQuicStream request = await _session.Connection
            .OpenStreamAsync(QuicStreamDirection.Bidirectional, cancellationToken).ConfigureAwait(false);
        _announcements.Add(request);

        var payload = new ArrayBufferWriter<byte>();
        new PublishNamespaceMessage(requestId, @namespace).EncodePayload(new MoqWriter(payload));
        var frame = new ArrayBufferWriter<byte>();
        ControlMessage.Write(frame, MoqControlMessageType.PublishNamespace, payload.WrittenSpan);
        await request.WriteAsync(frame.WrittenMemory, completeWrites: false, cancellationToken).ConfigureAwait(false);

        (ulong type, byte[] okPayload) =
            await ControlMessage.ReadAsync(request, cancellationToken).ConfigureAwait(false);
        if (type != MoqControlMessageType.RequestOk)
        {
            throw new MoqProtocolException(
                $"Expected REQUEST_OK (0x{MoqControlMessageType.RequestOk:X}) after PUBLISH_NAMESPACE, got 0x{type:X}.");
        }

        RequestOkMessage ok = RequestOkMessage.DecodePayload(okPayload);
        if (ok.TrackProperties.Count > 0)
        {
            // §10.5: Track Properties belong to TRACK_STATUS_OK alone; in any other REQUEST_OK
            // flavour their presence is a protocol violation.
            throw new MoqProtocolException(
                "The REQUEST_OK answering PUBLISH_NAMESPACE must not carry Track Properties (§10.5).");
        }
    }

    /// <summary>
    /// Declares a track this publisher offers. Objects written to the returned handle are
    /// delivered once a subscriber SUBSCRIBEs to this exact full name. Declare tracks before
    /// (or concurrently with) <see cref="RunAsync"/>.
    /// </summary>
    public MoqPublishedTrack PublishTrack(FullTrackName name)
    {
        ArgumentNullException.ThrowIfNull(name);
        var track = new MoqPublishedTrack(_session.Connection, name);
        if (_stopped is { } stopped)
        {
            track.FailSubscribers(stopped);
        }

        _tracks[Key(name)] = track;
        return track;
    }

    // What moxygen answers a SUBSCRIBE for a track it does not know: REQUEST_ERROR code 0x10,
    // "no such namespace or track".
    private const ulong DoesNotExistErrorCode = 0x10;

    /// <summary>
    /// Registers this publisher as the session's request handler and runs the session's demux
    /// loop until cancellation: every SUBSCRIBE that matches a declared track is answered with
    /// SUBSCRIBE_OK carrying a newly assigned Track Alias, after which the track streams its
    /// objects to that subscription alongside any others already attached. A SUBSCRIBE for an
    /// unknown track is answered with
    /// REQUEST_ERROR ("does not exist") — a reset alone reads as a transient failure and
    /// invites the peer to retry forever.
    /// <para>
    /// When this loop stops — cancelled, or because the session died under it — every track's wait
    /// for a subscriber stops with it. Nothing else would notice: a publisher whose connection has
    /// gone silently sits in <see cref="MoqPublishedTrack.BeginGroupAsync"/> forever, waiting for a
    /// subscriber that no longer has any way to arrive, and this task's exception is only seen by a
    /// caller who awaits it.
    /// </para>
    /// </summary>
    public async Task RunAsync(CancellationToken cancellationToken = default)
    {
        _session.OnRequest(HandleRequestAsync);
        try
        {
            await _session.RunAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            // Whatever ended the loop — a dead connection, a malformed request — is what every
            // waiting track should be told, rather than being left to wait on it.
            Stop(ex);
            throw;
        }

        Stop(new MoqProtocolException("The publisher stopped serving subscriptions."));
    }

    // Runs on the session's dispatch loop, one request at a time — the track table and the
    // alias counter need no locking because of that.
    private async Task HandleRequestAsync(MoqRequestStream request, CancellationToken cancellationToken)
    {
        if (request.MessageType != MoqControlMessageType.Subscribe)
        {
            // This cut only handles SUBSCRIBE request streams — but the stream was accepted,
            // and an accepted stream holds inbound-stream credit until it is closed.
            await request.Stream.DisposeAsync().ConfigureAwait(false);
            return;
        }

        SubscribeMessage subscribe = SubscribeMessage.DecodePayload(request.Payload.Span);
        if (!_tracks.TryGetValue(Key(subscribe.Track), out MoqPublishedTrack? track))
        {
            await WriteRequestErrorAsync(request.Stream, cancellationToken).ConfigureAwait(false);
            await request.Stream.DisposeAsync().ConfigureAwait(false);
            return;
        }

        ulong alias = _nextAlias++;
        await WriteSubscribeOkAsync(request.Stream, alias, cancellationToken).ConfigureAwait(false);
        // The demux token bounds the subscriber's fan-out pump: when the session ends, the pump
        // stops with it.
        track.AttachSubscriber(alias, cancellationToken);
    }

    private static async Task WriteRequestErrorAsync(IQuicStream requestStream, CancellationToken cancellationToken)
    {
        var payload = new ArrayBufferWriter<byte>();
        new RequestErrorMessage(DoesNotExistErrorCode, retryInterval: 0, "no such namespace or track")
            .EncodePayload(new MoqWriter(payload));
        var frame = new ArrayBufferWriter<byte>();
        ControlMessage.Write(frame, MoqControlMessageType.RequestError, payload.WrittenSpan);
        await requestStream.WriteAsync(frame.WrittenMemory, completeWrites: true, cancellationToken)
            .ConfigureAwait(false);
    }

    // The publisher answers no more subscriptions, so nothing is coming for the tracks that are
    // waiting on one, and nothing is coming for tracks declared after this either.
    private void Stop(Exception reason)
    {
        _stopped ??= reason;
        foreach (MoqPublishedTrack track in _tracks.Values)
        {
            track.FailSubscribers(reason);
        }
    }

    private static async Task WriteSubscribeOkAsync(IQuicStream requestStream, ulong alias,
        CancellationToken cancellationToken)
    {
        var payload = new ArrayBufferWriter<byte>();
        new SubscribeOkMessage(alias).EncodePayload(new MoqWriter(payload));
        var frame = new ArrayBufferWriter<byte>();
        ControlMessage.Write(frame, MoqControlMessageType.SubscribeOk, payload.WrittenSpan);
        await requestStream.WriteAsync(frame.WrittenMemory, completeWrites: false, cancellationToken)
            .ConfigureAwait(false);
    }

    // A dictionary key unique per full track name. The namespace fields and the name are joined
    // with a NUL separator, which the UTF-8 field bytes never contain, so distinct names never
    // collide onto the same key.
    private static string Key(FullTrackName name) =>
        string.Join('\0', name.Namespace.ToStrings()) + "\0" + name.NameAsString;
}

/// <summary>
/// A track a <see cref="MoqPublisher"/> offers, fanned out to every subscriber that asked for it.
/// Media is written group by group (<see cref="BeginGroupAsync"/>); each group is delivered to
/// every active subscription on its own subgroup stream, tagged with that subscription's Track
/// Alias. Each subscription is served by its own pump, so the subscribers do not pace one another:
/// a slow or stalled one falls behind on its own streams and, past a bounded backlog, is dropped —
/// it never blocks the track or another subscriber (see <see cref="MoqGroupWriter"/>).
/// </summary>
public sealed class MoqPublishedTrack
{
    // How far one subscriber may fall behind — this many undelivered group boundaries and objects
    // buffered for it — before it is dropped rather than allowed to stall the track. The buffer is
    // per subscriber, so one slow consumer costs only its own backlog; the objects are already
    // copied out of the caller's buffers, so what is held is bounded by this count, not by the
    // producer's willingness to wait.
    private const int MaxBufferedCommandsPerSubscriber = 1024;

    private readonly IQuicConnection _connection;
    private readonly Lock _lock = new();
    private readonly List<PublishedSubscription> _subscribers = [];
    private readonly TaskCompletionSource<ulong> _firstSubscriber =
        new(TaskCreationOptions.RunContinuationsAsynchronously);
    private Exception? _failed;

    internal MoqPublishedTrack(IQuicConnection connection, FullTrackName name)
    {
        _connection = connection;
        Name = name;
    }

    /// <summary>The full name subscribers ask for.</summary>
    public FullTrackName Name { get; }

    /// <summary>
    /// Whether at least one subscriber is currently attached, without waiting for one. A live
    /// publisher asks this: with nobody subscribed there is nothing to do with a frame but drop it,
    /// and blocking on <see cref="BeginGroupAsync"/> for the first subscriber would stall the
    /// pipeline feeding it. It falls back to false once the last subscriber goes away.
    /// </summary>
    public bool HasSubscriber
    {
        get
        {
            lock (_lock)
            {
                return _subscribers.Count > 0;
            }
        }
    }

    /// <summary>How many subscribers are attached right now.</summary>
    public int SubscriberCount
    {
        get
        {
            lock (_lock)
            {
                return _subscribers.Count;
            }
        }
    }

    /// <summary>
    /// Completes with the Track Alias of the <em>first</em> subscriber once one has subscribed to
    /// this track. The bridge can await this to avoid producing groups no one will receive; later
    /// subscribers are picked up by the next <see cref="BeginGroupAsync"/> without re-arming it.
    /// </summary>
    // The TCS is completed by AttachSubscriber on this same object; handing its Task out is not
    // the foreign-task hazard VSTHRD003 warns about.
#pragma warning disable VSTHRD003
    public Task<ulong> WaitForSubscriberAsync() => _firstSubscriber.Task;
#pragma warning restore VSTHRD003

    /// <summary>
    /// Attaches the subscription that just arrived and starts its fan-out pump. Every subscriber is
    /// served independently: the pump opens a subgroup stream per group and writes that group's
    /// objects to it at the subscriber's own pace.
    /// </summary>
    internal void AttachSubscriber(ulong alias, CancellationToken sessionToken)
    {
        var subscription = new PublishedSubscription(alias, MaxBufferedCommandsPerSubscriber);
        lock (_lock)
        {
            if (_failed is not null)
            {
                // The publisher already stopped; this subscriber has nothing coming. (A SUBSCRIBE
                // cannot normally land after the demux ends, so this is the belt-and-suspenders
                // path.)
                subscription.Commands.Writer.TryComplete();
                return;
            }

            _subscribers.Add(subscription);
        }

        _firstSubscriber.TrySetResult(alias);
        _ = PumpSubscriberAsync(subscription, sessionToken);
    }

    /// <summary>Ends the wait for a subscriber and stops every pump: none can arrive or continue now.</summary>
    internal void FailSubscribers(Exception reason)
    {
        List<PublishedSubscription> stopping;
        lock (_lock)
        {
            _failed ??= reason;
            stopping = [.. _subscribers];
            _subscribers.Clear();
        }

        _firstSubscriber.TrySetException(reason);
        foreach (PublishedSubscription subscription in stopping)
        {
            subscription.Drop();
        }
    }

    /// <summary>
    /// Begins a group and returns a writer that fans its objects out to every subscriber attached
    /// right now — each on its own subgroup stream, tagged with its own Track Alias. A subscriber
    /// that joins after this call is not on this group but is picked up by the next one. Awaits the
    /// first subscriber if none has ever arrived; once one has, this no longer blocks (a group with
    /// no current subscribers is written to nobody). Dispose or complete the returned writer when
    /// the subgroup is done.
    /// <para>
    /// Set <paramref name="hasProperties"/> when the objects carry Extension Headers — it selects
    /// the header's Properties bit, which every object on the stream must then honour.
    /// </para>
    /// <para>
    /// <paramref name="endOfGroup"/> sets the header's END_OF_GROUP bit, asserting that this
    /// subgroup holds the group's largest Object — so on FIN the group is complete. It is worth
    /// setting: it is the only thing that tells a subscriber a group ended on purpose. Without it a
    /// receiver cannot tell a finished group from one whose rest is still in flight, and waits out
    /// a timeout before moving on. A publisher that puts a whole group on one subgroup stream knows
    /// this at the point it opens the stream and should always set it; one that spreads a group
    /// across subgroups only knows it for the subgroup it finishes last.
    /// </para>
    /// <para>
    /// <paramref name="subgroupId"/> distinguishes concurrent subgroups within one group. Leave it
    /// at 0 for one-subgroup-per-group; a publisher that opens several must give each its own. A
    /// track writes one group at a time: begin it, write its objects, complete it, then begin the
    /// next.
    /// </para>
    /// </summary>
    public async ValueTask<MoqGroupWriter> BeginGroupAsync(ulong groupId, byte publisherPriority,
        bool hasProperties = false, bool endOfGroup = false, ulong subgroupId = 0,
        CancellationToken cancellationToken = default)
    {
        // _firstSubscriber is this track's own TCS, completed by AttachSubscriber on the demux
        // loop; awaiting it is not the foreign-task deadlock hazard VSTHRD003 guards against.
#pragma warning disable VSTHRD003
        await _firstSubscriber.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
#pragma warning restore VSTHRD003

        PublishedSubscription[] snapshot;
        lock (_lock)
        {
            snapshot = [.. _subscribers];
        }

        var lanes = new List<PublishedSubscription>(snapshot.Length);
        foreach (PublishedSubscription subscription in snapshot)
        {
            var header = new SubgroupHeader
            {
                TrackAlias = subscription.Alias,
                GroupId = groupId,
                SubgroupIdMode = SubgroupIdMode.Explicit,
                SubgroupId = subgroupId,
                HasProperties = hasProperties,
                EndOfGroup = endOfGroup,
                PublisherPriority = publisherPriority,
            };
            if (subscription.Commands.Writer.TryWrite(GroupCommand.Begin(header)))
            {
                lanes.Add(subscription);
            }
            else
            {
                // Already this far behind at a group boundary: drop it rather than let it stall.
                DropSubscriber(subscription);
            }
        }

        return new MoqGroupWriter(this, lanes, groupId, subgroupId, publisherPriority);
    }

    // Hands one group command to every lane that is still keeping up. A lane whose buffer is full
    // has fallen too far behind and is dropped; the others are untouched, so no subscriber's
    // backlog is any other subscriber's problem.
    internal void Deliver(IReadOnlyList<PublishedSubscription> lanes, GroupCommand command)
    {
        foreach (PublishedSubscription lane in lanes)
        {
            if (lane.IsDropped)
            {
                continue;
            }

            if (!lane.Commands.Writer.TryWrite(command))
            {
                DropSubscriber(lane);
            }
        }
    }

    private void DropSubscriber(PublishedSubscription subscription)
    {
        lock (_lock)
        {
            _subscribers.Remove(subscription);
        }

        subscription.Drop();
    }

    // One subscriber's fan-out pump, for the life of the subscription: it turns the group commands
    // the track hands it into subgroup streams, one group at a time, at this subscriber's own pace.
    // A reset, an abort, or falling too far behind ends only this pump — the subscriber loses its
    // place on the track and at most the group in flight, never the track or another subscriber.
    private async Task PumpSubscriberAsync(PublishedSubscription subscription, CancellationToken sessionToken)
    {
        IQuicStream? stream = null;
        SubgroupStreamWriter? writer = null;
        try
        {
            await foreach (GroupCommand command in subscription.Commands.Reader
                               .ReadAllAsync(sessionToken).ConfigureAwait(false))
            {
                switch (command.Kind)
                {
                    case GroupCommandKind.Begin:
                        if (stream is not null)
                        {
                            await DisposeQuietlyAsync(stream).ConfigureAwait(false);
                        }

                        stream = await _connection
                            .OpenStreamAsync(QuicStreamDirection.Unidirectional, sessionToken).ConfigureAwait(false);
                        subscription.SetCurrentStream(stream);
                        writer = new SubgroupStreamWriter(stream, command.Header);
                        break;
                    case GroupCommandKind.Object:
                        await writer!.WriteObjectAsync(command.Object!, sessionToken).ConfigureAwait(false);
                        break;
                    case GroupCommandKind.End:
                        if (writer is not null)
                        {
                            await writer.CompleteAsync(sessionToken).ConfigureAwait(false);
                        }

                        if (stream is not null)
                        {
                            await stream.DisposeAsync().ConfigureAwait(false);
                        }

                        subscription.SetCurrentStream(null);
                        stream = null;
                        writer = null;
                        break;
                }
            }
        }
#pragma warning disable CA1031 // any failure on this subscriber's streams stays this subscriber's
        catch (Exception)
        {
            // The peer reset or aborted this subscription's stream, the connection went, or the
            // subscriber fell far enough behind that DropSubscriber aborted its stream — and
            // cancellation (the session ending) lands here too. In every case only this pump ends.
        }
#pragma warning restore CA1031
        finally
        {
            subscription.SetCurrentStream(null);
            if (stream is not null)
            {
                await DisposeQuietlyAsync(stream).ConfigureAwait(false);
            }

            DropSubscriber(subscription);
        }
    }

    private static async ValueTask DisposeQuietlyAsync(IQuicStream stream)
    {
#pragma warning disable CA1031 // closing a stream the peer already tore down is best-effort
        try
        {
            await stream.DisposeAsync().ConfigureAwait(false);
        }
        catch (Exception)
        {
            // best-effort by definition
        }
#pragma warning restore CA1031
    }
}

// One subscriber's slot on a track: its Track Alias and the bounded command queue its pump drains.
// The current stream is tracked so a drop can abort it — unblocking a pump stalled on a write to a
// slow subscriber, so the drop takes effect at once instead of waiting the stall out.
internal sealed class PublishedSubscription
{
    private readonly Lock _streamLock = new();
    private IQuicStream? _currentStream;
    private int _dropped;

    public PublishedSubscription(ulong alias, int bufferCapacity)
    {
        Alias = alias;
        Commands = Channel.CreateBounded<GroupCommand>(new BoundedChannelOptions(bufferCapacity)
        {
            SingleReader = true,
            SingleWriter = false,
            FullMode = BoundedChannelFullMode.Wait,
        });
    }

    public ulong Alias { get; }

    public Channel<GroupCommand> Commands { get; }

    public bool IsDropped => Volatile.Read(ref _dropped) != 0;

    public void SetCurrentStream(IQuicStream? stream)
    {
        lock (_streamLock)
        {
            _currentStream = stream;
        }
    }

    // Stops taking commands and aborts the stream in flight, if any. Idempotent: the producer's
    // drop path and the pump's own teardown may both call it.
    public void Drop()
    {
        if (Interlocked.Exchange(ref _dropped, 1) != 0)
        {
            return;
        }

        Commands.Writer.TryComplete();
        lock (_streamLock)
        {
            _currentStream?.Abort(0);
        }
    }
}

// What the track hands a subscriber's pump: begin a group's stream, write an object, or end the
// group (FIN the stream). A per-object copy of the payload rides in the object, so the producer's
// buffers are free the instant it enqueues.
internal enum GroupCommandKind
{
    Begin,
    Object,
    End,
}

internal readonly struct GroupCommand
{
    private GroupCommand(GroupCommandKind kind, SubgroupHeader header, MoqObject? moqObject)
    {
        Kind = kind;
        Header = header;
        Object = moqObject;
    }

    public GroupCommandKind Kind { get; }

    public SubgroupHeader Header { get; }

    public MoqObject? Object { get; }

    public static GroupCommand Begin(SubgroupHeader header) => new(GroupCommandKind.Begin, header, moqObject: null);

    public static GroupCommand Write(MoqObject moqObject) =>
        new(GroupCommandKind.Object, header: default, moqObject);

    public static GroupCommand End() => new(GroupCommandKind.End, header: default, moqObject: null);
}

/// <summary>
/// Writes the objects of one group, fanning them out to every subscriber the group was begun for —
/// each on its own subgroup stream, which is FINed when the group completes. Object IDs must
/// strictly increase (the wire uses delta encoding). Obtain one from
/// <see cref="MoqPublishedTrack.BeginGroupAsync"/>.
/// <para>
/// Delivery is handed to each subscriber's pump, so a call returns as soon as the object is queued,
/// not when it reaches the wire; a slow subscriber never delays this writer or the others. Because
/// the write is deferred, each object's payload (and properties) are copied here, so the caller's
/// buffers are free to reuse the instant the call returns — the same guarantee a straight-to-wire
/// write gave.
/// </para>
/// </summary>
public sealed class MoqGroupWriter : IAsyncDisposable
{
    private readonly MoqPublishedTrack _track;
    private readonly IReadOnlyList<PublishedSubscription> _lanes;
    private readonly byte _publisherPriority;
    private ulong _previousObjectId;
    private bool _firstObject = true;
    private bool _completed;

    internal MoqGroupWriter(MoqPublishedTrack track, IReadOnlyList<PublishedSubscription> lanes,
        ulong groupId, ulong subgroupId, byte publisherPriority)
    {
        _track = track;
        _lanes = lanes;
        _publisherPriority = publisherPriority;
        GroupId = groupId;
        SubgroupId = subgroupId;
    }

    /// <summary>The group these objects belong to.</summary>
    public ulong GroupId { get; }

    /// <summary>The subgroup within that group these objects are on.</summary>
    public ulong SubgroupId { get; }

    /// <summary>
    /// Appends one object (with the group's priority and subgroup) to every subscriber's stream,
    /// optionally carrying Extension Headers — which requires the group to have been opened with
    /// <c>hasProperties</c>.
    /// </summary>
    public ValueTask WriteObjectAsync(ulong objectId, ReadOnlyMemory<byte> payload,
        IReadOnlyList<MoqKeyValuePair>? properties = null, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        NextObjectId(objectId);
        IReadOnlyList<MoqKeyValuePair>? copiedProperties = properties is null ? null : [.. properties];
        MoqObject moqObject = MoqObject.Normal(GroupId, objectId, SubgroupId, _publisherPriority,
            payload.ToArray(), copiedProperties);
        _track.Deliver(_lanes, GroupCommand.Write(moqObject));
        return ValueTask.CompletedTask;
    }

    /// <summary>
    /// Appends a zero-length object with the End of Group status (§11.2.1.1) to every subscriber's
    /// stream: no object at or after <paramref name="objectId"/> exists in the group. This says on
    /// the wire what the header's END_OF_GROUP bit says in the header — which is what a publisher
    /// needs when it could not have known the group was ending at the time it opened the stream,
    /// because the bit is written with the header and the news arrives later.
    /// </summary>
    public ValueTask WriteEndOfGroupAsync(ulong objectId, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        NextObjectId(objectId);
        var moqObject = new MoqObject(GroupId, objectId, SubgroupId, _publisherPriority,
            MoqObjectStatus.EndOfGroup, ReadOnlyMemory<byte>.Empty);
        _track.Deliver(_lanes, GroupCommand.Write(moqObject));
        return ValueTask.CompletedTask;
    }

    /// <summary>FINs every subscriber's subgroup stream, signalling the subgroup is complete.</summary>
    public ValueTask CompleteAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        EndGroup();
        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        EndGroup();
        return ValueTask.CompletedTask;
    }

    // Delivers the group's end to every lane, once: each lane's pump FINs its subgroup stream.
    private void EndGroup()
    {
        if (_completed)
        {
            return;
        }

        _completed = true;
        _track.Deliver(_lanes, GroupCommand.End());
    }

    // The strictly-increasing-Object-ID rule the delta encoding needs, checked once for the whole
    // fan-out rather than separately per lane, so a bad ID is the producer's error to see here and
    // not a divergence between subscribers.
    private void NextObjectId(ulong objectId)
    {
        if (!_firstObject && objectId <= _previousObjectId)
        {
            throw new ArgumentException("Object IDs must strictly increase within a subgroup.", nameof(objectId));
        }

        _previousObjectId = objectId;
        _firstObject = false;
    }
}
