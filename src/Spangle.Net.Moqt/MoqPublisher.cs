using System.Buffers;
using System.Diagnostics.CodeAnalysis;
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
/// Scope (first cut): SUBSCRIBE-driven pull to <b>one</b> subscriber per track. No PUBLISH push
/// (§10.10), no SUBSCRIBE_ERROR, no fan-out to multiple subscribers — enough for a single egress
/// consumer or a relay upstream. The session owns the connection lifetime; this facade does not
/// dispose it.
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

        (ulong type, byte[] _) = await ControlMessage.ReadAsync(request, cancellationToken).ConfigureAwait(false);
        if (type != MoqControlMessageType.RequestOk)
        {
            throw new MoqProtocolException(
                $"Expected REQUEST_OK (0x{MoqControlMessageType.RequestOk:X}) after PUBLISH_NAMESPACE, got 0x{type:X}.");
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
    /// objects to that subscriber. A SUBSCRIBE for an unknown track is answered with
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
        track.AttachSubscriber(alias);
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
/// A track a <see cref="MoqPublisher"/> offers. Media is written group by group: each group opens
/// its own subgroup stream (subgroup 0) tagged with the track's assigned alias. Writes block on
/// the first subscriber, then flow; this cut targets one subscriber.
/// </summary>
public sealed class MoqPublishedTrack
{
    private readonly IQuicConnection _connection;
    private readonly TaskCompletionSource<ulong> _subscribed =
        new(TaskCreationOptions.RunContinuationsAsynchronously);
    private ulong _alias;

    internal MoqPublishedTrack(IQuicConnection connection, FullTrackName name)
    {
        _connection = connection;
        Name = name;
    }

    /// <summary>The full name subscribers ask for.</summary>
    public FullTrackName Name { get; }

    /// <summary>
    /// Whether a subscriber has arrived, without waiting for one. A live publisher asks this: with
    /// nobody subscribed there is nothing to do with a frame but drop it, and blocking on
    /// <see cref="BeginGroupAsync"/> instead would stall the pipeline feeding it.
    /// </summary>
    public bool HasSubscriber => _subscribed.Task.IsCompletedSuccessfully;

    /// <summary>
    /// The Track Alias the objects of the next group will carry — the one the newest subscription
    /// assigned. A relay re-subscribes with a fresh alias every time its own subscriber count goes
    /// from none to one, and objects sent under the previous alias are objects it has nowhere to
    /// put: it answers "unknown track alias" and the viewer sees nothing at all.
    /// </summary>
    public ulong CurrentAlias => Volatile.Read(ref _alias);

    /// <summary>
    /// Completes with the assigned Track Alias once a subscriber has subscribed to this track.
    /// The bridge can await this to avoid producing groups no one will receive.
    /// </summary>
    // The TCS is completed by AttachSubscriber on this same object; handing its Task out is not
    // the foreign-task hazard VSTHRD003 warns about.
#pragma warning disable VSTHRD003
    public Task<ulong> WaitForSubscriberAsync() => _subscribed.Task;
#pragma warning restore VSTHRD003

    /// <summary>
    /// Binds the track to the subscription that just arrived. Later subscriptions replace the alias
    /// rather than being ignored: the first one completes the wait, but it is the newest that says
    /// where objects go.
    /// </summary>
    internal void AttachSubscriber(ulong alias)
    {
        Volatile.Write(ref _alias, alias);
        _subscribed.TrySetResult(alias);
    }

    /// <summary>Ends the wait for a subscriber: none can arrive now, so waiting is not an option.</summary>
    internal void FailSubscribers(Exception reason) => _subscribed.TrySetException(reason);

    /// <summary>
    /// Opens a subgroup stream and returns a writer for its objects, awaiting the first subscriber
    /// if none has arrived yet. Dispose or complete the returned writer when the subgroup is done.
    /// Set <paramref name="hasExtensions"/> when the objects carry Extension Headers — it selects
    /// the header's Properties bit, which every object on the stream must then honour.
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
    /// at 0 for one-subgroup-per-group; a publisher that opens several must give each its own.
    /// </para>
    /// </summary>
    [SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope",
        Justification = "The opened stream is owned by the returned MoqGroupWriter and disposed there.")]
    public async ValueTask<MoqGroupWriter> BeginGroupAsync(ulong groupId, byte publisherPriority,
        bool hasExtensions = false, bool endOfGroup = false, ulong subgroupId = 0,
        CancellationToken cancellationToken = default)
    {
        // _subscribed is this track's own TCS, completed by AttachSubscriber on the demux loop;
        // awaiting it is not the foreign-task deadlock hazard VSTHRD003 guards against.
#pragma warning disable VSTHRD003
        await _subscribed.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
#pragma warning restore VSTHRD003
        // Read the alias per group rather than taking the one the wait returned: a track outlives
        // its subscriptions, and each new one renames it.
        ulong alias = CurrentAlias;
        IQuicStream stream = await _connection
            .OpenStreamAsync(QuicStreamDirection.Unidirectional, cancellationToken).ConfigureAwait(false);
        var header = new SubgroupHeader
        {
            TrackAlias = alias,
            GroupId = groupId,
            SubgroupIdMode = SubgroupIdMode.Explicit,
            SubgroupId = subgroupId,
            HasProperties = hasExtensions,
            EndOfGroup = endOfGroup,
            PublisherPriority = publisherPriority,
        };
        return new MoqGroupWriter(stream, header);
    }
}

/// <summary>
/// Writes the objects of one group onto a subgroup stream, then FINs it. Object IDs must strictly
/// increase (the wire uses delta encoding). Obtain one from <see cref="MoqPublishedTrack.BeginGroupAsync"/>.
/// </summary>
public sealed class MoqGroupWriter : IAsyncDisposable
{
    private readonly IQuicStream _stream;
    private readonly SubgroupHeader _header;
    private readonly SubgroupStreamWriter _writer;

    internal MoqGroupWriter(IQuicStream stream, SubgroupHeader header)
    {
        _stream = stream;
        _header = header;
        _writer = new SubgroupStreamWriter(stream, header);
    }

    /// <summary>The group these objects belong to.</summary>
    public ulong GroupId => _header.GroupId;

    /// <summary>The subgroup within that group these objects are on.</summary>
    public ulong SubgroupId => _header.SubgroupId;

    /// <summary>
    /// Appends one object (with the group's priority and subgroup 0) to the stream, optionally
    /// carrying Extension Headers — which requires the group to have been opened with
    /// <c>hasExtensions</c>.
    /// </summary>
    public ValueTask WriteObjectAsync(ulong objectId, ReadOnlyMemory<byte> payload,
        IReadOnlyList<MoqKeyValuePair>? extensions = null, CancellationToken cancellationToken = default) =>
        _writer.WriteObjectAsync(
            MoqObject.Normal(_header.GroupId, objectId, _header.SubgroupId, _header.PublisherPriority, payload,
                extensions),
            cancellationToken);

    /// <summary>
    /// Appends a zero-length object with the End of Group status (§11.2.1.1): no object at or after
    /// <paramref name="objectId"/> exists in the group. This says on the wire what the header's
    /// END_OF_GROUP bit says in the header — which is what a publisher needs when it could not have
    /// known the group was ending at the time it opened the stream, because the bit is written with
    /// the header and the news arrives later.
    /// </summary>
    public ValueTask WriteEndOfGroupAsync(ulong objectId, CancellationToken cancellationToken = default) =>
        _writer.WriteObjectAsync(
            new MoqObject(_header.GroupId, objectId, _header.SubgroupId, _header.PublisherPriority,
                MoqObjectStatus.EndOfGroup, ReadOnlyMemory<byte>.Empty),
            cancellationToken);

    /// <summary>FINs the subgroup stream, signalling the subgroup is complete.</summary>
    public ValueTask CompleteAsync(CancellationToken cancellationToken = default) =>
        _writer.CompleteAsync(cancellationToken);

    /// <inheritdoc />
    public ValueTask DisposeAsync() => _stream.DisposeAsync();
}
