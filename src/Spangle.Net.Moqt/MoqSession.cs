using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using System.Threading.Channels;
using Spangle.Net.Moqt.Messages;
using Spangle.Net.Moqt.Wire;
using Spangle.Net.Transport.Quic;

namespace Spangle.Net.Moqt;

/// <summary>
/// A MOQT session over one QUIC connection. draft-18 (§10) exchanges control messages on a
/// pair of unidirectional streams — each endpoint opens its own outbound control stream and
/// begins it with SETUP. This type performs that handshake, then <see cref="RunAsync"/> is the
/// session's one demux loop: it pumps the peer's control stream (where GOAWAY arrives), accepts
/// every incoming stream, and routes each to whoever it belongs to — request streams to the
/// registered handler (<see cref="OnRequest"/>), subgroup streams to the subscription that
/// claimed their Track Alias, FETCH streams to the request that awaits them. One loop owns the
/// accepting, so any number of subscriptions can coexist on the session without racing each
/// other for streams.
/// </summary>
public sealed class MoqSession : IAsyncDisposable
{
    private readonly IQuicConnection _connection;
    private readonly IQuicStream _outboundControl;
    private readonly IQuicStream _inboundControl;
    private readonly MoqSessionOptions _options;

    private readonly Lock _stateLock = new();
    private readonly Dictionary<ulong, SubgroupRoute> _subgroupRoutes = new();
    private readonly Dictionary<ulong, FetchRoute> _fetchRoutes = new();
    private readonly Channel<MoqRequestStream> _requests =
        Channel.CreateUnbounded<MoqRequestStream>(new UnboundedChannelOptions { SingleReader = true });
    private readonly TaskCompletionSource<GoAwayMessage> _goAwayReceived =
        new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly TaskCompletionSource _demuxFailed =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    private Func<MoqRequestStream, CancellationToken, Task>? _requestHandler;
    private int _unclaimedStreams;
    private bool _runStarted;

    private MoqSession(IQuicConnection connection, IQuicStream outboundControl, IQuicStream inboundControl,
        SetupMessage localSetup, SetupMessage remoteSetup, bool isServer, MoqSessionOptions options)
    {
        _connection = connection;
        _outboundControl = outboundControl;
        _inboundControl = inboundControl;
        _options = options;
        LocalSetup = localSetup;
        RemoteSetup = remoteSetup;
        IsServer = isServer;
    }

    /// <summary>The SETUP this endpoint sent.</summary>
    public SetupMessage LocalSetup { get; }

    /// <summary>The SETUP the peer sent.</summary>
    public SetupMessage RemoteSetup { get; }

    /// <summary>Whether this endpoint accepted the connection (server role).</summary>
    public bool IsServer { get; }

    /// <summary>
    /// Completes when the peer sends GOAWAY (§10.4): the session should drain and reconnect —
    /// to <see cref="GoAwayMessage.NewSessionUri"/> when one is offered. Nothing else observes
    /// this; a consumer that ignores it simply rides the session until the peer closes it.
    /// </summary>
    // The TCS is completed by this session's own control pump; handing its Task out is not the
    // foreign-task hazard VSTHRD003 warns about.
#pragma warning disable VSTHRD003
    public Task<GoAwayMessage> GoAwayReceived => _goAwayReceived.Task;
#pragma warning restore VSTHRD003

    /// <summary>
    /// The underlying connection, on which the publisher/subscriber facades open request and
    /// data streams. The session owns its lifetime; callers borrow it.
    /// </summary>
    internal IQuicConnection Connection => _connection;

    /// <summary>
    /// Opens a new bidirectional request stream. In draft-18 every request/reply pair runs on its
    /// own stream (§10 Table: SUBSCRIBE, PUBLISH, FETCH, TRACK_STATUS, PUBLISH_NAMESPACE,
    /// SUBSCRIBE_NAMESPACE and SUBSCRIBE_TRACKS are each "Request, First"), so the Request ID in
    /// the reply is implicit and the stream's lifetime is the request's. The caller writes the
    /// opening control message and reads the reply on the returned stream.
    /// </summary>
    public ValueTask<IQuicStream> OpenRequestStreamAsync(CancellationToken cancellationToken = default) =>
        _connection.OpenStreamAsync(QuicStreamDirection.Bidirectional, cancellationToken);

    /// <summary>
    /// Establishes a session as the client: open the outbound control stream and send SETUP,
    /// then read the peer's SETUP off its control stream.
    /// </summary>
    [SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope",
        Justification = "The two control streams are owned by the returned MoqSession and closed in its DisposeAsync.")]
    public static async Task<MoqSession> ConnectAsync(IQuicConnection connection, SetupMessage localSetup,
        MoqSessionOptions? options = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentNullException.ThrowIfNull(localSetup);

        IQuicStream outbound = await connection.OpenStreamAsync(QuicStreamDirection.Unidirectional, cancellationToken)
            .ConfigureAwait(false);
        await WriteSetupAsync(outbound, localSetup, cancellationToken).ConfigureAwait(false);

        IQuicStream inbound = await connection.AcceptStreamAsync(cancellationToken).ConfigureAwait(false);
        SetupMessage remote = await ReadSetupAsync(inbound, cancellationToken).ConfigureAwait(false);

        return new MoqSession(connection, outbound, inbound, localSetup, remote, isServer: false,
            options ?? MoqSessionOptions.Default);
    }

    /// <summary>
    /// Establishes a session as the server: read the peer's SETUP off the control stream it
    /// opened, then open the outbound control stream and send SETUP.
    /// </summary>
    [SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope",
        Justification = "The two control streams are owned by the returned MoqSession and closed in its DisposeAsync.")]
    public static async Task<MoqSession> AcceptAsync(IQuicConnection connection, SetupMessage localSetup,
        MoqSessionOptions? options = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentNullException.ThrowIfNull(localSetup);

        IQuicStream inbound = await connection.AcceptStreamAsync(cancellationToken).ConfigureAwait(false);
        SetupMessage remote = await ReadSetupAsync(inbound, cancellationToken).ConfigureAwait(false);

        IQuicStream outbound = await connection.OpenStreamAsync(QuicStreamDirection.Unidirectional, cancellationToken)
            .ConfigureAwait(false);
        await WriteSetupAsync(outbound, localSetup, cancellationToken).ConfigureAwait(false);

        return new MoqSession(connection, outbound, inbound, localSetup, remote, isServer: true,
            options ?? MoqSessionOptions.Default);
    }

    /// <summary>
    /// Registers the handler for incoming request streams (SUBSCRIBE and friends). At most one
    /// handler per session — the publisher facade registers itself. Without one, every incoming
    /// request stream is disposed: an accepted stream holds inbound-stream credit, so a session
    /// that consumes no requests must still close them.
    /// </summary>
    public void OnRequest(Func<MoqRequestStream, CancellationToken, Task> handler)
    {
        ArgumentNullException.ThrowIfNull(handler);
        if (Interlocked.CompareExchange(ref _requestHandler, handler, null) is not null)
        {
            throw new InvalidOperationException("A request handler is already registered on this session.");
        }
    }

    /// <summary>
    /// Runs the session's demux loop until cancellation: the control pump (GOAWAY, and the death
    /// of the control stream — which §3.3 makes a session error), the accept loop, and request
    /// dispatch. Nothing arrives anywhere — no subscription's objects, no publisher's requests —
    /// unless this is running, and it runs at most once per session. It returns normally on
    /// cancellation and throws when the session dies under it (the connection is gone, or the
    /// peer broke the protocol); the caller awaiting it is the one who learns why.
    /// </summary>
    public Task RunAsync(CancellationToken cancellationToken = default)
    {
        lock (_stateLock)
        {
            if (_runStarted)
            {
                throw new InvalidOperationException("The session's demux loop is already running.");
            }

            _runStarted = true;
        }

        return RunCoreAsync(cancellationToken);
    }

    private async Task RunCoreAsync(CancellationToken cancellationToken)
    {
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        CancellationToken token = linked.Token;
        Task control = PumpControlAsync(token);
        Task accept = AcceptStreamsAsync(token);
        Task dispatch = DispatchRequestsAsync(token);

        try
        {
            // The first loop to end carries the reason the session is over; the others are
            // stopped below. None of them ends on its own while the session is healthy.
            // (_demuxFailed is how a classification task reports a protocol violation — it
            // may never complete, so it is raced here but never drained below.)
#pragma warning disable VSTHRD003 // our own TCS, completed by this session's classification tasks
            Task first = await Task.WhenAny(control, accept, dispatch, _demuxFailed.Task).ConfigureAwait(false);
            await first.ConfigureAwait(false);
#pragma warning restore VSTHRD003
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // cancelled shutdown is the ordinary end of a session
        }
        finally
        {
            await linked.CancelAsync().ConfigureAwait(false);
            foreach (Task loop in new[] { control, accept, dispatch })
            {
                try
                {
                    await loop.ConfigureAwait(false);
                }
                catch (Exception)
                {
                    // the reason the session ended already propagated from the first loop
                }
            }

            await TearDownRoutesAsync().ConfigureAwait(false);
        }
    }

    // Reads the peer's control stream for the life of the session. After SETUP the only message
    // with a control-stream home in draft-18 is GOAWAY (§10.4); anything else there — SETUP
    // again included — is a protocol violation. The stream closing at all is one too (§3.3).
    private async Task PumpControlAsync(CancellationToken token)
    {
        while (true)
        {
            token.ThrowIfCancellationRequested();
            (ulong Type, byte[] Payload)? message =
                await ControlMessage.TryReadAsync(_inboundControl, token).ConfigureAwait(false);
            if (message is null)
            {
                throw new MoqProtocolException(
                    "The peer closed its control stream, which ends the session (§3.3).");
            }

            (ulong type, byte[] payload) = message.Value;
            if (type == MoqControlMessageType.GoAway)
            {
                // The session drains from here; when to actually leave is the consumer's call.
                _goAwayReceived.TrySetResult(GoAwayMessage.DecodePayload(payload));
                continue;
            }

            throw new MoqProtocolException(
                $"0x{type:X} is not a control-stream message after SETUP; only GOAWAY lives there (§10).");
        }
    }

    private async Task AcceptStreamsAsync(CancellationToken token)
    {
        while (true)
        {
            token.ThrowIfCancellationRequested();
            IQuicStream stream = await _connection.AcceptStreamAsync(token).ConfigureAwait(false);
            // Classification reads the stream's leading element, and a peer that opens a stream
            // and sends nothing must not stall every stream behind it — so each stream is
            // classified off the accept loop. A protocol violation on any of them still ends
            // the session (via _demuxFailed); a mere stream failure costs that stream.
            _ = ClassifyAndRouteAsync(stream, token);
        }
    }

    private async Task ClassifyAndRouteAsync(IQuicStream stream, CancellationToken token)
    {
        MoqIncomingStream incoming;
        try
        {
            incoming = await MoqStreamRouter.ClassifyAsync(stream, _options.ReadLimits, token).ConfigureAwait(false);
        }
        catch (MoqProtocolException e)
        {
            // An unknown stream type or malformed header is what the spec says MUST close the
            // whole session — surfaced through the demux, whose caller awaits the reason.
            _demuxFailed.TrySetException(e);
            await DisposeQuietlyAsync(stream).ConfigureAwait(false);
            return;
        }
        catch (Exception)
        {
            // The peer may abort any stream it opened, before or during its leading element;
            // that costs the stream, not the session. (Cancellation lands here too — the
            // session is shutting down and the stream goes with it.)
            await DisposeQuietlyAsync(stream).ConfigureAwait(false);
            return;
        }

        try
        {
            switch (incoming)
            {
                case MoqPaddingStream padding:
                    padding.BeginDiscard();
                    break;
                case MoqRequestStream request:
                    if (!_requests.Writer.TryWrite(request))
                    {
                        await DisposeQuietlyAsync(stream).ConfigureAwait(false);
                    }

                    break;
                case MoqSubgroupStream subgroup:
                    await RouteSubgroupAsync(subgroup).ConfigureAwait(false);
                    break;
                case MoqFetchStream fetch:
                    await RouteFetchAsync(fetch).ConfigureAwait(false);
                    break;
                default:
                    await DisposeQuietlyAsync(stream).ConfigureAwait(false);
                    break;
            }
        }
        catch (Exception e) when (e is not OperationCanceledException)
        {
            // This task's exception has no other observer; routing must not fail silently.
            _demuxFailed.TrySetException(e);
            await DisposeQuietlyAsync(stream).ConfigureAwait(false);
        }
    }

    // Requests are dispatched one at a time off a queue, so the handler runs single-threaded —
    // a publisher's track table and alias counter need no locking — and requests are answered
    // in arrival order.
    private async Task DispatchRequestsAsync(CancellationToken token)
    {
        await foreach (MoqRequestStream request in _requests.Reader.ReadAllAsync(token).ConfigureAwait(false))
        {
            if (Volatile.Read(ref _requestHandler) is { } handler)
            {
                await handler(request, token).ConfigureAwait(false);
            }
            else
            {
                await DisposeQuietlyAsync(request.Stream).ConfigureAwait(false);
            }
        }
    }

    private async ValueTask RouteSubgroupAsync(MoqSubgroupStream subgroup)
    {
        ulong alias = subgroup.Reader.Header.TrackAlias;
        var routed = false;
        lock (_stateLock)
        {
            // The claim races the data: SUBSCRIBE_OK and the first subgroup stream may be in
            // flight together, so an unknown alias is held briefly — but boundedly, because
            // each held stream pins inbound-stream credit.
            bool haveRoute = _subgroupRoutes.TryGetValue(alias, out SubgroupRoute? route);
            if (!haveRoute && _unclaimedStreams < _options.MaxUnclaimedStreams)
            {
                route = new SubgroupRoute();
                _subgroupRoutes[alias] = route;
                haveRoute = true;
            }

            if (haveRoute && (route!.Claimed || _unclaimedStreams < _options.MaxUnclaimedStreams))
            {
                routed = route.Channel.Writer.TryWrite(subgroup);
                if (routed && !route.Claimed)
                {
                    route.QueuedUnclaimed++;
                    _unclaimedStreams++;
                }
            }
        }

        if (!routed)
        {
            await DisposeQuietlyAsync(subgroup.Stream).ConfigureAwait(false);
        }
    }

    private async ValueTask RouteFetchAsync(MoqFetchStream fetch)
    {
        TaskCompletionSource<MoqFetchStream>? waiter = null;
        var held = false;
        lock (_stateLock)
        {
            if (_fetchRoutes.TryGetValue(fetch.Header.RequestId, out FetchRoute? route))
            {
                if (route.Waiter is not null)
                {
                    waiter = route.Waiter;
                    _fetchRoutes.Remove(fetch.Header.RequestId);
                }

                // else: a second stream answering the same fetch — a peer anomaly; the first
                // one already holds the route, and this one is disposed below.
            }
            else if (_unclaimedStreams < _options.MaxUnclaimedStreams)
            {
                _fetchRoutes[fetch.Header.RequestId] = new FetchRoute { Arrived = fetch };
                _unclaimedStreams++;
                held = true;
            }
        }

        if (waiter is not null)
        {
            waiter.TrySetResult(fetch);
        }
        else if (!held)
        {
            await DisposeQuietlyAsync(fetch.Stream).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Returns the FETCH data stream answering <paramref name="requestId"/> once the peer opens
    /// it. Call after (or concurrently with) sending the FETCH whose reply it is; the demux
    /// holds an early-arriving stream briefly, so the race between the reply and the data is
    /// safe either way.
    /// </summary>
    public Task<MoqFetchStream> ReceiveFetchStreamAsync(ulong requestId, CancellationToken cancellationToken = default)
    {
        TaskCompletionSource<MoqFetchStream> waiter;
        lock (_stateLock)
        {
            RequireRunning();
            if (_fetchRoutes.TryGetValue(requestId, out FetchRoute? route))
            {
                if (route.Arrived is { } arrived)
                {
                    _fetchRoutes.Remove(requestId);
                    _unclaimedStreams--;
                    return Task.FromResult(arrived);
                }

                throw new InvalidOperationException(
                    $"FETCH request {requestId} already has a waiter; a fetch has exactly one reply stream.");
            }

            waiter = new TaskCompletionSource<MoqFetchStream>(TaskCreationOptions.RunContinuationsAsynchronously);
            _fetchRoutes[requestId] = new FetchRoute { Waiter = waiter };
        }

        return waiter.Task.WaitAsync(cancellationToken);
    }

    /// <summary>
    /// Claims the subgroup streams carrying <paramref name="alias"/>: everything already held
    /// for it and everything still to come flows through the returned reader, which completes
    /// when the session's demux loop ends. One claim per alias.
    /// </summary>
    internal ChannelReader<MoqSubgroupStream> ClaimSubgroups(ulong alias)
    {
        lock (_stateLock)
        {
            RequireRunning();
            if (!_subgroupRoutes.TryGetValue(alias, out SubgroupRoute? route))
            {
                route = new SubgroupRoute();
                _subgroupRoutes[alias] = route;
            }

            if (route.Claimed)
            {
                throw new InvalidOperationException($"Track Alias {alias} is already claimed by a subscription.");
            }

            route.Claimed = true;
            _unclaimedStreams -= route.QueuedUnclaimed;
            route.QueuedUnclaimed = 0;
            return route.Channel.Reader;
        }
    }

    /// <summary>Releases a claim: the route is dropped and anything still queued is closed.</summary>
    internal async ValueTask ReleaseSubgroupsAsync(ulong alias)
    {
        SubgroupRoute? route;
        lock (_stateLock)
        {
            if (_subgroupRoutes.Remove(alias, out route))
            {
                route.Channel.Writer.TryComplete();
                _unclaimedStreams -= route.QueuedUnclaimed;
                route.QueuedUnclaimed = 0;
            }
        }

        if (route is not null)
        {
            while (route.Channel.Reader.TryRead(out MoqSubgroupStream? queued))
            {
                await DisposeQuietlyAsync(queued.Stream).ConfigureAwait(false);
            }
        }
    }

    private void RequireRunning()
    {
        if (!_runStarted)
        {
            throw new InvalidOperationException(
                "Run the session's demux loop (MoqSession.RunAsync) first — nothing arrives without it.");
        }
    }

    // The demux is over: complete every route so consumers finish, close what nobody claimed
    // (claimed streams belong to their consumers, who drain and dispose them), and tell every
    // fetch waiter the stream it awaits can no longer arrive.
    private async ValueTask TearDownRoutesAsync()
    {
        List<MoqSubgroupStream> orphanSubgroups = [];
        List<MoqFetchStream> orphanFetches = [];
        List<TaskCompletionSource<MoqFetchStream>> fetchWaiters = [];
        lock (_stateLock)
        {
            foreach (SubgroupRoute route in _subgroupRoutes.Values)
            {
                route.Channel.Writer.TryComplete();
                if (!route.Claimed)
                {
                    while (route.Channel.Reader.TryRead(out MoqSubgroupStream? queued))
                    {
                        orphanSubgroups.Add(queued);
                    }
                }
            }

            _subgroupRoutes.Clear();

            foreach (FetchRoute route in _fetchRoutes.Values)
            {
                if (route.Arrived is { } arrived)
                {
                    orphanFetches.Add(arrived);
                }

                if (route.Waiter is { } waiter)
                {
                    fetchWaiters.Add(waiter);
                }
            }

            _fetchRoutes.Clear();
            _unclaimedStreams = 0;
            _requests.Writer.TryComplete();
        }

        foreach (MoqSubgroupStream orphan in orphanSubgroups)
        {
            await DisposeQuietlyAsync(orphan.Stream).ConfigureAwait(false);
        }

        foreach (MoqFetchStream orphan in orphanFetches)
        {
            await DisposeQuietlyAsync(orphan.Stream).ConfigureAwait(false);
        }

        foreach (TaskCompletionSource<MoqFetchStream> waiter in fetchWaiters)
        {
            waiter.TrySetException(
                new MoqProtocolException("The session ended before the FETCH stream arrived."));
        }

        while (_requests.Reader.TryRead(out MoqRequestStream? request))
        {
            await DisposeQuietlyAsync(request.Stream).ConfigureAwait(false);
        }
    }

    private static async ValueTask DisposeQuietlyAsync(IQuicStream stream)
    {
        try
        {
            await stream.DisposeAsync().ConfigureAwait(false);
        }
        catch (Exception)
        {
            // closing a stream the peer already tore down is best-effort by definition
        }
    }

    private static async Task WriteSetupAsync(IQuicStream stream, SetupMessage setup, CancellationToken cancellationToken)
    {
        var payload = new ArrayBufferWriter<byte>();
        setup.EncodePayload(new MoqWriter(payload));

        var frame = new ArrayBufferWriter<byte>();
        ControlMessage.Write(frame, MoqControlMessageType.Setup, payload.WrittenSpan);

        // The control stream stays open for later messages, so do not complete writes here.
        await stream.WriteAsync(frame.WrittenMemory, completeWrites: false, cancellationToken).ConfigureAwait(false);
    }

    private static async Task<SetupMessage> ReadSetupAsync(IQuicStream stream, CancellationToken cancellationToken)
    {
        (ulong type, byte[] payload) = await ControlMessage.ReadAsync(stream, cancellationToken).ConfigureAwait(false);
        if (type != MoqControlMessageType.Setup)
        {
            throw new MoqProtocolException(
                $"Expected SETUP (0x{MoqControlMessageType.Setup:X}) as the first control message, got 0x{type:X}.");
        }

        return SetupMessage.DecodePayload(payload);
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        // Close the connection first (error code 0 = no error). This aborts every stream, so the
        // stream and connection DisposeAsync calls below return at once instead of waiting on a
        // graceful FIN drain / idle timeout for streams the higher layers left open — which is
        // tens of seconds against a live peer.
#pragma warning disable CA1031 // best-effort during disposal: the peer may have already aborted
        try
        {
            await _connection.CloseAsync(0).ConfigureAwait(false);
        }
        catch (Exception)
        {
            // fall through to DisposeAsync regardless
        }
#pragma warning restore CA1031

        await _outboundControl.DisposeAsync().ConfigureAwait(false);
        await _inboundControl.DisposeAsync().ConfigureAwait(false);
        await _connection.DisposeAsync().ConfigureAwait(false);
    }

    // One subscription's stream feed. The channel is unbounded because its size is already
    // bounded by transport credit: every queued stream is an accepted stream, and QUIC caps
    // how many of those exist at once.
    private sealed class SubgroupRoute
    {
        public Channel<MoqSubgroupStream> Channel { get; } =
            System.Threading.Channels.Channel.CreateUnbounded<MoqSubgroupStream>(
                new UnboundedChannelOptions { SingleReader = true });

        public bool Claimed { get; set; }
        public int QueuedUnclaimed { get; set; }
    }

    private sealed class FetchRoute
    {
        public TaskCompletionSource<MoqFetchStream>? Waiter { get; init; }
        public MoqFetchStream? Arrived { get; init; }
    }
}
