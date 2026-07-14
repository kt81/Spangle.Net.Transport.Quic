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
        ulong requestId = _nextRequestId;
        _nextRequestId += 2;

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
        _tracks[Key(name)] = track;
        return track;
    }

    /// <summary>
    /// Runs the request-stream demux loop until cancellation: every SUBSCRIBE that matches a
    /// declared track is answered with SUBSCRIBE_OK carrying a newly assigned Track Alias, after
    /// which the track streams its objects to that subscriber. A SUBSCRIBE for an unknown track
    /// is rejected by resetting the request stream (no SUBSCRIBE_ERROR yet).
    /// </summary>
    public async Task RunAsync(CancellationToken cancellationToken = default)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            MoqIncomingStream incoming;
            try
            {
                incoming = await MoqStreamRouter.AcceptAsync(_session.Connection, cancellationToken)
                    .ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }

            if (incoming is not MoqRequestStream request || request.MessageType != MoqControlMessageType.Subscribe)
            {
                continue; // this cut only handles SUBSCRIBE request streams
            }

            SubscribeMessage subscribe = SubscribeMessage.DecodePayload(request.Payload.Span);
            if (!_tracks.TryGetValue(Key(subscribe.Track), out MoqPublishedTrack? track))
            {
                request.Stream.Abort(0); // unknown track; no SUBSCRIBE_ERROR in this cut
                continue;
            }

            ulong alias = _nextAlias++;
            await WriteSubscribeOkAsync(request.Stream, alias, cancellationToken).ConfigureAwait(false);
            track.AttachSubscriber(alias);
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

    internal MoqPublishedTrack(IQuicConnection connection, FullTrackName name)
    {
        _connection = connection;
        Name = name;
    }

    /// <summary>The full name subscribers ask for.</summary>
    public FullTrackName Name { get; }

    /// <summary>
    /// Completes with the assigned Track Alias once a subscriber has subscribed to this track.
    /// The bridge can await this to avoid producing groups no one will receive.
    /// </summary>
    // The TCS is completed by AttachSubscriber on this same object; handing its Task out is not
    // the foreign-task hazard VSTHRD003 warns about.
#pragma warning disable VSTHRD003
    public Task<ulong> WaitForSubscriberAsync() => _subscribed.Task;
#pragma warning restore VSTHRD003

    internal void AttachSubscriber(ulong alias) => _subscribed.TrySetResult(alias);

    /// <summary>
    /// Opens a subgroup stream for a new group and returns a writer for its objects, awaiting the
    /// first subscriber if none has arrived yet. Each group is one subgroup stream (subgroup 0);
    /// dispose or complete the returned writer before beginning the next group. Set
    /// <paramref name="hasExtensions"/> when the objects carry Extension Headers — it selects the
    /// header's Properties bit, which every object on the stream must then honour.
    /// </summary>
    [SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope",
        Justification = "The opened stream is owned by the returned MoqGroupWriter and disposed there.")]
    public async ValueTask<MoqGroupWriter> BeginGroupAsync(ulong groupId, byte publisherPriority,
        bool hasExtensions = false, CancellationToken cancellationToken = default)
    {
        // _subscribed is this track's own TCS, completed by AttachSubscriber on the demux loop;
        // awaiting it is not the foreign-task deadlock hazard VSTHRD003 guards against.
#pragma warning disable VSTHRD003
        ulong alias = await _subscribed.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
#pragma warning restore VSTHRD003
        IQuicStream stream = await _connection
            .OpenStreamAsync(QuicStreamDirection.Unidirectional, cancellationToken).ConfigureAwait(false);
        var header = new SubgroupHeader
        {
            TrackAlias = alias,
            GroupId = groupId,
            SubgroupIdMode = SubgroupIdMode.Explicit,
            SubgroupId = 0,
            HasProperties = hasExtensions,
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

    /// <summary>FINs the subgroup stream, signalling the group is complete.</summary>
    public ValueTask CompleteAsync(CancellationToken cancellationToken = default) =>
        _writer.CompleteAsync(cancellationToken);

    /// <inheritdoc />
    public ValueTask DisposeAsync() => _stream.DisposeAsync();
}
