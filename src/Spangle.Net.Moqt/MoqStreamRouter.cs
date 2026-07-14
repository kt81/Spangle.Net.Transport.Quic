using Spangle.Net.Moqt.Data;
using Spangle.Net.Moqt.Wire;
using Spangle.Net.Transport.Quic;

namespace Spangle.Net.Moqt;

/// <summary>
/// Classifies an incoming QUIC stream into the MOQT stream it carries, so a caller that does
/// not know the wire format never has to read a stream-type varint by hand. After the SETUP
/// handshake (<see cref="MoqSession"/>), a peer opens a bidirectional <em>request</em> stream
/// that begins with a control message (SUBSCRIBE, PUBLISH, FETCH…), or a unidirectional
/// <em>data</em> stream that begins with a type varint selecting a subgroup (SUBGROUP_HEADER)
/// or a fetch response (FETCH_HEADER) — draft-18 §3.4, §11.4.2, §11.4.4. This router reads that
/// leading element and hands back a typed <see cref="MoqIncomingStream"/> to pattern-match on.
/// </summary>
public static class MoqStreamRouter
{
    /// <summary>Accepts the next stream on <paramref name="connection"/> and classifies it.</summary>
    public static async ValueTask<MoqIncomingStream> AcceptAsync(IQuicConnection connection,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(connection);
        IQuicStream stream = await connection.AcceptStreamAsync(cancellationToken).ConfigureAwait(false);
        return await ClassifyAsync(stream, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Classifies an already-accepted <paramref name="stream"/>, reading its leading element: a
    /// bidirectional stream is a request stream (its first control message is read), and a
    /// unidirectional stream leads with a type varint that selects its data stream — a
    /// SUBGROUP_HEADER or a FETCH_HEADER (§3.4, Table 3). Throws
    /// <see cref="MoqProtocolException"/> on any other unidirectional type, which the spec says
    /// an endpoint MUST close the session over rather than skip.
    /// </summary>
    public static async ValueTask<MoqIncomingStream> ClassifyAsync(IQuicStream stream,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(stream);

        if (stream.Direction == QuicStreamDirection.Bidirectional)
        {
            (ulong messageType, byte[] payload) =
                await ControlMessage.ReadAsync(stream, cancellationToken).ConfigureAwait(false);
            return new MoqRequestStream(stream, messageType, payload);
        }

        ulong streamType = await StreamIo.ReadVarIntAsync(stream, cancellationToken).ConfigureAwait(false);
        if (streamType == FetchHeader.StreamType)
        {
            FetchHeader fetchHeader = await FetchHeader.ReadAfterTypeAsync(stream, cancellationToken)
                .ConfigureAwait(false);
            return new MoqFetchStream(stream, fetchHeader);
        }

        SubgroupHeader header = await SubgroupHeader.ReadAfterTypeAsync(stream, streamType, cancellationToken)
            .ConfigureAwait(false);
        return new MoqSubgroupStream(stream, SubgroupStreamReader.Create(stream, header));
    }
}

/// <summary>An incoming MOQT stream, classified by <see cref="MoqStreamRouter"/>.</summary>
public abstract class MoqIncomingStream
{
    private protected MoqIncomingStream(IQuicStream stream) => Stream = stream;

    /// <summary>The underlying QUIC stream (for a request stream, where the reply is written).</summary>
    public IQuicStream Stream { get; }
}

/// <summary>
/// A bidirectional request stream whose first control message has been read. The caller
/// switches on <see cref="MessageType"/> (a <see cref="MoqControlMessageType"/> code) and
/// decodes <see cref="Payload"/> with the matching message type — e.g.
/// <c>SubscribeMessage.DecodePayload(req.Payload.Span)</c> — then writes its response on
/// <see cref="MoqIncomingStream.Stream"/>.
/// </summary>
public sealed class MoqRequestStream : MoqIncomingStream
{
    internal MoqRequestStream(IQuicStream stream, ulong messageType, ReadOnlyMemory<byte> payload)
        : base(stream)
    {
        MessageType = messageType;
        Payload = payload;
    }

    /// <summary>The first control message's type code (e.g. <see cref="MoqControlMessageType.Subscribe"/>).</summary>
    public ulong MessageType { get; }

    /// <summary>The first control message's payload, ready for the matching <c>DecodePayload</c>.</summary>
    public ReadOnlyMemory<byte> Payload { get; }
}

/// <summary>
/// A unidirectional subgroup data stream whose SUBGROUP_HEADER has been read. The caller reads
/// objects off <see cref="Reader"/> until it returns null (the stream FINs).
/// </summary>
public sealed class MoqSubgroupStream : MoqIncomingStream
{
    internal MoqSubgroupStream(IQuicStream stream, SubgroupStreamReader reader)
        : base(stream) => Reader = reader;

    /// <summary>The reader positioned at the first object, its <see cref="SubgroupStreamReader.Header"/> read.</summary>
    public SubgroupStreamReader Reader { get; }
}

/// <summary>
/// A unidirectional fetch data stream whose FETCH_HEADER has been read. Unlike a subgroup stream
/// this hands back no ready-made reader: a fetch response's Group ID Deltas only resolve against
/// the group order the fetch asked for, which lives with the request, not on the wire. The caller
/// matches <see cref="Header"/>'s Request ID to the FETCH it sent and calls
/// <see cref="OpenReader"/> with that request's order, then reads entries until null.
/// </summary>
public sealed class MoqFetchStream : MoqIncomingStream
{
    private bool _opened;

    internal MoqFetchStream(IQuicStream stream, FetchHeader header)
        : base(stream) => Header = header;

    /// <summary>The FETCH_HEADER, whose Request ID names the FETCH this stream answers.</summary>
    public FetchHeader Header { get; }

    /// <summary>
    /// Creates the reader for the rest of the stream. <paramref name="groupOrder"/> must be the
    /// order the answered FETCH used; the spec's default when a FETCH omits it is Ascending.
    /// </summary>
    public FetchStreamReader OpenReader(MoqGroupOrder groupOrder = MoqGroupOrder.Ascending)
    {
        if (_opened)
        {
            throw new InvalidOperationException("The reader for this fetch stream has already been opened.");
        }

        _opened = true;
        return FetchStreamReader.Create(Stream, Header, groupOrder);
    }
}
