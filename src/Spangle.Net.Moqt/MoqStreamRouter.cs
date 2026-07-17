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
    /// <summary>The PADDING stream type (§3.4): carries no MOQT data, only bytes to discard.</summary>
    private const ulong PaddingStreamType = 0x132B3E28;

    /// <summary>
    /// Accepts the next MOQT stream on <paramref name="connection"/> and classifies it. A PADDING
    /// stream never comes back from here: its bytes are discarded in the background (§11.5.1) and
    /// the accept loop continues to the next stream.
    /// </summary>
    public static async ValueTask<MoqIncomingStream> AcceptAsync(IQuicConnection connection,
        MoqReadLimits? limits = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(connection);
        while (true)
        {
            IQuicStream stream = await connection.AcceptStreamAsync(cancellationToken).ConfigureAwait(false);
            MoqIncomingStream incoming = await ClassifyAsync(stream, limits, cancellationToken).ConfigureAwait(false);
            if (incoming is MoqPaddingStream padding)
            {
                padding.BeginDiscard();
                continue;
            }

            return incoming;
        }
    }

    /// <summary>
    /// Classifies an already-accepted <paramref name="stream"/>, reading its leading element: a
    /// bidirectional stream is a request stream (its first control message is read), and a
    /// unidirectional stream leads with a type varint that selects its data stream — a
    /// SUBGROUP_HEADER or a FETCH_HEADER (§3.4, Table 3). A PADDING stream classifies as
    /// <see cref="MoqPaddingStream"/>, whose bytes the spec says to discard (§11.5.1). Throws
    /// <see cref="MoqProtocolException"/> on any other unidirectional type, which the spec says
    /// an endpoint MUST close the session over rather than skip.
    /// </summary>
    public static async ValueTask<MoqIncomingStream> ClassifyAsync(IQuicStream stream,
        MoqReadLimits? limits = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(stream);
        limits ??= MoqReadLimits.Default;

        if (stream.Direction == QuicStreamDirection.Bidirectional)
        {
            (ulong messageType, byte[] payload) =
                await ControlMessage.ReadAsync(stream, cancellationToken).ConfigureAwait(false);
            return new MoqRequestStream(stream, messageType, payload);
        }

        ulong streamType = await StreamIo.ReadVarIntAsync(stream, cancellationToken).ConfigureAwait(false);
        if (streamType == PaddingStreamType)
        {
            return new MoqPaddingStream(stream);
        }

        if (streamType == FetchHeader.StreamType)
        {
            FetchHeader fetchHeader = await FetchHeader.ReadAfterTypeAsync(stream, cancellationToken)
                .ConfigureAwait(false);
            return new MoqFetchStream(stream, fetchHeader, limits);
        }

        SubgroupHeader header = await SubgroupHeader.ReadAfterTypeAsync(stream, streamType, cancellationToken)
            .ConfigureAwait(false);
        return new MoqSubgroupStream(stream, SubgroupStreamReader.Create(stream, header, limits));
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
    private readonly MoqReadLimits _limits;
    private bool _opened;

    internal MoqFetchStream(IQuicStream stream, FetchHeader header, MoqReadLimits limits)
        : base(stream)
    {
        Header = header;
        _limits = limits;
    }

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
        return FetchStreamReader.Create(Stream, Header, groupOrder, _limits);
    }
}

/// <summary>
/// A unidirectional PADDING stream (§3.4, §11.5.1): it carries no MOQT data, and the spec has
/// its receiver discard the bytes — not reset the stream, which would tell an observer the
/// padding apart from data, and not treat it as unknown, which would close the session.
/// <see cref="MoqStreamRouter.AcceptAsync"/> handles these itself; only a direct
/// <see cref="MoqStreamRouter.ClassifyAsync"/> caller ever sees one, and calls
/// <see cref="BeginDiscard"/> on it.
/// </summary>
public sealed class MoqPaddingStream : MoqIncomingStream
{
    internal MoqPaddingStream(IQuicStream stream) : base(stream)
    {
    }

    /// <summary>
    /// Discards the stream's bytes in the background until it ends, then disposes it. Errors are
    /// ignored: nothing about a padding stream is worth reporting, and the drain task ends with
    /// the stream (or the connection under it) in every case.
    /// </summary>
    public void BeginDiscard() => _ = DiscardAsync();

    private async Task DiscardAsync()
    {
        var scratch = new byte[4096];
        try
        {
            while (await Stream.ReadAsync(scratch, CancellationToken.None).ConfigureAwait(false) > 0)
            {
            }
        }
        catch (Exception)
        {
            // A padding stream failing is of no consequence; the dispose below returns its credit.
        }
        finally
        {
            await Stream.DisposeAsync().ConfigureAwait(false);
        }
    }
}
