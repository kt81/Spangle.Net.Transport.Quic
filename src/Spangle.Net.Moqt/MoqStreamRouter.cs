using Spangle.Net.Moqt.Data;
using Spangle.Net.Moqt.Wire;
using Spangle.Net.Transport.Quic;

namespace Spangle.Net.Moqt;

/// <summary>
/// Classifies an incoming QUIC stream into the MOQT stream it carries, so a caller that does
/// not know the wire format never has to read a stream-type varint by hand. After the SETUP
/// handshake (<see cref="MoqSession"/>), a peer opens two kinds of stream on the connection:
/// a bidirectional <em>request</em> stream that begins with a control message (SUBSCRIBE, and
/// later PUBLISH/FETCH), and a unidirectional <em>subgroup</em> data stream that begins with a
/// SUBGROUP_HEADER (draft-18 §3.4, §11.4.2). This router reads that leading element and hands
/// back a typed <see cref="MoqIncomingStream"/> the caller can pattern-match on.
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
    /// Classifies an already-accepted <paramref name="stream"/>, reading its leading element:
    /// a bidirectional stream is a request stream (its first control message is read), a
    /// unidirectional stream is a subgroup data stream (its SUBGROUP_HEADER is read). Throws
    /// <see cref="MoqProtocolException"/> if a unidirectional stream does not start with a
    /// valid SUBGROUP_HEADER — the only unidirectional data stream this implementation carries.
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

        SubgroupStreamReader reader = await SubgroupStreamReader.OpenAsync(stream, cancellationToken)
            .ConfigureAwait(false);
        return new MoqSubgroupStream(stream, reader);
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
