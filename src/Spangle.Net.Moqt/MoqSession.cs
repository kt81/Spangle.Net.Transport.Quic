using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using Spangle.Net.Moqt.Messages;
using Spangle.Net.Moqt.Wire;
using Spangle.Net.Transport.Quic;

namespace Spangle.Net.Moqt;

/// <summary>
/// A MOQT session over one QUIC connection. draft-18 (§10) exchanges control messages on a
/// pair of unidirectional streams — each endpoint opens its own outbound control stream and
/// begins it with SETUP. This type performs that handshake and holds both control streams;
/// subscribe/publish and the object data plane build on top in later work.
/// </summary>
public sealed class MoqSession : IAsyncDisposable
{
    private readonly IQuicConnection _connection;
    private readonly IQuicStream _outboundControl;
    private readonly IQuicStream _inboundControl;

    private MoqSession(IQuicConnection connection, IQuicStream outboundControl, IQuicStream inboundControl,
        SetupMessage localSetup, SetupMessage remoteSetup, bool isServer)
    {
        _connection = connection;
        _outboundControl = outboundControl;
        _inboundControl = inboundControl;
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
    /// Establishes a session as the client: open the outbound control stream and send SETUP,
    /// then read the peer's SETUP off its control stream.
    /// </summary>
    [SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope",
        Justification = "The two control streams are owned by the returned MoqSession and closed in its DisposeAsync.")]
    public static async Task<MoqSession> ConnectAsync(IQuicConnection connection, SetupMessage localSetup,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentNullException.ThrowIfNull(localSetup);

        IQuicStream outbound = await connection.OpenStreamAsync(QuicStreamDirection.Unidirectional, cancellationToken)
            .ConfigureAwait(false);
        await WriteSetupAsync(outbound, localSetup, cancellationToken).ConfigureAwait(false);

        IQuicStream inbound = await connection.AcceptStreamAsync(cancellationToken).ConfigureAwait(false);
        SetupMessage remote = await ReadSetupAsync(inbound, cancellationToken).ConfigureAwait(false);

        return new MoqSession(connection, outbound, inbound, localSetup, remote, isServer: false);
    }

    /// <summary>
    /// Establishes a session as the server: read the peer's SETUP off the control stream it
    /// opened, then open the outbound control stream and send SETUP.
    /// </summary>
    [SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope",
        Justification = "The two control streams are owned by the returned MoqSession and closed in its DisposeAsync.")]
    public static async Task<MoqSession> AcceptAsync(IQuicConnection connection, SetupMessage localSetup,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentNullException.ThrowIfNull(localSetup);

        IQuicStream inbound = await connection.AcceptStreamAsync(cancellationToken).ConfigureAwait(false);
        SetupMessage remote = await ReadSetupAsync(inbound, cancellationToken).ConfigureAwait(false);

        IQuicStream outbound = await connection.OpenStreamAsync(QuicStreamDirection.Unidirectional, cancellationToken)
            .ConfigureAwait(false);
        await WriteSetupAsync(outbound, localSetup, cancellationToken).ConfigureAwait(false);

        return new MoqSession(connection, outbound, inbound, localSetup, remote, isServer: true);
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
        await _outboundControl.DisposeAsync().ConfigureAwait(false);
        await _inboundControl.DisposeAsync().ConfigureAwait(false);
        await _connection.DisposeAsync().ConfigureAwait(false);
    }
}
