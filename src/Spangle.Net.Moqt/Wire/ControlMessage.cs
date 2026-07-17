using System.Buffers;
using System.Buffers.Binary;
using Spangle.Net.Transport.Quic;

namespace Spangle.Net.Moqt.Wire;

/// <summary>
/// The control-message frame (draft-18 §10, Figure 3): a varint Message Type, a 16-bit
/// Message Length, then that many payload bytes. Both control-stream and request-stream
/// messages share this framing.
/// </summary>
public static class ControlMessage
{
    /// <summary>Serializes one framed control message into <paramref name="output"/>.</summary>
    public static void Write(IBufferWriter<byte> output, ulong type, ReadOnlySpan<byte> payload)
    {
        ArgumentNullException.ThrowIfNull(output);
        if (payload.Length > ushort.MaxValue)
        {
            throw new ArgumentException("A control message payload may not exceed 2^16-1 bytes.", nameof(payload));
        }

        new MoqWriter(output).WriteVarInt(type);
        Span<byte> length = output.GetSpan(sizeof(ushort));
        BinaryPrimitives.WriteUInt16BigEndian(length, (ushort)payload.Length);
        output.Advance(sizeof(ushort));
        output.Write(payload);
    }

    /// <summary>
    /// Reads one framed control message from a QUIC stream: the varint type (sized by its
    /// first byte), the 16-bit length, then the payload. Throws
    /// <see cref="MoqProtocolException"/> if the stream ends mid-message.
    /// </summary>
    public static async ValueTask<(ulong Type, byte[] Payload)> ReadAsync(IQuicStream stream,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(stream);

        var firstByte = new byte[1];
        await ReadExactlyAsync(stream, firstByte, cancellationToken).ConfigureAwait(false);

        return await ReadAfterFirstByteAsync(stream, firstByte[0], cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Reads one framed control message, or null when the stream ends cleanly before one
    /// begins — the case a control-stream pump must tell apart from truncation, because a
    /// clean close is a deliberate act of the peer (and §3.3 makes it a session error), while
    /// ending mid-message still throws <see cref="MoqProtocolException"/>.
    /// </summary>
    public static async ValueTask<(ulong Type, byte[] Payload)?> TryReadAsync(IQuicStream stream,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(stream);

        var firstByte = new byte[1];
        int read = await stream.ReadAsync(firstByte, cancellationToken).ConfigureAwait(false);
        if (read == 0)
        {
            return null;
        }

        return await ReadAfterFirstByteAsync(stream, firstByte[0], cancellationToken).ConfigureAwait(false);
    }

    private static async ValueTask<(ulong Type, byte[] Payload)> ReadAfterFirstByteAsync(IQuicStream stream,
        byte firstByte, CancellationToken cancellationToken)
    {
        int typeLength = VarInt.GetEncodedLength(firstByte);
        var typeBytes = new byte[typeLength];
        typeBytes[0] = firstByte;
        if (typeLength > 1)
        {
            await ReadExactlyAsync(stream, typeBytes.AsMemory(1), cancellationToken).ConfigureAwait(false);
        }

        VarInt.TryRead(typeBytes, out ulong type, out _);

        var lengthBytes = new byte[sizeof(ushort)];
        await ReadExactlyAsync(stream, lengthBytes, cancellationToken).ConfigureAwait(false);
        int payloadLength = BinaryPrimitives.ReadUInt16BigEndian(lengthBytes);

        var payload = new byte[payloadLength];
        if (payloadLength > 0)
        {
            await ReadExactlyAsync(stream, payload, cancellationToken).ConfigureAwait(false);
        }

        return (type, payload);
    }

    private static async ValueTask ReadExactlyAsync(IQuicStream stream, Memory<byte> buffer,
        CancellationToken cancellationToken)
    {
        int total = 0;
        while (total < buffer.Length)
        {
            int read = await stream.ReadAsync(buffer[total..], cancellationToken).ConfigureAwait(false);
            if (read == 0)
            {
                throw new MoqProtocolException("The control stream ended in the middle of a message.");
            }

            total += read;
        }
    }
}
