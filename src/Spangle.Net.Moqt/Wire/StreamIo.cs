using Spangle.Net.Transport.Quic;

namespace Spangle.Net.Moqt.Wire;

/// <summary>
/// Reads MOQT wire values straight off a QUIC stream, one field at a time — for the data
/// plane, where a stream carries a header then an open-ended run of objects and the reader
/// must tell a clean end-of-stream (FIN at a value boundary) from a truncated one.
/// </summary>
internal static class StreamIo
{
    /// <summary>
    /// Fills <paramref name="buffer"/> completely. Returns false only on a clean end of
    /// stream before the first byte; throws if the stream ends partway through.
    /// </summary>
    public static async ValueTask<bool> ReadFullyAsync(IQuicStream stream, Memory<byte> buffer,
        CancellationToken cancellationToken)
    {
        int total = 0;
        while (total < buffer.Length)
        {
            int read = await stream.ReadAsync(buffer[total..], cancellationToken).ConfigureAwait(false);
            if (read == 0)
            {
                if (total == 0)
                {
                    return false;
                }

                throw new MoqProtocolException("The stream ended in the middle of a value.");
            }

            total += read;
        }

        return true;
    }

    /// <summary>Reads one byte, or -1 at a clean end of stream (a value boundary).</summary>
    public static async ValueTask<int> TryReadByteAsync(IQuicStream stream, CancellationToken cancellationToken)
    {
        var one = new byte[1];
        int read = await stream.ReadAsync(one, cancellationToken).ConfigureAwait(false);
        return read == 0 ? -1 : one[0];
    }

    /// <summary>Completes a variable-length integer whose first byte has already been read.</summary>
    public static async ValueTask<ulong> ReadVarIntAsync(IQuicStream stream, int firstByte,
        CancellationToken cancellationToken)
    {
        int length = VarInt.GetEncodedLength((byte)firstByte);
        var bytes = new byte[length];
        bytes[0] = (byte)firstByte;
        if (length > 1 && !await ReadFullyAsync(stream, bytes.AsMemory(1), cancellationToken).ConfigureAwait(false))
        {
            throw new MoqProtocolException("The stream ended in the middle of a variable-length integer.");
        }

        VarInt.TryRead(bytes, out ulong value, out _);
        return value;
    }

    /// <summary>Reads one full variable-length integer; throws at end of stream.</summary>
    public static async ValueTask<ulong> ReadVarIntAsync(IQuicStream stream, CancellationToken cancellationToken)
    {
        int firstByte = await TryReadByteAsync(stream, cancellationToken).ConfigureAwait(false);
        if (firstByte < 0)
        {
            throw new MoqProtocolException("The stream ended where a variable-length integer was expected.");
        }

        return await ReadVarIntAsync(stream, firstByte, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Reads exactly <paramref name="count"/> bytes; throws if the stream ends first.</summary>
    public static async ValueTask<byte[]> ReadExactAsync(IQuicStream stream, int count,
        CancellationToken cancellationToken)
    {
        var bytes = new byte[count];
        if (count > 0 && !await ReadFullyAsync(stream, bytes, cancellationToken).ConfigureAwait(false))
        {
            throw new MoqProtocolException("The stream ended where a payload was expected.");
        }

        return bytes;
    }
}
