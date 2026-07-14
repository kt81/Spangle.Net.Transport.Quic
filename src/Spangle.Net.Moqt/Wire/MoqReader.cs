using System.Text;

namespace Spangle.Net.Moqt.Wire;

/// <summary>
/// A forward-only reader over a MOQT message body: variable-length integers and the
/// length-prefixed byte strings and UTF-8 strings built on them. A ref struct so it stays
/// on the stack and borrows the caller's span without copying. Every read throws
/// <see cref="MoqProtocolException"/> on a truncated buffer rather than returning garbage.
/// </summary>
public ref struct MoqReader
{
    private readonly ReadOnlySpan<byte> _buffer;
    private int _position;

    /// <summary>Creates a reader positioned at the start of <paramref name="buffer"/>.</summary>
    public MoqReader(ReadOnlySpan<byte> buffer)
    {
        _buffer = buffer;
        _position = 0;
    }

    /// <summary>The number of bytes consumed so far.</summary>
    public readonly int Position => _position;

    /// <summary>The number of bytes not yet consumed.</summary>
    public readonly int Remaining => _buffer.Length - _position;

    /// <summary>Whether the whole buffer has been consumed.</summary>
    public readonly bool End => _position >= _buffer.Length;

    /// <summary>Reads one variable-length integer, or returns false if the buffer is short.</summary>
    public bool TryReadVarInt(out ulong value)
    {
        if (VarInt.TryRead(_buffer[_position..], out value, out int bytesRead))
        {
            _position += bytesRead;
            return true;
        }

        return false;
    }

    /// <summary>Reads one variable-length integer, throwing on a truncated buffer.</summary>
    public ulong ReadVarInt() =>
        TryReadVarInt(out ulong value) ? value : throw new MoqProtocolException("Truncated variable-length integer.");

    /// <summary>Reads a variable-length integer and returns it as a bounded <see cref="int"/>.</summary>
    public int ReadVarIntAsInt32()
    {
        ulong value = ReadVarInt();
        if (value > int.MaxValue)
        {
            throw new MoqProtocolException($"Value {value} does not fit in a 32-bit integer.");
        }

        return (int)value;
    }

    /// <summary>
    /// Reads one raw byte, for the spec's fixed 8-bit fields (e.g. FETCH_OK's End Of Track) —
    /// which are not varints.
    /// </summary>
    public byte ReadByte()
    {
        if (_position >= _buffer.Length)
        {
            throw new MoqProtocolException("Truncated buffer where a byte was expected.");
        }

        return _buffer[_position++];
    }

    /// <summary>Reads a varint length followed by that many bytes, returned as a borrowed slice.</summary>
    public ReadOnlySpan<byte> ReadBytes()
    {
        ulong length = ReadVarInt();
        if (length > (ulong)Remaining)
        {
            throw new MoqProtocolException("Length-prefixed byte string runs past the end of the buffer.");
        }

        ReadOnlySpan<byte> slice = _buffer.Slice(_position, (int)length);
        _position += (int)length;
        return slice;
    }

    /// <summary>Reads a varint-length-prefixed UTF-8 string.</summary>
    public string ReadString() => Encoding.UTF8.GetString(ReadBytes());

    /// <summary>Consumes and returns the remaining bytes (e.g. a message's trailing payload).</summary>
    public ReadOnlySpan<byte> ReadToEnd()
    {
        ReadOnlySpan<byte> rest = _buffer[_position..];
        _position = _buffer.Length;
        return rest;
    }
}
