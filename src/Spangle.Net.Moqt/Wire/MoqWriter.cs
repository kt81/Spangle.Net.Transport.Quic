using System.Buffers;
using System.Text;

namespace Spangle.Net.Moqt.Wire;

/// <summary>
/// Writes MOQT wire fields into an <see cref="IBufferWriter{T}"/>: variable-length integers
/// and the length-prefixed byte strings and UTF-8 strings built on them. The mirror of
/// <see cref="MoqReader"/>.
/// </summary>
public sealed class MoqWriter
{
    private readonly IBufferWriter<byte> _output;

    /// <summary>Creates a writer that appends to <paramref name="output"/>.</summary>
    public MoqWriter(IBufferWriter<byte> output)
    {
        ArgumentNullException.ThrowIfNull(output);
        _output = output;
    }

    /// <summary>Writes one variable-length integer in its minimal encoding.</summary>
    public void WriteVarInt(ulong value)
    {
        int length = VarInt.GetLength(value);
        Span<byte> span = _output.GetSpan(length);
        VarInt.Write(span, value);
        _output.Advance(length);
    }

    /// <summary>Writes a varint length followed by the bytes themselves.</summary>
    public void WriteBytes(ReadOnlySpan<byte> bytes)
    {
        WriteVarInt((ulong)bytes.Length);
        _output.Write(bytes);
    }

    /// <summary>Writes a varint-length-prefixed UTF-8 string.</summary>
    public void WriteString(string value)
    {
        ArgumentNullException.ThrowIfNull(value);
        int byteCount = Encoding.UTF8.GetByteCount(value);
        WriteVarInt((ulong)byteCount);
        Span<byte> span = _output.GetSpan(byteCount);
        int written = Encoding.UTF8.GetBytes(value, span);
        _output.Advance(written);
    }
}
