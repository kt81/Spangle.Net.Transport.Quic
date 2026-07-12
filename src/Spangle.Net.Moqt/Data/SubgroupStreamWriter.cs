using System.Buffers;
using Spangle.Net.Moqt.Wire;
using Spangle.Net.Transport.Quic;

namespace Spangle.Net.Moqt.Data;

/// <summary>
/// Writes a subgroup data stream (draft-18 §11.4.2): the SUBGROUP_HEADER once, then each
/// object as Object ID Delta, optional properties, payload length, and either a status
/// (zero-length) or the payload. The header is emitted lazily with the first object (or on
/// <see cref="CompleteAsync"/>), and <see cref="CompleteAsync"/> FINs the stream to signal
/// the subgroup is complete.
/// </summary>
public sealed class SubgroupStreamWriter
{
    private readonly IQuicStream _stream;
    private readonly SubgroupHeader _header;
    private ulong _previousObjectId;
    private bool _first = true;
    private bool _headerWritten;

    /// <summary>Creates a writer that emits <paramref name="header"/> before the first object.</summary>
    public SubgroupStreamWriter(IQuicStream stream, SubgroupHeader header)
    {
        ArgumentNullException.ThrowIfNull(stream);
        _stream = stream;
        _header = header;
    }

    /// <summary>Appends one object to the subgroup stream.</summary>
    public async ValueTask WriteObjectAsync(MoqObject moqObject, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(moqObject);

        ulong delta;
        if (_first)
        {
            delta = moqObject.ObjectId;
        }
        else if (moqObject.ObjectId <= _previousObjectId)
        {
            throw new ArgumentException("Object IDs must strictly increase within a subgroup.", nameof(moqObject));
        }
        else
        {
            delta = moqObject.ObjectId - _previousObjectId - 1;
        }

        var buffer = new ArrayBufferWriter<byte>();
        if (!_headerWritten)
        {
            _header.WriteTo(buffer);
            _headerWritten = true;
        }

        var writer = new MoqWriter(buffer);
        writer.WriteVarInt(delta);
        if (_header.HasProperties)
        {
            writer.WriteVarInt(0); // Properties Length 0 — this writer emits no object properties
        }

        writer.WriteVarInt((ulong)moqObject.Payload.Length);
        if (moqObject.Payload.IsEmpty)
        {
            writer.WriteVarInt((ulong)moqObject.Status);
        }
        else
        {
            buffer.Write(moqObject.Payload.Span);
        }

        _previousObjectId = moqObject.ObjectId;
        _first = false;
        await _stream.WriteAsync(buffer.WrittenMemory, completeWrites: false, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Flushes the header if no objects were written, then FINs the stream.</summary>
    public async ValueTask CompleteAsync(CancellationToken cancellationToken = default)
    {
        if (!_headerWritten)
        {
            var buffer = new ArrayBufferWriter<byte>();
            _header.WriteTo(buffer);
            _headerWritten = true;
            await _stream.WriteAsync(buffer.WrittenMemory, completeWrites: false, cancellationToken).ConfigureAwait(false);
        }

        _stream.CompleteWrites();
    }
}
