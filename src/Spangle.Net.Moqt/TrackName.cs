using System.Text;
using Spangle.Net.Moqt.Wire;

namespace Spangle.Net.Moqt;

/// <summary>
/// A Track Namespace (draft-18 §1.5): an ordered set of 0–32 opaque byte fields. Media apps
/// use UTF-8 strings, so string helpers are offered, but the wire form is bytes.
/// </summary>
public sealed class TrackNamespace
{
    /// <summary>The maximum number of fields a Track Namespace may hold.</summary>
    public const int MaxFields = 32;

    private readonly byte[][] _fields;

    /// <summary>Creates a namespace from its byte fields.</summary>
    public TrackNamespace(IReadOnlyList<byte[]> fields)
    {
        ArgumentNullException.ThrowIfNull(fields);
        if (fields.Count > MaxFields)
        {
            throw new ArgumentException($"A Track Namespace may hold at most {MaxFields} fields.", nameof(fields));
        }

        _fields = [.. fields];
    }

    /// <summary>The namespace fields, in order.</summary>
    public IReadOnlyList<byte[]> Fields => _fields;

    /// <summary>Builds a namespace from UTF-8 string fields.</summary>
    public static TrackNamespace FromStrings(params string[] fields)
    {
        ArgumentNullException.ThrowIfNull(fields);
        return new TrackNamespace([.. fields.Select(Encoding.UTF8.GetBytes)]);
    }

    /// <summary>The fields decoded as UTF-8 strings.</summary>
    public IReadOnlyList<string> ToStrings() => [.. _fields.Select(f => Encoding.UTF8.GetString(f))];

    internal void WriteTo(MoqWriter writer)
    {
        writer.WriteVarInt((ulong)_fields.Length);
        foreach (byte[] field in _fields)
        {
            writer.WriteBytes(field);
        }
    }

    internal static TrackNamespace Read(ref MoqReader reader)
    {
        int count = reader.ReadVarIntAsInt32();
        if (count > MaxFields)
        {
            throw new MoqProtocolException($"A Track Namespace may hold at most {MaxFields} fields, got {count}.");
        }

        var fields = new byte[count][];
        for (int i = 0; i < count; i++)
        {
            fields[i] = reader.ReadBytes().ToArray();
        }

        return new TrackNamespace(fields);
    }
}

/// <summary>
/// A Full Track Name (draft-18 §1.5): a <see cref="TrackNamespace"/> plus a Track Name. This
/// identifies the track a subscriber asks for and a publisher offers.
/// </summary>
public sealed class FullTrackName
{
    /// <summary>Creates a full track name from a namespace and a byte name.</summary>
    public FullTrackName(TrackNamespace @namespace, ReadOnlyMemory<byte> name)
    {
        ArgumentNullException.ThrowIfNull(@namespace);
        Namespace = @namespace;
        Name = name;
    }

    /// <summary>The track's namespace.</summary>
    public TrackNamespace Namespace { get; }

    /// <summary>The track name (opaque bytes).</summary>
    public ReadOnlyMemory<byte> Name { get; }

    /// <summary>The track name decoded as a UTF-8 string.</summary>
    public string NameAsString => Encoding.UTF8.GetString(Name.Span);

    /// <summary>Builds a full track name from UTF-8 strings.</summary>
    public static FullTrackName FromStrings(IReadOnlyList<string> namespaceFields, string name)
    {
        ArgumentNullException.ThrowIfNull(namespaceFields);
        ArgumentNullException.ThrowIfNull(name);
        return new FullTrackName(new TrackNamespace([.. namespaceFields.Select(Encoding.UTF8.GetBytes)]),
            Encoding.UTF8.GetBytes(name));
    }

    internal void WriteTo(MoqWriter writer)
    {
        Namespace.WriteTo(writer);
        writer.WriteBytes(Name.Span);
    }

    internal static FullTrackName Read(ref MoqReader reader)
    {
        TrackNamespace @namespace = TrackNamespace.Read(ref reader);
        byte[] name = reader.ReadBytes().ToArray();
        return new FullTrackName(@namespace, name);
    }
}
