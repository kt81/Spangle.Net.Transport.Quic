using System.Diagnostics.CodeAnalysis;
using Spangle.Net.Moqt.Wire;

namespace Spangle.Net.Moqt.Messages;

/// <summary>
/// The GOAWAY message (draft-18 §10.4, type 0x10): an endpoint warns the peer it intends to close
/// the session soon, optionally naming a URI to migrate to. Besides SETUP it is the only message
/// allowed on the control stream; sent instead on a request stream it migrates just that request,
/// and only then does it carry a Request ID.
/// <para>
/// A client MUST send a zero-length New Session URI — only a server can direct a migration.
/// </para>
/// </summary>
[SuppressMessage("Design", "CA1054:URI-like parameters should not be strings",
    Justification = "The wire field is a length-prefixed string and is empty in every client-sent " +
                    "GOAWAY; System.Uri cannot represent that, nor round-trip a malformed peer value.")]
[SuppressMessage("Design", "CA1056:URI-like properties should not be strings",
    Justification = "The wire field is a length-prefixed string and is empty in every client-sent " +
                    "GOAWAY; System.Uri cannot represent that, nor round-trip a malformed peer value.")]
public sealed class GoAwayMessage
{
    /// <summary>Creates a GOAWAY.</summary>
    public GoAwayMessage(string newSessionUri = "", ulong timeout = 0, ulong? requestId = null)
    {
        ArgumentNullException.ThrowIfNull(newSessionUri);
        NewSessionUri = newSessionUri;
        Timeout = timeout;
        RequestId = requestId;
    }

    /// <summary>The session to migrate to; empty when none is offered (always empty from a client).</summary>
    public string NewSessionUri { get; }

    /// <summary>How long the peer has before the session closes.</summary>
    public ulong Timeout { get; }

    /// <summary>The request being migrated — present only on a request stream.</summary>
    public ulong? RequestId { get; }

    /// <summary>Writes the GOAWAY payload.</summary>
    public void EncodePayload(MoqWriter writer)
    {
        ArgumentNullException.ThrowIfNull(writer);
        writer.WriteString(NewSessionUri);
        writer.WriteVarInt(Timeout);
        if (RequestId is { } requestId)
        {
            writer.WriteVarInt(requestId);
        }
    }

    /// <summary>
    /// Parses a GOAWAY payload. The Request ID is optional and trailing, so it is present exactly
    /// when bytes remain after the timeout.
    /// </summary>
    public static GoAwayMessage DecodePayload(ReadOnlySpan<byte> payload)
    {
        var reader = new MoqReader(payload);
        string uri = reader.ReadString();
        ulong timeout = reader.ReadVarInt();
        ulong? requestId = reader.End ? null : reader.ReadVarInt();
        return new GoAwayMessage(uri, timeout, requestId);
    }
}
