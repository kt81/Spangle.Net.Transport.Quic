using Spangle.Net.Moqt.Wire;

namespace Spangle.Net.Moqt.Messages;

/// <summary>
/// The REQUEST_ERROR reply (draft-18 §10.6, type 0x5): the generic failure answer to any request,
/// carrying why it failed, how long to wait before retrying, and optionally where to retry. Like
/// <see cref="RequestOkMessage"/> it rides the request's own bidirectional stream, so the Request
/// ID is implicit.
/// </summary>
public sealed class RequestErrorMessage
{
    /// <summary>Creates a REQUEST_ERROR.</summary>
    public RequestErrorMessage(ulong errorCode, ulong retryInterval, string errorReason,
        MoqRedirect? redirect = null)
    {
        ArgumentNullException.ThrowIfNull(errorReason);
        ErrorCode = errorCode;
        RetryInterval = retryInterval;
        ErrorReason = errorReason;
        Redirect = redirect;
    }

    /// <summary>Why the request failed.</summary>
    public ulong ErrorCode { get; }

    /// <summary>How long the requester should wait before retrying.</summary>
    public ulong RetryInterval { get; }

    /// <summary>A human-readable reason phrase.</summary>
    public string ErrorReason { get; }

    /// <summary>Where to retry, when the error directs the requester elsewhere.</summary>
    public MoqRedirect? Redirect { get; }

    /// <summary>Writes the REQUEST_ERROR payload.</summary>
    public void EncodePayload(MoqWriter writer)
    {
        ArgumentNullException.ThrowIfNull(writer);
        writer.WriteVarInt(ErrorCode);
        writer.WriteVarInt(RetryInterval);
        writer.WriteString(ErrorReason);
        Redirect?.WriteTo(writer);
    }

    /// <summary>
    /// Parses a REQUEST_ERROR payload. The Redirect is optional and trailing, so it is present
    /// exactly when bytes remain after the reason phrase.
    /// </summary>
    public static RequestErrorMessage DecodePayload(ReadOnlySpan<byte> payload)
    {
        var reader = new MoqReader(payload);
        ulong errorCode = reader.ReadVarInt();
        ulong retryInterval = reader.ReadVarInt();
        string reason = reader.ReadString();
        MoqRedirect? redirect = reader.End ? null : MoqRedirect.Read(ref reader);
        return new RequestErrorMessage(errorCode, retryInterval, reason, redirect);
    }
}
