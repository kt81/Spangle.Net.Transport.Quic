namespace Spangle.Net.Moqt;

/// <summary>
/// The wire constants of MOQT, pinned to draft-ietf-moq-transport-18. Every value is
/// isolated here (with its draft section) so revising to a later draft is a one-file diff;
/// nothing else in the codec hard-codes a number. Message type codes are the varint
/// <em>values</em> from Table 5 — they are encoded/decoded with <see cref="Wire.VarInt"/>.
/// </summary>
public static class MoqtConstants
{
    /// <summary>The draft this implementation targets.</summary>
    public const int DraftVersion = 18;

    /// <summary>The QUIC ALPN token; version is negotiated by ALPN in draft ≥ 15 (§3.4).</summary>
    public const string Alpn = "moqt-18";
}

/// <summary>
/// Control-message type codes from draft-18 Table 5 (§10). These are varint values; the
/// SETUP code 0x2F00 encodes to the two bytes 0x6F 0x00 on the wire.
/// </summary>
public static class MoqControlMessageType
{
    /// <summary>SETUP (§10.3) — first message on each control stream.</summary>
    public const ulong Setup = 0x2F00;

    /// <summary>GOAWAY (§10.4).</summary>
    public const ulong GoAway = 0x10;

    /// <summary>SUBSCRIBE (§10.7) — first message on a request stream.</summary>
    public const ulong Subscribe = 0x3;

    /// <summary>SUBSCRIBE_OK (§10.8).</summary>
    public const ulong SubscribeOk = 0x4;

    /// <summary>PUBLISH (§10.10) — first message on a request stream.</summary>
    public const ulong Publish = 0x1D;

    /// <summary>PUBLISH_OK (§10.5).</summary>
    public const ulong PublishOk = 0x1E;

    /// <summary>PUBLISH_DONE (§10.11).</summary>
    public const ulong PublishDone = 0xB;

    /// <summary>FETCH (§10.12) — first message on a request stream.</summary>
    public const ulong Fetch = 0x16;

    /// <summary>FETCH_OK (§10.13).</summary>
    public const ulong FetchOk = 0x18;

    /// <summary>TRACK_STATUS (§10.14) — first message on a request stream.</summary>
    public const ulong TrackStatus = 0xD;

    /// <summary>PUBLISH_NAMESPACE (§10.15) — first message on a request stream.</summary>
    public const ulong PublishNamespace = 0x6;

    /// <summary>NAMESPACE (§10.16).</summary>
    public const ulong Namespace = 0x8;

    /// <summary>NAMESPACE_DONE (§10.17).</summary>
    public const ulong NamespaceDone = 0xE;

    /// <summary>PUBLISH_BLOCKED (§10.20).</summary>
    public const ulong PublishBlocked = 0xF;

    /// <summary>SUBSCRIBE_NAMESPACE (§10.18) — first message on a request stream.</summary>
    public const ulong SubscribeNamespace = 0x50;

    /// <summary>SUBSCRIBE_TRACKS (§10.19) — first message on a request stream.</summary>
    public const ulong SubscribeTracks = 0x51;
}

/// <summary>
/// Setup Option type codes (draft-18 §10.3.1). Setup Options are Key-Value-Pairs, so the
/// even/odd rule applies: an even type carries a single varint, an odd type carries bytes.
/// </summary>
public static class MoqSetupOption
{
    /// <summary>PATH (§10.3.1.2) — the WebTransport-style path; odd, so length-prefixed bytes.</summary>
    public const ulong Path = 0x01;

    /// <summary>AUTHORIZATION_TOKEN (§10.3.1.5); odd.</summary>
    public const ulong AuthorizationToken = 0x03;

    /// <summary>MAX_AUTH_TOKEN_CACHE_SIZE (§10.3.1.4); even varint.</summary>
    public const ulong MaxAuthTokenCacheSize = 0x04;

    /// <summary>AUTHORITY (§10.3.1.1); odd, length-prefixed bytes.</summary>
    public const ulong Authority = 0x05;

    /// <summary>MAX_FILTER_RANGES (§10.3.1.7); even varint.</summary>
    public const ulong MaxFilterRanges = 0x06;

    /// <summary>MOQT_IMPLEMENTATION (§10.3.1.6); odd, length-prefixed bytes.</summary>
    public const ulong MoqtImplementation = 0x07;

    /// <summary>MAX_REQUEST_UPDATES (§10.3.1.8); even varint.</summary>
    public const ulong MaxRequestUpdates = 0x08;
}
