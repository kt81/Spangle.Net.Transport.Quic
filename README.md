Spangle.Net.Moqt
================

[![Build and Test](https://github.com/kt81/Spangle.Net.Moqt/actions/workflows/build_test.yml/badge.svg)](https://github.com/kt81/Spangle.Net.Moqt/actions/workflows/build_test.yml)

A [Media over QUIC Transport](https://datatracker.ietf.org/doc/draft-ietf-moq-transport/)
(MOQT) implementation for .NET, pinned to **draft-18** — the wire codec, control messages,
the subgroup object data plane, and the session handshake. It is the reusable MIT building
block that [Spangle](https://github.com/kt81/spangle) (an AGPL media server) embeds and
drives; the media mapping itself (CMAF/CMSF frames) stays in the Spangle core, which writes
only the bridge onto the types here.

> Pre-1.0 / under active development. Interfaces may change without notice.

What's here
-----------

Two layered assemblies — the protocol on top, the QUIC seam it runs on beneath. They are
published as two packages so a future non-MoQ QUIC protocol can depend on the transport
alone:

| Package | What it is |
|---|---|
| **`Spangle.Net.Moqt`** | The MOQT protocol: the [`Wire`](src/Spangle.Net.Moqt/Wire) codec (QUIC var-ints, length-prefixed strings, Key-Value-Pairs, control-message framing), [`Messages`](src/Spangle.Net.Moqt/Messages) (SETUP, SUBSCRIBE, SUBSCRIBE_OK), the [subgroup](src/Spangle.Net.Moqt/Data) object data plane, and [`MoqSession`](src/Spangle.Net.Moqt/MoqSession.cs) — the control-stream handshake. |
| **`Spangle.Net.Transport.Quic`** | The QUIC seam it sits on: one interface, two interchangeable backends (below). Knows nothing about MoQ, so any QUIC-based protocol can target it. |

Every wire constant is isolated in [`MoqtConstants`](src/Spangle.Net.Moqt/MoqtConstants.cs)
with its draft section, so moving to a later draft is a one-file diff.

The protocol shape
------------------

- **Control plane.** Each endpoint opens its own unidirectional control stream and begins it
  with SETUP (draft-18 §10); `MoqSession.ConnectAsync` / `AcceptAsync` perform that handshake.
  SUBSCRIBE / SUBSCRIBE_OK then run on a bidirectional request stream, the publisher assigning
  the Track Alias the data plane is keyed by.
- **Data plane.** A publisher streams a track's objects on a unidirectional subgroup stream
  (draft-18 §11.4.2): a SUBGROUP_HEADER whose var-int type selects the field layout, then
  objects with delta-encoded Object IDs. A subscriber matches the header's Track Alias and
  reads objects until the stream FINs.

[`PubSubFlowTests`](tests/Spangle.Net.Moqt.Tests/PubSubFlowTests.cs) exercises this end to
end — SUBSCRIBE → SUBSCRIBE_OK → objects on the assigned alias — and reads as a narrative of
the flow.

**Receiving streams without touching the wire.** After the handshake, a peer just accepts
streams; [`MoqStreamRouter`](src/Spangle.Net.Moqt/MoqStreamRouter.cs) classifies each one so
the caller never reads a stream-type varint by hand:

```csharp
switch (await MoqStreamRouter.AcceptAsync(connection, ct))
{
    case MoqRequestStream req when req.MessageType == MoqControlMessageType.Subscribe:
        var subscribe = SubscribeMessage.DecodePayload(req.Payload.Span); // reply on req.Stream
        break;
    case MoqSubgroupStream sub:
        while (await sub.Reader.ReadObjectAsync(ct) is { } obj) { /* obj.Payload ... */ }
        break;
}
```

The QUIC seam
-------------

QUIC in .NET means native **msquic**, and `System.Net.Quic` additionally needs the dual-mode
sockets an IPv6 stack provides. On a host without those, `System.Net.Quic` reports
`IsSupported = false` and cannot run at all. Putting the protocol code behind a small
interface means it can be built and tested where msquic (or IPv6) is absent, and leaves room
to reach msquic features `System.Net.Quic` does not surface (QUIC datagrams, stream priority)
via a direct-msquic backend later — all without touching the protocol.

`IQuicTransport` — listen / connect. `IQuicConnection` — open / accept streams, close.
`IQuicStream` — a byte channel; unidirectional (write-only for the opener, read-only for the
acceptor) or bidirectional, with graceful `CompleteWrites` and abrupt `Abort`.

Two backends implement it:

| Backend | What | Runs where |
|---|---|---|
| `MsQuicTransport` | The real thing: `System.Net.Quic` over native msquic | msquic present **and** IPv6/dual-mode sockets available |
| `InMemoryQuicTransport` | An in-process loopback; connections and streams are wired with `System.IO.Pipelines`, no sockets | everywhere (deterministic, used for tests and single-process runs) |

```csharp
IQuicTransport transport = MsQuicTransport.Shared.IsSupported
    ? MsQuicTransport.Shared
    : new InMemoryQuicTransport();
```

Native msquic
-------------

On **Windows**, msquic ships with the OS / runtime — nothing to install. On **Linux**,
`libmsquic` is a native OS dependency (like libsrt): `apt install libmsquic` from
packages.microsoft.com, or bundle it per-RID with the app for a portable binary. Because
there is only one `System.Net.Quic` in a process, that single `libmsquic` is shared with
Kestrel's HTTP/3 and WebTransport — the transport never runs two msquic binaries.

Testing
-------

The in-memory backend runs anywhere, so the abstraction and the MoQT logic built on it are
covered on every platform with no native dependency. The real `MsQuicTransport` can only be
exercised where QUIC actually runs, so its loopback tests run when `IsSupported` is true and
skip otherwise. CI closes the gap: the Windows job sets `SPANGLE_REQUIRE_QUIC=1`, which turns
a skip into a failure — guaranteeing the real msquic backend is exercised on every push. The
Linux job installs `libmsquic` and runs it best-effort; the macOS (arm64) job covers the
managed logic and the build.

License
-------

MIT (see [LICENSE](./LICENSE)). Spangle itself is AGPL-3.0; its reusable building blocks
are published separately under MIT so embedding them carries no copyleft.
