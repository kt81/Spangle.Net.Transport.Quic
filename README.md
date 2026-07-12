Spangle.Net.Transport.Quic
==========================

[![Build and Test](https://github.com/kt81/Spangle.Net.Transport.Quic/actions/workflows/build_test.yml/badge.svg)](https://github.com/kt81/Spangle.Net.Transport.Quic/actions/workflows/build_test.yml)

The QUIC transport seam for [Spangle](https://github.com/kt81/spangle) — the layer that
keeps Spangle's QUIC-based protocols (Media over QUIC first) off `System.Net.Quic`
directly, so the protocol code has one interface to target and two interchangeable
backends beneath it.

> Pre-1.0 / under active development. Interfaces may change without notice.

Why this exists
---------------

QUIC in .NET means native **msquic**, and `System.Net.Quic` additionally needs the
dual-mode sockets an IPv6 stack provides. On a host without those, `System.Net.Quic`
reports `IsSupported = false` and cannot run at all. Putting the protocol code behind a
small interface means it can be built and tested where msquic (or IPv6) is absent, and
leaves room to reach msquic features `System.Net.Quic` does not surface (QUIC datagrams,
stream priority) via a direct-msquic backend later — all without touching the protocol.

The seam
--------

`IQuicTransport` — listen / connect. `IQuicConnection` — open / accept streams, close.
`IQuicStream` — a byte channel; unidirectional (write-only for the opener, read-only for
the acceptor) or bidirectional, with graceful `CompleteWrites` and abrupt `Abort`.

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

The in-memory backend runs anywhere, so the abstraction and any protocol built on it are
covered on every platform with no native dependency. The real `MsQuicTransport` can only be
exercised where QUIC actually runs, so its loopback test runs when `IsSupported` is true and
skips otherwise. CI closes the gap: the Windows job sets `SPANGLE_REQUIRE_QUIC=1`, which turns
a skip into a failure — guaranteeing the real msquic backend is exercised on every push. The
Linux job installs `libmsquic` and runs it best-effort; the macOS (arm64) job covers the
managed logic and the build.

License
-------

MIT (see [LICENSE](./LICENSE)). Spangle itself is AGPL-3.0; its reusable building blocks
are published separately under MIT so embedding them carries no copyleft.
