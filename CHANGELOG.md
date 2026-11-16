# Changelog

Versions follow [Semantic Versioning](https://semver.org). The wire protocol has its own version, noted for each release: peers must speak the same one.

## 2.0.0-rc.5 — unreleased

**Wire protocol v4. Not compatible with 2.0.0-rc.1 – rc.4: update clients and servers together.** Protocol v4 is the stable contract from here on — later releases extend it through negotiated features instead of replacing it.

### Changed
- Every keyed connection (`PresharedKey` and `Ecdh`) now runs an ephemeral X25519 key exchange implemented by the library itself:
  - all modes, including `Ecdh` and server identities, work on every runtime — Unity Mono and IL2CPP included;
  - session keys are fresh for every session, so a recorded session cannot be sent again, and past sessions stay private if a key leaks later (this also applies to `PresharedKey` now).
- `NetIdentity` is an X25519 key pair: public and private keys are 32 bytes. **Identities created by earlier versions must be regenerated** and re-pinned on clients. `NetIdentity.IsSupported` is always `true`.
- A server proves its identity through the key exchange instead of a signature. A server that presents a pinned key without holding it is refused with `ConnectionRejection.ServerIdentity`.
- The relay protocol carries a version; a relay refuses peers of another version with a reason.

### Added
- `NetConfig.AppId` and `NetConfig.AppProtocolVersion`: a server refuses clients of another application or application protocol version (`ConnectionRejection.ApplicationMismatch`).
- `NetRelayServer.AccessKey` / `RelayRoom.AccessKey`: a relay can restrict who may open rooms.
- Extension points in the handshake (feature bits, extension block): unknown ones are ignored, so future releases stay compatible with v4.
- Reference vectors of protocol v4 (`ProtocolV4VectorsTests`, `tools/protocol-v4-vectors.py`).

### Unchanged
- `KeyExchangeMode`, `EncryptionConfig`, channels, events and the rest of the public API: existing code compiles as is.

## 2.0.0-rc.4 — 2026-11-10

Wire protocol v3.

- A kick or shutdown notice reaches the client before the connection closes (TCP, WebSocket, relay).
- A UDP server no longer shares its port with an unrelated IPv4 socket on Windows.
- Hole punching through a relay retries until a direct path exists.
- Packages are published by the release workflow: NuGet `NetSync` and the Unity package `com.netsync.core`.

## 2.0.0-rc.3 — 2026-11-01

Wire protocol v3. First public release candidate.
