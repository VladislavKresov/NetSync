# Changelog

Versions follow [Semantic Versioning](https://semver.org). The wire protocol has its own version, noted for each release: peers must speak the same one.

## 2.0.0-rc.6 — unreleased

Wire protocol v4 (compatible with 2.0.0-rc.5; the new behaviour needs both sides on rc.6).

### Changed
- Losing UDP mid-session no longer disconnects: the UDP channels move to TCP and the connection stays (`IsDegraded` becomes true on both sides). Reliable messages still in flight are resent over TCP before newer ones — nothing is lost, duplicated or reordered. With `RequireAllTransports = true`, or a peer older than rc.6, losing UDP disconnects as before.

### Fixed
- A client link closed right after it opened could crash the process: its receive thread, starting late, read a disposed cancellation source. Client transports no longer dispose it.
- `ReliableOrdered` channels could deliver messages out of order when the application didn't await each send and the window was full: waiting sends raced for sequence numbers. Messages now take their sequence numbers in send order, all fragments of a message in a row.

## 2.0.0-rc.5 — 2026-11-18

**Wire protocol v4. Not compatible with 2.0.0-rc.1 – rc.4: update clients and servers together.** Protocol v4 is the stable contract from here on — later releases extend it through negotiated features instead of replacing it.

### Changed
- Every keyed connection (`PresharedKey` and `Ecdh`) now runs an ephemeral X25519 key exchange implemented by the library itself:
  - all modes, including `Ecdh` and server identities, work on every runtime — Unity Mono and IL2CPP included;
  - session keys are fresh for every session, so a recorded session cannot be sent again, and past sessions stay private if a key leaks later (this also applies to `PresharedKey` now).
- `NetIdentity` is an X25519 key pair: public and private keys are 32 bytes. **Identities created by earlier versions must be regenerated** and re-pinned on clients. `NetIdentity.IsSupported` is always `true`.
- A server proves its identity through the key exchange instead of a signature. A server that presents a pinned key without holding it is refused with `ConnectionRejection.ServerIdentity`.
- The relay protocol carries a version; a relay refuses peers of another version with a reason.

- A reliable message is never lost silently: with Polled delivery, when the event queue is full (`MaxPendingEvents`), the connection that receives data on a reliable channel is closed with the new `DisconnectReason.Overloaded`. Messages of unreliable channels are dropped as before.
- `SendAsync` on a stream channel waits for room when the send queue is full instead of throwing: await your sends and a slow connection slows the sender down. `BroadcastAsync` hands the message to every connection before waiting for any of them.
- A busy or paused process no longer drops healthy connections:
  - after a retransmission timeout the timeout stays doubled until a fresh acknowledgement arrives (as in TCP), so slow acknowledgements no longer cause storms of needless retransmissions;
  - a reliable channel gives up on a packet only after `PingTimeoutMs` of silence, not after a number of attempts alone;
  - keepalive counts the ping rounds the process actually ran: a local stall (a starved thread pool, a laptop waking up) is no longer blamed on the peer.
- UDP sockets no longer use a receive timeout to stop (it could lose datagrams on Linux and macOS); closing wakes the receive loop instead.
- Path MTU discovery is skipped where the OS can't forbid fragmentation (macOS): fragments keep the default size there.
- `NetworkSimulation` delivers each direction of each link on its own thread, without spinning: a slow handler delays only its own link, and the simulator uses no CPU while waiting. On Windows it raises the timer resolution to 1 ms while it runs.

### Added
- `NetConfig.AppId` and `NetConfig.AppProtocolVersion`: a server refuses clients of another application or application protocol version (`ConnectionRejection.ApplicationMismatch`).
- `NetRelayServer.AccessKey` / `RelayRoom.AccessKey`: a relay can restrict who may open rooms.
- `NetConfig.CloseFlushTimeoutMs` and `NetRelayServer.CloseFlushTimeoutMs`: how long a kick or shutdown waits for queued data to leave (1 s by default, as before).
- Tests run on .NET 8 and .NET 10; CI covers Windows, Linux and macOS.
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
