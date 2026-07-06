# NetSync — Architecture & Maintainer Guide

This is the "read this first" document: what the project is, how every layer works,
where to find things, and how to release. The [README](README.md) is the user-facing
front page; this file is for anyone who needs to change or understand the code.

Everything below reflects the 2.0 rewrite completed 2026-07-06 (all 8 stages of
[PLAN.md](PLAN.md)).

---

## 1. What this is

NetSync 1.x was a Unity-embedded networking library. 2.0 split it into:

| Repository | What it is |
|---|---|
| **NetSync** (this repo) | The core: one zero-dependency DLL (`netstandard2.1` + `net8.0`), all networking logic, tests, benchmarks, console sample |
| **NetSync.Unity** (sibling repo) | Thin UPM adapter: precompiled core DLL + MonoBehaviour facade + Unity math serialization + importable samples |

Hard rules the core obeys:

1. **Zero NuGet dependencies** — BCL only. Nothing to conflict with in any host app.
2. **No engine references** — no `UnityEngine`, no main-thread assumptions.
3. **Both targets speak one wire protocol** — a Unity (netstandard2.1) client must
   interoperate with a net8.0 server byte-for-byte. This constraint decided the
   cipher choice (see §6).
4. **Off = free** — optional features (reliability, compression, encryption) cost
   nothing when not enabled on a channel.

## 2. Repository map

```
src/NetSync/
├── NetConfig.cs                  NetConfig, ChannelConfig, EncryptionConfig, enums
├── Peers/
│   ├── NetClient.cs              client peer: sessions, handshake, channel routing
│   ├── NetServer.cs              server peer: connections, handshake, routing, kick
│   ├── NetConnection.cs          a connected client as the server sees it
│   ├── PeerProtocol.cs           peer message layouts (Hello/Welcome/Data/…)
│   ├── ChannelTransform.cs       per-channel compress→encrypt pipeline
│   └── EventDispatcher.cs        Polled queue / Immediate dispatch
├── Transports/
│   ├── ITransport.cs             client transport contract
│   ├── IServerTransport.cs       server transport contract
│   ├── TransportPeer.cs          remote peer handle (endpoint, ping, internal state)
│   ├── PacketProtocol.cs         transport framing, pooled frame builders
│   ├── DisconnectReason.cs       Local / Remote / Timeout / Error
│   ├── Tcp/                      TcpTransport, TcpServerTransport
│   └── Udp/                      UdpTransport, UdpServerTransport
├── Pipeline/
│   ├── Fragmentation/            PacketFragmenter (split), FragmentBuffer (reassemble)
│   ├── Reliability/              ReliabilityMode, ReliableEndpoint (the ARQ engine)
│   ├── Compression/              Compressor (Deflate + bomb cap)
│   └── Security/                 ConnectionCrypto (AES-CBC+HMAC), EcdhKeyExchange
├── Sending/                      SendQueue (priority, pooled buffers), SendPriority
├── Discovery/                    LanServerBroadcaster, LanServerListener, DiscoveryMessage
├── Serialization/                NetWriter, NetReader
├── Diagnostics/                  INetLogger (+Null/Console), NetMetrics (Interlocked)
└── Internal/                     NetTime (monotonic clock)

tests/NetSync.Tests/              77 tests — see §10 for the map
benchmarks/NetSync.Benchmarks/    package-free micro + loopback throughput harness
samples/Echo/                     console echo client/server on the peer API
.github/workflows/                ci.yml (build+test+pack), release.yml (tag → NuGet)
```

## 3. The big picture: one send, top to bottom

`client.SendAsync(channel, data)`:

1. **NetClient** looks up the channel's `ChannelConfig`.
2. **ChannelTransform.Encode** (only if the channel opted in): Deflate-compress
   (skipped for <128 B or incompressible payloads), then encrypt; prepends one flags
   byte. Channels without these features skip this step entirely.
3. Routing:
   - TCP channel, or UDP channel with `Reliability == Unreliable` → straight to the
     transport with a cached 2-byte `[MsgData][channel]` prefix (scatter-gather, no
     concatenation).
   - UDP channel with reliability → **ReliableEndpoint** (§5): sequence number,
     window slot, retransmit bookkeeping, fragmentation above 1100 B.
4. **Transport** builds the wire frame in an `ArrayPool` buffer
   (`PacketProtocol.Rent*`) and hands it to the **SendQueue** (TCP) or the socket
   directly (UDP). Queue priorities: keepalive `Critical` > `Normal` data > `Low`
   bulk fragments, so a file transfer never starves real-time packets.
5. Buffer returns to the pool after the socket write. Steady-state framing allocates
   **zero bytes**.

Receive is the mirror: transport reads into pooled buffers → `PooledReceive`
internal hook (zero-copy handoff) → peer layer parses the message type → reliability
engine (dedup/order/ack) if applicable → `ChannelTransform.Decode` (verify MAC,
decrypt, decompress — failures **drop the packet with a warning**, never deliver
garbage) → exactly one exact-size `byte[]` is allocated for the user → event.

## 4. Connections and the handshake

- One **logical connection** spans one link per configured transport (typically
  TCP + UDP on the same port).
- The client generates a random 64-bit **session token** and sends `Hello` on every
  link. The server groups links by token into one `NetConnection` and replies
  `Welcome` (with the connection id). UDP `Hello`s are re-sent every 200 ms until
  acknowledged — datagram loss cannot break connecting.
- The connection is announced (`ConnectionOpened` / `Connected`) only when **all**
  configured transports completed the handshake. Client and server must therefore
  use identical channel tables. A maintenance loop evicts half-connected peers.
- Disconnect: explicit `Bye` (instant, matters for UDP-only), TCP FIN, ping timeout,
  or reliability giving up. All paths converge on a single close routine guarded by
  an interlocked flag → each side gets exactly one event with a `DisconnectReason`.
- Keepalive: every link pings (`[type][timestamp:8]`) once per `PingIntervalMs`;
  pongs echo the timestamp; RTT comes out of the monotonic `NetTime` clock
  (never `DateTime.UtcNow` — not monotonic).

## 5. Reliable UDP (`Pipeline/Reliability/ReliableEndpoint.cs`)

ENet-style ARQ, one instance per UDP link, transport-agnostic (takes a send
delegate). Per channel:

- 16-bit wrap-aware sequence numbers (`SeqDistance` = `(short)(a - b)`).
- Modes: `UnreliableSequenced` (drop stale, no acks), `Reliable` (exactly-once,
  any order, dedup ring), `ReliableOrdered` (plus reorder buffer + in-order drain).
- ACKs are sent immediately for every reliable packet: `[channel][anchorSeq][mask:4]`
  where mask bit *i* = `anchorSeq-1-i` received. Duplicates trigger a re-ACK anchored
  at the duplicate, so retransmit storms die fast.
- Retransmission: a 20 ms scan loop; RTO = `clamp(RTT*2+30, 50, 3000) << attempts`;
  after 12 unacked attempts the link is declared dead → connection closes (`Timeout`).
- **The invariant that matters** (was the hardest bug of the rewrite): the send
  window is a **range** gate — `NextSeq` may never run more than 256 ahead of the
  *oldest unacked* seq. A count-based window lets the sender outrun the receiver's
  256-slot dedup ring under loss, and packets get acked-but-never-stored (lost
  forever) or dropped-as-ancient. `ReliableEndpointTests` simulate 25% loss + 10%
  duplication to pin this down.
- Fragmentation: payloads >1100 B are split so each fragment fits one datagram;
  reassembly reuses `FragmentBuffer` with a 10-minute timeout (fragments under
  reliability can be delayed, not lost).

## 6. Security (`Pipeline/Security/`)

- Cipher: **AES-256-CBC + HMAC-SHA256, encrypt-then-MAC**, layout
  `[iv:16][mac:32][ciphertext]`, MAC over iv‖ciphertext, constant-time compare.
  *Why not AES-GCM*: `AesGcm` does not exist on netstandard2.1, and rule #3 (one
  wire protocol for Unity ↔ .NET 8) forbids per-target ciphers. Deliberate deviation
  from the original plan; GCM could later be negotiated via the Hello cipher byte.
- Key exchange (`NetConfig.Encryption.Mode`):
  - `PresharedKey` — session keys = HKDF-style HMAC chain over (psk, session token).
  - `Ecdh` — ephemeral P-256, raw 64-byte X‖Y public keys ride in `Hello`/`Welcome`.
- Honest threat model (also in user docs): ECDH is **unauthenticated** — safe
  against eavesdropping, not an active MITM; the handshake itself (tokens, ids) is
  plaintext. Wrong keys/tampering → MAC failure → packet silently dropped + warning.
- Downgrade protection: an encrypted channel never accepts a packet whose flags say
  "plaintext".

## 7. Threading model

- Each transport link runs its own receive loop; TCP additionally a send-queue drain
  loop and a ping loop; `ReliableEndpoint` adds one retransmit loop per UDP link;
  `NetServer` has one maintenance loop. All are thread-pool tasks.
- Events: `EventDelivery.Polled` (default) queues events until `PollEvents()` —
  Unity calls it in `Update`, so handlers run on the main thread. `Immediate`
  invokes straight from network threads (documented as needing thread-safe
  handlers); the hot data path avoids closure allocations in this mode.
- `SendAsync` is thread-safe from any thread in both modes.
- Reliability engine locking: sender state under a per-channel lock (touched by user
  threads + retransmit loop); receiver state is only ever touched by the link's
  single receive thread — no lock.

## 8. Wire protocol reference

Transport layer (TCP adds a length because it's a stream):

| Packet | Layout |
|---|---|
| UDP datagram | `[type:1][payload]` |
| TCP frame | `[type:1][bodyLen:4 BE][body]` |
| Ping / Pong | `[0x01 / 0x02][timestampMs:8 BE]` |
| Data | type `0x10`, body = peer message |
| Fragment (TCP bulk) | body = `[0x11][fragId:4][index:4][total:4][chunk]` |

Peer layer (inside transport Data):

| Message | Layout |
|---|---|
| Hello `0x01` | `[ver:1][token:8][cipher:1][pubLen:1][pubKey…]` (tail optional) |
| Welcome `0x02` | `[connectionId:8][pubLen:1][pubKey…]` |
| Reject `0x03` | `[reason:1]` (1 = version, 2 = encryption mismatch) |
| Bye `0x04` | `[reason:1]` |
| Data `0x10` | `[channel:1][payload]` |
| RelData `0x20` | `[flags:1][channel:1][seq:2 BE][payload]` (flags bit0 = fragment) |
| RelAck `0x21` | `[channel:1][anchorSeq:2 BE][mask:4 BE]` |

Channel payload when compression/encryption is on: `[flags:1][body]`
(bit0 compressed, bit1 encrypted); compressed body = `[origLen:4 BE][deflate]`.

Bump `PeerProtocol.ProtocolVersion` on any breaking change here — old peers get a
clean `Reject` instead of undefined behavior.

## 9. Error-handling philosophy

- **Caller mistakes throw**: unknown channel, oversized unreliable payload, missing
  key config, send while disconnected.
- **Network garbage never throws**: malformed frames, bad MACs, bomb-sized
  decompression claims, unknown channels from the wire → drop + `Warning`/`Debug`
  log. A hostile peer must not be able to take the process down.
- Handler exceptions are caught and logged (`EventDispatcher` / direct-invoke
  try/catch) — user bugs don't kill network threads.

## 10. Tests (`tests/NetSync.Tests`, 77 total)

| File | Covers |
|---|---|
| `FragmentationTests` | split/reassemble roundtrips, out-of-order, dups, corrupt headers, buffer cap |
| `SendQueueTests` | priority order, overflow rejection, failure resilience, pool returns |
| `TcpLoopbackTests` / `UdpLoopbackTests` | transport-level echo, large fragmented echo, reconnect, single disconnect event, ping, silent-peer timeout |
| `PeerTests` | TCP+UDP as one connection, channel routing, broadcast, kick, Polled mode, UDP-only handshake, reuse after disconnect |
| `ReliableEndpointTests` | in-memory lossy pipe (25% loss / 10% dup): ordered, exactly-once, fragmented-under-loss, sequenced stale drop, dead-link timeout |
| `PeerReliabilityTests` | end-to-end: 300 KB over reliable UDP; **1 MB file through a 5%-loss UDP proxy arrives intact** (the plan's acceptance test) |
| `SecurityTests` | crypto roundtrip/tamper/wrong-key, ECDH agreement, compressor bombs, downgrade rejection, e2e PSK + ECDH, handshake mismatch, wrong-PSK silent drop |
| `SerializationTests` / `DiscoveryTests` | writer/reader edge cases, var-int sizes, truncation safety; discovery matching/filtering |

Run: `dotnet test` · CI runs Windows + Ubuntu on every push.

## 11. Benchmarks (`benchmarks/NetSync.Benchmarks`)

Package-free by necessity and philosophy (Stopwatch + GC counters).
`dotnet run -c Release -- micro` (framing allocs/latency) and `-- throughput`
(end-to-end loopback: msg/s, MB/s, RTT percentiles, B allocated per message).
Current numbers are in the README's Performance table; re-run after touching
transports, SendQueue, framing or the reliability engine.

## 12. Releasing

**NuGet** (core):
1. Bump `<Version>` in `src/NetSync/NetSync.csproj` (SemVer; wire-breaking changes
   also bump `PeerProtocol.ProtocolVersion`).
2. `dotnet pack src/NetSync -c Release` → `NetSync.<version>.nupkg` + `.snupkg`
   (README embedded, MIT license expression, deterministic build).
3. Tag `v<version>` and push — `.github/workflows/release.yml` builds, tests, packs
   and pushes to nuget.org using the `NUGET_API_KEY` repository secret.
   Before the first publish: set the real `RepositoryUrl`/`PackageProjectUrl` in the
   csproj and verify the `NetSync` package id is free on nuget.org.

**Unity** (sibling repo):
1. `powershell -File tools~/update-core.ps1` — rebuilds the core and refreshes
   `Runtime/Plugins/NetSync.dll` + XML docs.
2. Bump `version` in `package.json` (keep in sync with the NuGet version).
3. Commit + tag. Users install via Package Manager → git URL; samples appear under
   the package's **Samples** section (declared in `package.json`, stored in
   `Samples~/`, imported on demand into the user's Assets).

## 13. Decision log

| Date | Decision |
|---|---|
| 2026-07-05 | Bluetooth dropped from requirements entirely |
| 2026-07-05 | Targets: `netstandard2.1` + `net8.0`; free API redesign; docs in English |
| 2026-07-05 | Reliability window must be range-based, not count-based (loss-test bug) |
| 2026-07-05 | Single cipher AES-CBC+HMAC on both targets (no `AesGcm` on ns2.1; interop first) |
| 2026-07-06 | Benchmarks package-free (also: no NuGet access in the dev environment) |
| 2026-07-06 | License MIT; NuGet id `NetSync`; versions synced across core and Unity package |

## 14. Known limitations / future work

- NAT traversal / hole punching / relay — LAN and direct-IP only today.
- No authenticated key exchange (certificates/signatures) — ECDH is MITM-able.
- `UdpClient.ReceiveAsync` allocates per datagram; a raw-`Socket` receive path would
  cut the remaining UDP receive allocations.
- AES-GCM negotiation between capable peers (Hello already carries a cipher byte).
- High-level object replication / RPC — intentionally out of the core; would be a
  separate module on top.
- Server-side loops are per-connection; thousands of connections would want a shared
  timer wheel for retransmit/ping scans.
