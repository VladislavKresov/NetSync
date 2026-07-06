# NetSync

Fast, lightweight networking core for .NET — a single **zero-dependency** DLL you can drop into anything: game servers, desktop apps, services, or Unity (via the [NetSync.Unity](https://github.com/VladislavKresov/NetSync.Unity) adapter package).

```
┌ Application ─────────────────────────────────────────────────┐
│  NetClient / NetServer          channels · events · config   │
│  Pipeline (per channel, opt-in) compress · encrypt ·         │
│                                 reliability · fragmentation  │
│  Transports                     TCP · UDP · your own         │
└──────────────────────────────────────────────────────────────┘
```

## Features

- **TCP and UDP simultaneously** — one logical connection spans both; each *channel* picks its transport and guarantees.
- **Reliable UDP** — ENet-style ARQ: ACK bitmasks, RTT-based retransmission, four delivery modes (`Unreliable`, `UnreliableSequenced`, `Reliable`, `ReliableOrdered`).
- **Automatic fragmentation** — send a 100 MB payload on a reliable channel; NetSync splits, paces (256-packet window) and reassembles it. Bulk transfers never starve small real-time packets (priority send queue).
- **Optional per-channel compression** (Deflate, auto-skips incompressible data) and **encryption** (AES-256-CBC + HMAC-SHA256 encrypt-then-MAC; pre-shared key or ephemeral ECDH P-256).
- **LAN discovery** — servers announce themselves, clients find them, no address entry.
- **Runs on background threads** — your app never blocks; consume events by polling (game loop) or immediately on network threads (lowest latency).
- **Allocation-free hot paths** — pooled buffers, scatter-gather framing, zero allocations per packet in the framing/queueing layers.
- **Zero dependencies** — BCL only. No NuGet packages, nothing to conflict with.

## Targets

| Target | Where it runs |
|---|---|
| `netstandard2.1` | Unity 2021.2+ (Mono & IL2CPP), Mono, older runtimes |
| `net8.0` | servers, desktop, containers |

Both targets speak the same wire protocol — a Unity client talks to a .NET 8 server.

## Install

**.NET (NuGet):**

```bash
dotnet add package NetSync
```

**From source:**

```bash
git clone https://github.com/VladislavKresov/NetSync
dotnet build NetSync/src/NetSync -c Release   # or: dotnet pack
```

**Unity 2021.2+:** use the [NetSync.Unity](../NetSync.Unity) UPM package (Package Manager → *Add package from git URL…*) — it ships the precompiled DLL, MonoBehaviour integration and importable samples (Chat, Position Sync) under the package's *Samples* section.

## Quick start

```csharp
using NetSync;
using NetSync.Peers;

NetConfig MakeConfig() {
    var config = new NetConfig { EventDelivery = EventDelivery.Immediate };
    config.Channels[0] = new ChannelConfig(TransportType.Tcp);                                     // commands, chat
    config.Channels[1] = new ChannelConfig(TransportType.Udp, ReliabilityMode.UnreliableSequenced); // live state
    config.Channels[2] = new ChannelConfig(TransportType.Udp, ReliabilityMode.ReliableOrdered);     // files
    return config; // client and server must use identical channel tables
}

// Server
using var server = new NetServer(MakeConfig());
server.ConnectionOpened += conn => Console.WriteLine($"+ {conn}");
server.DataReceived += (conn, channel, data) => _ = conn.SendAsync(channel, data); // echo
int port = await server.StartAsync(7777); // 0 = OS picks a port shared by TCP+UDP

// Client
using var client = new NetClient(MakeConfig());
client.DataReceived += (channel, data) => Console.WriteLine($"ch{channel}: {data.Length} B");
await client.ConnectAsync("192.168.0.10", port);  // connects TCP+UDP in parallel, one handshake
await client.SendAsync(0, "hello"u8.ToArray());
```

Runnable demo: [`samples/Echo`](samples/Echo/Program.cs) (`dotnet run -- server 7777`, then `dotnet run -- client 7777 tcp|udp`).

## Channels

A channel is a byte id mapped to a transport plus per-channel options. Everything is opt-in; a plain channel costs nothing extra.

```csharp
config.Channels[3] = new ChannelConfig(TransportType.Udp, ReliabilityMode.ReliableOrdered,
                                       compression: true, encryption: true);
```

| Reliability mode | Guarantees | Typical use |
|---|---|---|
| `Unreliable` | none (raw datagram) | video/audio frames |
| `UnreliableSequenced` | stale packets dropped, no retransmit | positions, state snapshots |
| `Reliable` | exactly-once, any order | events that must arrive |
| `ReliableOrdered` | exactly-once, in order, auto-fragmentation | files, big messages, RPC |

TCP channels are inherently reliable-ordered and ignore the reliability setting. Unreliable UDP channels are limited to one datagram (~65 KB; ~1.1 KB recommended for sequenced); reliable channels have no size limit.

## Encryption

```csharp
config.Encryption = new EncryptionConfig {
    Mode = KeyExchangeMode.PresharedKey,          // or KeyExchangeMode.Ecdh
    PresharedKey = my32ByteKey                    // PSK mode only
};
config.Channels[0] = new ChannelConfig(TransportType.Tcp, encryption: true);
```

- Cipher: AES-256-CBC + HMAC-SHA256 (encrypt-then-MAC) — identical on every target so all peers interoperate.
- `PresharedKey`: per-connection keys derived from the key + session token.
- `Ecdh`: ephemeral P-256 exchange in the handshake — no shared secret needed. **Not authenticated**: protects against eavesdropping, not an active man-in-the-middle.
- Tampered or wrongly-keyed packets fail the MAC and are dropped (never delivered as garbage); encrypted channels refuse plaintext (no downgrade).

## LAN discovery

```csharp
// Server side
using var broadcaster = new LanServerBroadcaster("MyGame", serverPort: port);
broadcaster.Start();

// Client side
using var listener = new LanServerListener("MyGame");
listener.ServerFound += endpoint => _ = client.ConnectAsync(endpoint.Address.ToString(), endpoint.Port);
listener.Start();
```

## Serialization

Bring your own format — channels carry raw bytes. For quick binary messages the built-in writer/reader is allocation-light and safe against malformed input:

```csharp
var writer = new NetWriter();
writer.WriteVarInt(playerId).WriteString("Anna").WriteSingle(hp);
await client.SendAsync(0, writer.ToArray());

var reader = new NetReader(data);          // throws EndOfStreamException on truncated/hostile input
int id = reader.ReadVarInt();
string name = reader.ReadString();
float hp = reader.ReadSingle();
```

## Threading model

All networking runs on background threads. Two delivery modes:

- `EventDelivery.Polled` (default) — events queue up; call `client.PollEvents()` / `server.PollEvents()` wherever you want handlers to run (a game loop, a timer). Your app can never be blocked or raced by the network.
- `EventDelivery.Immediate` — handlers run directly on network threads for minimum latency. They must be fast and thread-safe.

`SendAsync` is safe from any thread in both modes.

## Custom transports

Implement `ITransport`/`IServerTransport` (see `src/NetSync/Transports/`) and the whole pipeline — channels, reliability, compression, encryption — works on top of your transport unchanged.

## Performance

Loopback, Windows, .NET 8, client + server in one process (`benchmarks/NetSync.Benchmarks`, no external packages — `dotnet run -c Release -- micro|throughput`):

| Scenario | Result |
|---|---|
| Frame building (any size) | **0 B allocated** per frame (1.x: 192–32 832 B) |
| TCP small packets | ~74 000 msg/s round-trip |
| UDP small packets | ~82 000 msg/s round-trip, 0 % loss under burst |
| UDP RTT | p50 ≈ 30 µs, p99 ≈ 80 µs |
| Reliable-ordered UDP | ~43 000 msg/s RT; RTT p50 ≈ 42 µs; 1 MB file intact over a 5 % loss link |
| TCP bulk (fragmented) | ~427 MB/s through the full stack (send + echo) |
| Encrypted TCP | ~48 000 msg/s round-trip |

## Documentation

- [ARCHITECTURE.md](ARCHITECTURE.md) — full context for contributors: layer-by-layer internals, wire protocol reference, threading rules, test map, release process, decision log.
- [PLAN.md](PLAN.md) — the 2.0 rewrite plan (completed).

## Repository layout

```
src/NetSync/            the core library (the only thing you ship)
tests/NetSync.Tests/    77 unit + integration tests (incl. lossy-network simulation)
benchmarks/             micro + end-to-end throughput benchmarks
samples/Echo/           console client/server demo
```

## Protocol notes

- One logical connection = one link per configured transport, grouped by a random 64-bit session token exchanged in the handshake (`Hello`/`Welcome`); UDP handshake datagrams are re-sent until acknowledged.
- Keepalive ping/pong on every link measures RTT and drops dead peers.
- Reliable UDP: 16-bit wrap-aware sequences per channel, immediate ACKs with a 32-packet history bitmask, RTO with exponential backoff, 256-packet range-bound send window.
- The handshake itself is not authenticated or encrypted; encryption covers channel payloads. Don't treat the session token as a security boundary on hostile networks.

## License

[MIT](LICENSE)
