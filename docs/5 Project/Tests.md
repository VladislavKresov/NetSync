---
tags: [project]
---

# Tests

~200 tests, run twice: against the net8.0 build (`tests/NetSync.Tests`) and, from the same sources, against the netstandard2.1 build (`tests/NetSync.Tests.NetStandard`, `CORE_NETSTANDARD` defined — proves compilation and `#if` paths; it runs on .NET 8, not Mono).

```bash
dotnet test NetSync.sln -c Release
```

| File | Covers |
|---|---|
| `FragmentationTests`, `SendQueueTests` | split/reassemble, priorities, overflow, pool ownership |
| `TcpLoopbackTests`, `UdpLoopbackTests` | transport echo, fragmentation, reuse, ping, timeouts, no ping reflection |
| `DatagramBatchingTests` | batches, order, size cap, backlog cap, malformed batches, untrusted sources, collapsed acks, flush on close |
| `PeerTests` | TCP+UDP as one connection, routing, broadcast, kick, Polled mode, config-hash rejection |
| `HandshakeTests` | suites, identity pinning, connect data, approval, MaxConnections under simultaneous joins, cookies, connect timeout |
| `ReliableEndpointTests`, `PeerReliabilityTests` | 25 % loss + duplicates, sequenced, dead link, 70 000-message wraparound; 1 MB through a 5 %-loss proxy |
| `SecurityTests`, `HeaderSecurityTests` | ciphers, ECDH, compression bombs, downgrade, wrong PSK; envelope tamper/replay/cross-link, forged-ACK attack, link hijack |
| `LimitsTests` | 2 GB frame header, pre-handshake limits, slow consumer, caps, MaxMessageSize both ways |
| `TransportFactoryTests`, `WebSocketTests` | in-memory transport, UDP→TCP fallback, WebSocket (ws/wss, Origin allow-list, UDP-less clients) |
| `RelayTests` | relay, cone NAT (direct) / symmetric NAT (relayed) simulators, end-to-end encryption, relay restart, UDP-only direct path |
| `SessionServicesTests` | time sync, queries (no amplification, rate cap), discovery payload, simulator, path MTU |
| `TrafficTests` | congestion control on a 2 MB/s bottleneck, Low priority yielding, streams and receiver limits |
| `ZeroAllocationTests` | per-message allocation budgets on every transport, relayed and direct, typed messages; send-buffer reuse |
| `ScaleTests`, `UsabilityTests`, `SerializationTests`, `DiscoveryTests` | 1000 connections; messages, reconnect, dual stack; reader/writer and hostile lengths; discovery |

## Conventions

- A test that guards a fix was checked to **fail with the fix reverted** (or, for races, a deterministic unit test covers it).
- Allocation tests run alone (`AllocationCollection`), take the best of several windows; netstandard2.1 has its own budgets where the platform lacks allocation-free socket APIs.
- Network tests on loopback retry raw probes and allow for punching to finish — a loaded CI machine must not make them flaky.
- Helpers: `InMemoryTransport` (reference third-party transport, wiretap, injection), `BottleneckPipe` (virtual-time drop-tail link), NAT simulators in `RelayTests`.
- Gotcha: restoring a file with an old timestamp makes MSBuild reuse a stale binary — touch it or build with `--no-incremental`.
