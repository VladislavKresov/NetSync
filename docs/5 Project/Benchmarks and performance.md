---
tags: [project]
---

# Benchmarks and performance

`benchmarks/NetSync.Benchmarks`, `dotnet run -c Release -- <mode>`:

| Mode | Measures |
|---|---|
| `micro` | framing and fragmentation |
| `throughput` | loopback echo rates, bulk, RTT, allocations — plain and secure pairs |
| `compare` | NetSync next to LiteNetLib 2.1.4, same methodology (64 messages in flight) |
| `load [N]` | N clients on one server: connect time, memory, broadcast, fan-in, idle CPU |
| `alloc [relay]` | sampled allocation events by type (direct or through a relay) |
| `reliability` | the reliability engine alone, no sockets |

## Current numbers

Windows 11, Ryzen 9 5900X, .NET 8, client and server in one process (2026-10-16):

| Scenario | Result |
|---|---|
| UDP small messages, echo | ~1.2 M msg/s round-trip, 0 B/message |
| TCP small messages, echo | ~0.7–0.8 M msg/s, ≈0 B/message |
| Reliable-ordered UDP, echo | ~0.2 M msg/s; bulk ~250 MB/s |
| TCP bulk (fragmented) | ~470 MB/s through the full stack |
| Authenticated TCP / UDP | ~0.55 M / ~0.6 M msg/s; + encrypted TCP ~0.4 M |
| UDP RTT | p50 ≈ 27 µs, p99 ≈ 80 µs |
| 1000 clients | connect in 0.2 s, ~100 KB per connection (both sides), idle ≈ 3 % of one core |
| Congestion control (2 MB/s bottleneck) | 4.5 % retransmissions at ~92 % utilization |

| vs LiteNetLib 2.1.4 | NetSync | LiteNetLib |
|---|---|---|
| UDP unreliable | 1.1–1.3 M msg/s | 0.6–0.8 M |
| UDP reliable-ordered | 0.18–0.25 M | 0.4–0.65 M |
| Latency p50 | 0.027 ms | 0.055 ms |

## History

| Change | Effect |
|---|---|
| Zero-allocation data path (stage 15) | TCP echo 112 k → 650–800 k msg/s; 250–1500 B → <1 B per message |
| Growable receive buffers (stage 20) | 596 KB → 100 KB per connection |
| UDP batching + collapsed acks (stage 20) | unreliable 101 k → 1.2 M, reliable-ordered 62 k → 0.2 M |
| Relay address caches, closure fixes (review) | relayed paths 32–700 B → 0 B per round trip |

Where reliable-ordered could go next: [[Known limitations]]. Why datagram count matters: [[UDP batching]].
