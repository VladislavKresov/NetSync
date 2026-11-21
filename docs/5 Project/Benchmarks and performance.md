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

Windows 11, Ryzen 9 5900X, .NET 8, client and server in one process, 2.0.0-rc.6 (2026-11-21):

| Scenario | Result |
|---|---|
| UDP small messages, echo | ~1.13 M msg/s round-trip, ~1 B/message |
| TCP small messages, echo | ~0.69 M msg/s, ~5 B/message |
| Reliable-ordered UDP, echo | ~0.23 M msg/s; bulk ~220 MB/s |
| TCP bulk (fragmented) | ~475 MB/s through the full stack |
| Authenticated TCP / UDP | ~0.58 M / ~0.64 M msg/s, 0 B/message; + encrypted TCP ~0.43 M |
| UDP RTT | p50 ≈ 28 µs, p99 ≈ 64 µs (reliable-ordered: 41 / 81 µs) |
| 1000 clients | connect in 0.2 s, ~101 KB per connection (both sides), idle ≈ 2.8 % of one core |
| Congestion control (2 MB/s bottleneck, 50 ms queue) | ~0.4 % retransmissions end to end, ~70 % without it (`TrafficTests`) |

The allocation figures of the benchmark include the harness; the data path itself is checked at 0 B per message by `ZeroAllocationTests`.

| vs LiteNetLib 2.1.4 | NetSync | LiteNetLib |
|---|---|---|
| UDP unreliable | 1.18 M msg/s | 0.69 M |
| UDP reliable-ordered | 0.22 M | 0.40 M |
| Latency p50 (unreliable) | 0.031 ms | 0.059 ms |

## History

| Change | Effect |
|---|---|
| Zero-allocation data path (stage 15) | TCP echo 112 k → 650–800 k msg/s; 250–1500 B → <1 B per message |
| Growable receive buffers (stage 20) | 596 KB → 100 KB per connection |
| UDP batching + collapsed acks (stage 20) | unreliable 101 k → 1.2 M, reliable-ordered 62 k → 0.2 M |
| Relay address caches, closure fixes (review) | relayed paths 32–700 B → 0 B per round trip |

Where reliable-ordered could go next: [Known limitations](Known%20limitations.md). Why datagram count matters: [UDP batching](../3%20Internals/UDP%20batching.md).
