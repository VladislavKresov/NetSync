---
tags: [overview]
---

# What is NetSync

NetSync is a networking core: it moves bytes between a server and its clients over TCP, UDP, WebSocket or a relay, as one logical connection with typed *channels*, and takes care of reliability, ordering, fragmentation, compression, encryption, authentication, keepalive and reconnection. It is deliberately **low-level** — no object replication, no RPC framework, no engine types — so the same DLL serves a chat app, a file sync tool, a video stream and a game server. Engine integrations sit one level up in their own packages ([NetSync.Unity](https://github.com/VladislavKresov/NetSync.Unity) is the first).

## Principles (hard rules of the code)

1. **Zero dependencies** — BCL only; nothing to conflict with in any host.
2. **No engine references** — no `UnityEngine`, no main-thread assumptions; events are delivered either on network threads or when *you* poll ([Threading and events](../2%20Guides/Threading%20and%20events.md)).
3. **Both targets interoperate** — `netstandard2.1` (Unity 2021.2+) and `net8.0` speak one wire protocol; where a target lacks something (AES-GCM) the handshake negotiates ([Keys and envelope](../3%20Internals/Keys%20and%20envelope.md)).
4. **Off = free** — compression, encryption and reliability cost nothing on channels that don't use them.
5. **The network is hostile** — every length, count and header is bounded before anything is allocated or trusted ([Limits and hardening](../3%20Internals/Limits%20and%20hardening.md)).
6. **No allocation per message** on the steady-state data path of every built-in transport (net8.0), enforced by tests ([Tests](../5%20Project/Tests.md)).

## What's in the box

| Area | What you get | Notes |
|---|---|---|
| Connection | TCP + UDP as one connection, any number of channels | [Channels and delivery modes](../2%20Guides/Channels%20and%20delivery%20modes.md), [Connecting and approval](../2%20Guides/Connecting%20and%20approval.md) |
| Reliability | five UDP delivery modes, congestion control, priorities | [Reliable UDP](../3%20Internals/Reliable%20UDP.md) |
| Throughput | UDP batching, coalesced TCP writes, zero-copy receive | [UDP batching](../3%20Internals/UDP%20batching.md), [Benchmarks and performance](../5%20Project/Benchmarks%20and%20performance.md) |
| Security | per-packet authentication, AES-GCM/CBC, server identity, approval | [Security](../2%20Guides/Security.md), [Handshake v4](../3%20Internals/Handshake%20v4.md) |
| Reach | WebSocket for browsers, UDP→TCP fallback, relay + hole punching, IPv6 | [Browsers and WebSocket](../2%20Guides/Browsers%20and%20WebSocket.md), [Relay and NAT traversal](../2%20Guides/Relay%20and%20NAT%20traversal.md) |
| Services | server clock, queries, LAN discovery, path MTU, network simulator | [Session services](../2%20Guides/Session%20services.md) |
| Usability | typed messages, file streaming, statistics, auto-reconnect | [Typed messages and streams](../2%20Guides/Typed%20messages%20and%20streams.md) |

## Where to go next

[Layers](Layers.md) for the architecture, [Quick start](../2%20Guides/Quick%20start.md) for code, [Glossary](Glossary.md) for the vocabulary.
