---
tags: [overview]
---

# What is NetSync

NetSync is a networking core: it moves bytes between a server and its clients over TCP, UDP, WebSocket or a relay, as one logical connection with typed *channels*, and takes care of reliability, ordering, fragmentation, compression, encryption, authentication, keepalive and reconnection. It is deliberately **low-level** — no object replication, no RPC framework, no engine types — so the same DLL serves a chat app, a file sync tool, a video stream and a game server. Engine integrations sit one level up in their own packages ([[Roadmap#Unity adapter|NetSync.Unity]] is the first).

## Principles (hard rules of the code)

1. **Zero dependencies** — BCL only; nothing to conflict with in any host.
2. **No engine references** — no `UnityEngine`, no main-thread assumptions; events are delivered either on network threads or when *you* poll ([[Threading and events]]).
3. **Both targets interoperate** — `netstandard2.1` (Unity 2021.2+) and `net8.0` speak one wire protocol; where a target lacks something (AES-GCM) the handshake negotiates ([[Keys and envelope]]).
4. **Off = free** — compression, encryption and reliability cost nothing on channels that don't use them.
5. **The network is hostile** — every length, count and header is bounded before anything is allocated or trusted ([[Limits and hardening]]).
6. **No allocation per message** on the steady-state data path of every built-in transport (net8.0), enforced by tests ([[Tests]]).

## What's in the box

| Area | What you get | Notes |
|---|---|---|
| Connection | TCP + UDP as one connection, any number of channels | [[Channels and delivery modes]], [[Connecting and approval]] |
| Reliability | five UDP delivery modes, congestion control, priorities | [[Reliable UDP]] |
| Throughput | UDP batching, coalesced TCP writes, zero-copy receive | [[UDP batching]], [[Benchmarks and performance]] |
| Security | per-packet authentication, AES-GCM/CBC, server identity, approval | [[Security]], [[Handshake v3]] |
| Reach | WebSocket for browsers, UDP→TCP fallback, relay + hole punching, IPv6 | [[Browsers and WebSocket]], [[Relay and NAT traversal]] |
| Services | server clock, queries, LAN discovery, path MTU, network simulator | [[Session services]] |
| Usability | typed messages, file streaming, statistics, auto-reconnect | [[Typed messages and streams]] |

## Where to go next

[[Layers]] for the architecture, [[Quick start]] for code, [[Glossary]] for the vocabulary.
