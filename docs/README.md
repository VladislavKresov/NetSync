---
tags: [moc]
aliases: [Index, Start]
---

# NetSync documentation

A universal low-level networking core for .NET — simple to use, reliable, flexible and secure. One zero-dependency DLL for apps and services, game servers and engines, streaming, file transfer and (over WebSocket) browsers. Version **2.0.0-rc.6**, wire protocol **4** (frozen: later releases extend it through negotiated features).

> [!TIP]
> **How these pages work**
> Plain Markdown with relative links: read them on GitHub, in an editor, or open `docs/` as a vault in Obsidian (its graph view shows the dependency graph; filter `path:Code`). Notes in `1`–`5` are written by hand; everything under `Code/` and the dependency diagrams in `4 Diagrams/` are generated from the source by `python tools/generate-vault.py` — rerun it after changing code. `python tools/mdlinks.py --check` verifies every link (CI runs it).

## Start here

- [What is NetSync](1%20Overview/What%20is%20NetSync.md) — purpose, principles, what's in the box
- [Layers](1%20Overview/Layers.md) — the architecture on one page
- [Quick start](2%20Guides/Quick%20start.md) — a server and a client in twenty lines
- [Repository and projects](1%20Overview/Repository%20and%20projects.md) — where things live, how the projects depend on each other
- [Glossary](1%20Overview/Glossary.md) — channel, slot, link, envelope, room, cork…

## Guides (using NetSync)

| Topic | Note |
|---|---|
| Channels, delivery modes, priorities | [Channels and delivery modes](2%20Guides/Channels%20and%20delivery%20modes.md) |
| Keys, cipher suites, server identity | [Security](2%20Guides/Security.md) |
| Connecting, connect data, approval, reconnect, fallback | [Connecting and approval](2%20Guides/Connecting%20and%20approval.md) |
| Server clock, queries, LAN discovery, MTU, simulator | [Session services](2%20Guides/Session%20services.md) |
| Browsers and firewalled networks | [Browsers and WebSocket](2%20Guides/Browsers%20and%20WebSocket.md) |
| Player-hosted games behind NAT | [Relay and NAT traversal](2%20Guides/Relay%20and%20NAT%20traversal.md) |
| Typed messages, files of any size | [Typed messages and streams](2%20Guides/Typed%20messages%20and%20streams.md) |
| Events and threads | [Threading and events](2%20Guides/Threading%20and%20events.md) |
| Your own transport | [Custom transports](2%20Guides/Custom%20transports.md) |
| Runnable examples | [Samples](2%20Guides/Samples.md) |

## Internals (how it works)

| Topic | Note |
|---|---|
| One message from `SendAsync` to the wire | [Send path](3%20Internals/Send%20path.md) |
| From the socket to your handler | [Receive path](3%20Internals/Receive%20path.md) |
| Hello, Welcome, Join | [Handshake v4](3%20Internals/Handshake%20v4.md) |
| ARQ, acks, congestion control | [Reliable UDP](3%20Internals/Reliable%20UDP.md) |
| Packing small packets into datagrams | [UDP batching](3%20Internals/UDP%20batching.md) |
| Key schedule and packet authentication | [Keys and envelope](3%20Internals/Keys%20and%20envelope.md) |
| Rooms, registration, hole punching | [Relay internals](3%20Internals/Relay%20internals.md) |
| Every byte on the wire | [Wire protocol](3%20Internals/Wire%20protocol.md) |
| Bounds against hostile input | [Limits and hardening](3%20Internals/Limits%20and%20hardening.md) |
| Threads, loops, buffer ownership | [Threads and loops](3%20Internals/Threads%20and%20loops.md) |

## Diagrams

- [Dependency overview](4%20Diagrams/Dependency%20overview.md) — folders/layers and the references between them (generated)
- [Full dependency graph](4%20Diagrams/Full%20dependency%20graph.md) — every module and every dependency (generated)
- [Type hierarchy](4%20Diagrams/Type%20hierarchy.md) — interfaces and their implementations (generated)
- [Architecture.canvas](4%20Diagrams/Architecture.canvas) — a board of all modules grouped by folder, clickable (generated)
- Per-folder maps: [Module map Peers](4%20Diagrams/Modules/Module%20map%20Peers.md), [Module map Transports](4%20Diagrams/Modules/Module%20map%20Transports.md), [Module map Transports Udp](4%20Diagrams/Modules/Module%20map%20Transports%20Udp.md), [Module map Relay](4%20Diagrams/Modules/Module%20map%20Relay.md), [Module map Pipeline Reliability](4%20Diagrams/Modules/Module%20map%20Pipeline%20Reliability.md), [Module map Pipeline Security](4%20Diagrams/Modules/Module%20map%20Pipeline%20Security.md) … (all in `4 Diagrams/Modules`)
- Sequence and flow diagrams live in the internals notes: [Handshake v4](3%20Internals/Handshake%20v4.md), [Send path](3%20Internals/Send%20path.md), [Receive path](3%20Internals/Receive%20path.md), [UDP batching](3%20Internals/UDP%20batching.md), [Relay internals](3%20Internals/Relay%20internals.md), [Keys and envelope](3%20Internals/Keys%20and%20envelope.md)

## Code reference

- [Code index](Code/Code%20index.md) — every module by layer, with summaries
- Entry points: [NetClient](Code/Peers/NetClient.md), [NetServer](Code/Peers/NetServer.md), [NetConfig](Code/Root/NetConfig.md), [NetConnection](Code/Peers/NetConnection.md), [NetRelayServer](Code/Relay/NetRelayServer.md), [RelayRoom](Code/Relay/RelayRoom.md)

## Project

- [Tests](5%20Project/Tests.md) · [Benchmarks and performance](5%20Project/Benchmarks%20and%20performance.md) · [Releasing](5%20Project/Releasing.md) · [Decision log](5%20Project/Decision%20log.md) · [Known limitations](5%20Project/Known%20limitations.md) · [Roadmap](5%20Project/Roadmap.md)
