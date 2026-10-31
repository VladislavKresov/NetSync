---
tags: [moc]
aliases: [Index, Start]
---

# NetSync knowledge base

A universal low-level networking core for .NET — simple to use, reliable, flexible and secure. One zero-dependency DLL for apps and services, game servers and engines, streaming, file transfer and (over WebSocket) browsers. Version **2.0.0-rc.3**, wire protocol **3**.

> [!tip] How to use this vault
> Open the `docs/` folder as a vault in Obsidian. Notes in `1`–`5` are written by hand; everything under `Code/` and the dependency diagrams in `4 Diagrams/` are generated from the source by `python tools/generate-vault.py` — rerun it after changing code. The **graph view** shows the full dependency graph interactively (filter `path:Code`, colour groups are preset by layer).

## Start here

- [[What is NetSync]] — purpose, principles, what's in the box
- [[Layers]] — the architecture on one page
- [[Quick start]] — a server and a client in twenty lines
- [[Repository and projects]] — where things live, how the projects depend on each other
- [[Glossary]] — channel, slot, link, envelope, room, cork…

## Guides (using NetSync)

| Topic | Note |
|---|---|
| Channels, delivery modes, priorities | [[Channels and delivery modes]] |
| Keys, cipher suites, server identity | [[Security]] |
| Connecting, connect data, approval, reconnect, fallback | [[Connecting and approval]] |
| Server clock, queries, LAN discovery, MTU, simulator | [[Session services]] |
| Browsers and firewalled networks | [[Browsers and WebSocket]] |
| Player-hosted games behind NAT | [[Relay and NAT traversal]] |
| Typed messages, files of any size | [[Typed messages and streams]] |
| Events and threads | [[Threading and events]] |
| Your own transport | [[Custom transports]] |
| Runnable examples | [[Samples]] |

## Internals (how it works)

| Topic | Note |
|---|---|
| One message from `SendAsync` to the wire | [[Send path]] |
| From the socket to your handler | [[Receive path]] |
| Hello, Welcome, Join | [[Handshake v3]] |
| ARQ, acks, congestion control | [[Reliable UDP]] |
| Packing small packets into datagrams | [[UDP batching]] |
| Key schedule and packet authentication | [[Keys and envelope]] |
| Rooms, registration, hole punching | [[Relay internals]] |
| Every byte on the wire | [[Wire protocol]] |
| Bounds against hostile input | [[Limits and hardening]] |
| Threads, loops, buffer ownership | [[Threads and loops]] |

## Diagrams

- [[Dependency overview]] — folders/layers and the references between them (generated)
- [[Full dependency graph]] — every module and every dependency (generated)
- [[Type hierarchy]] — interfaces and their implementations (generated)
- [[Architecture.canvas]] — a board of all modules grouped by folder, clickable (generated)
- Per-folder maps: [[Module map Peers]], [[Module map Transports]], [[Module map Transports Udp]], [[Module map Relay]], [[Module map Pipeline Reliability]], [[Module map Pipeline Security]] … (all in `4 Diagrams/Modules`)
- Sequence and flow diagrams live in the internals notes: [[Handshake v3]], [[Send path]], [[Receive path]], [[UDP batching]], [[Relay internals]], [[Keys and envelope]]

## Code reference

- [[Code index]] — every module by layer, with summaries
- Entry points: [[NetClient]], [[NetServer]], [[NetConfig]], [[NetConnection]], [[NetRelayServer]], [[RelayRoom]]

## Project

- [[Tests]] · [[Benchmarks and performance]] · [[Releasing]] · [[Decision log]] · [[Known limitations]] · [[Roadmap]]
