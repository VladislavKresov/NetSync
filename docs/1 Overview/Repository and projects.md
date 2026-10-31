---
tags: [overview, diagram]
---

# Repository and projects

```mermaid
flowchart LR
  core["src/NetSync<br/>NetSync.dll<br/>netstandard2.1 + net8.0"]
  tests["tests/NetSync.Tests<br/>net8.0 build"] --> core
  testsns["tests/NetSync.Tests.NetStandard<br/>same sources, ns2.1 build"] --> core
  bench["benchmarks/NetSync.Benchmarks"] --> core
  bench --> lnl["LiteNetLib 2.1.4<br/>(comparison only)"]
  samples["samples/*<br/>Echo · FileTransfer · SecureLogin · Relay · WebSocket"] --> core
  unity["NetSync.Unity (sibling repo)<br/>UPM adapter"] -- "precompiled ns2.1 DLL" --> core
  nuget["NuGet package NetSync"] --- core
```

The core has **no** package references; the only external package anywhere is LiteNetLib in the benchmark project, for the side-by-side comparison ([[Benchmarks and performance]]).

## Layout

```
src/NetSync/                 the library (the only thing you ship)
  NetConfig.cs               configuration types
  Peers/                     NetClient, NetServer, NetConnection, handshake, events
  Transports/                slot contracts, framing, shared transport machinery
    Tcp/ Udp/ WebSockets/ Simulation/
  Relay/                     relay server and relay transports
  Pipeline/                  Reliability/ Security/ Fragmentation/ Compression/
  Sending/                   SendQueue
  Messaging/ Streams/ Discovery/ Serialization/ Diagnostics/ Internal/
tests/                       ~200 tests, run against both builds
benchmarks/                  throughput, compare, load, alloc, reliability
samples/                     console samples, each runnable in one process
tools/generate-vault.py      regenerates this vault's code notes and diagrams
docs/                        this vault
README.md, ARCHITECTURE.md
```

Per-folder module lists with diagrams: see [[Code index]] and `4 Diagrams/Modules`.

## Partial classes

[[NetClient]] and [[NetServer]] are split by concern into partial files — one code note each covers all parts:

| Module | Files |
|---|---|
| [[NetClient]] | `NetClient.cs` (API, send/receive, session end), `NetClient.Connect.cs` (links, handshake, fallback, join), `NetClient.Services.cs` (clock sync, MTU, reconnect) |
| [[NetServer]] | `NetServer.cs` (API, listeners, dispatch, send, stop), `NetServer.Handshake.cs` (hello, cookies, keys, join, approval, slots) |
