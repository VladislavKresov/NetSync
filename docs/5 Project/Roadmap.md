---
tags: [project]
---

# Roadmap

## Done

Phases 2.0 (stages 0–8: core extraction, transports, peer layer, reliability, security, discovery, Unity adapter), 2.1 (stages 9–14: hardening, protocol v2, factories, congestion control, packet authentication, usability) and 2.2 (stages 15–21: zero allocation, protocol v3, session services, WebSocket and listeners, relay, scale, docs), followed by a full review — see [Decision log](Decision%20log.md).

## Next

Towards 1.0, on protocol v4 ([Handshake v4](../3%20Internals/Handshake%20v4.md)):

- [x] Protocol v4: key exchange that works on every runtime, extension points, application id, relay protocol version.
- [x] Verified in Unity 2021.3 (Mono) and 6000.2 (IL2CPP) on Windows.
- [x] Behaviour fixes: reliable data never dropped silently when the event queue overflows; one backpressure rule for sends; UDP loss mid-session degrades to the stream link instead of closing; ordered channels keep send order under backpressure.
- [x] Tests on .NET 8 and .NET 10, CI on Windows, Linux and macOS; public API checked against 2.0.0-rc.5 on every build.
- [ ] Verify on Android and iOS.

After 1.0, without breaking v4:

- [ ] Reliable-ordered throughput (extended reliability format under a feature bit) — [Known limitations](Known%20limitations.md).
- [ ] Connection migration for clients whose address changes (feature bit).
- [ ] JavaScript/TypeScript browser client over WebSocket (separate repo), then Unity WebGL.
- Out of scope for the core: codecs, object replication, RPC frameworks — they belong in packages on top.
