---
tags: [project]
---

# Known limitations

| Limitation | Impact | Next step / workaround |
|---|---|---|
| Reliable-ordered UDP in a closed request/response loop is ~2–3× slower than LiteNetLib | throughput-bound reliable traffic | piggyback acks on data more aggressively; per-datagram cost (~7.5 µs on Windows loopback) makes datagram count the lever — [[UDP batching]] |
| The relay's stream slot is always relayed | TCP channels of relayed clients cost relay bandwidth | configure only UDP channels — [[Relay and NAT traversal]] |
| netstandard2.1 lacks allocation-free `ReceiveFrom`/`SendTo` | ~130 B per datagram on UDP servers and relay sockets (net8.0: 0); CBC encryption allocates per message | platform limit |
| No connection migration | a client whose address changes reconnects instead | would need a connection id in the envelope |
| No JavaScript/TypeScript client | browsers need .NET (Blazor) for now | separate package — [[Roadmap]] |
| Not yet verified in real Unity/Mono/IL2CPP or on devices | runtime surprises possible (ECDH/ECDsa availability) | [[Releasing]] checklist |
| A few dependencies go against the layering | `ReliableEndpoint`/`LinkSecurity` build `PeerProtocol` messages, the relay reuses `ConnectionCrypto.FixedTimeEquals`, transports use `ChannelPriority` from the reliability folder | see [[Dependency overview#Dependencies against the layering]]; candidates for small moves (e.g. a shared constant-time helper in `Internal`) |
