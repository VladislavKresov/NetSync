---
tags: [project]
---

# Known limitations

| Limitation | Impact | Next step / workaround |
|---|---|---|
| Reliable-ordered UDP in a closed request/response loop is ~2–3× slower than LiteNetLib | throughput-bound reliable traffic | piggyback acks on data more aggressively; per-datagram cost (~7.5 µs on Windows loopback) makes datagram count the lever — [UDP batching](../3%20Internals/UDP%20batching.md) |
| The relay's stream slot is always relayed | TCP channels of relayed clients cost relay bandwidth | configure only UDP channels — [Relay and NAT traversal](../2%20Guides/Relay%20and%20NAT%20traversal.md) |
| netstandard2.1 lacks allocation-free `ReceiveFrom`/`SendTo` | ~130 B per datagram on UDP servers and relay sockets (net8.0: 0); CBC encryption allocates per message | platform limit |
| A connection whose UDP died stays on the stream link | after a mid-session switch to TCP (see [When UDP is blocked](../2%20Guides/Connecting%20and%20approval.md#when-udp-is-blocked)) UDP channels keep the stream link's latency until the client reconnects | reconnect when convenient; a switch back to UDP is not implemented |
| Path MTU discovery is skipped on macOS | the runtime can't forbid fragmentation there, so fragments stay at the safe 1100 bytes | none needed; slightly more packets for bulk data |
| No connection migration | a client whose address changes reconnects instead | would need a connection id in the envelope |
| No JavaScript/TypeScript client | browsers need .NET (Blazor) for now | separate package — [Roadmap](Roadmap.md) |
| Verified in Unity 2021.3 (Mono) and 6000.2 (IL2CPP) on Windows only | Android, iOS and macOS are untested | run the adapter's PlayMode tests on devices |
| A few dependencies go against the layering | `ReliableEndpoint`/`LinkSecurity` build `PeerProtocol` messages, the relay reuses `ConnectionCrypto.FixedTimeEquals`, transports use `ChannelPriority` from the reliability folder | see [Dependencies against the layering](../4%20Diagrams/Dependency%20overview.md#dependencies-against-the-layering); candidates for small moves (e.g. a shared constant-time helper in `Internal`) |
