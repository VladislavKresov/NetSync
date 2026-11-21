---
tags: [internals, security]
---

# Limits and hardening

**Caller mistakes throw** (unknown channel, oversized message or unreliable payload, invalid config). **Network garbage never throws**: malformed frames, bad tags, replays, unknown channels and hostile lengths are dropped (and logged); limit violations close the connection with `Error`.

| Bound | Value | Where |
|---|---|---|
| TCP frame body | 64 KB; 1 KB before the handshake | [PacketProtocol](../Code/Transports/PacketProtocol.md), [TcpFrameReader](../Code/Transports/Tcp/TcpFrameReader.md) |
| Reassembled message | `MaxMessageSize` (16 MB) + 128 B wire headroom | [NetConfig](../Code/Root/NetConfig.md), [FragmentBuffer](../Code/Pipeline/Fragmentation/FragmentBuffer.md) |
| Fragments per message | derived from `MaxMessageSize` | [FragmentBuffer](../Code/Pipeline/Fragmentation/FragmentBuffer.md) |
| Concurrent reassemblies | 16; buffered fragment bytes capped | [FragmentBuffer](../Code/Pipeline/Fragmentation/FragmentBuffer.md) |
| Decompression | declared size ≤ `MaxMessageSize`, exact length enforced | [Compressor](../Code/Pipeline/Compression/Compressor.md) |
| Send queue | 10 000 jobs, 4 MB of bulk bytes | [SendQueue](../Code/Sending/SendQueue.md) |
| UDP batch queue | 256 KB per destination | [DatagramBatcher](../Code/Transports/Udp/DatagramBatcher.md) |
| Polled events | `MaxPendingEvents` (100 000) | [EventDispatcher](../Code/Peers/EventDispatcher.md) |
| Untrusted UDP peers | 1024; ≤ 1 KB datagrams; one packet per batch | [UdpServerTransport](../Code/Transports/Udp/UdpServerTransport.md) |
| Pending handshakes | 1024 | [NetServer](../Code/Peers/NetServer.md) |
| Connect data | 4 KB | [PeerProtocol](../Code/Peers/PeerProtocol.md) |
| Connectionless replies | ≤ request size, ≤ 1000/s | [UdpServerTransport](../Code/Transports/Udp/UdpServerTransport.md), [NetQuery](../Code/Discovery/NetQuery.md) |
| WebSocket handshake | 8 KB request, 10 s | [WebSocketHandshake](../Code/Transports/WebSockets/WebSocketHandshake.md), [WebSocketServerTransport](../Code/Transports/WebSockets/WebSocketServerTransport.md) |
| Open streams per receiver | 16 | [NetStreams](../Code/Streams/NetStreams.md) |
| Write stalled | `WriteTimeoutMs` (15 s) → close | [SendQueue](../Code/Sending/SendQueue.md), [TcpTransport](../Code/Transports/Tcp/TcpTransport.md) |
| Relay | clients per room capped, stale UDP records expire after 60 s | [NetRelayServer](../Code/Relay/NetRelayServer.md) |

## Spoofing and floods

- UDP peers are created only by data, never by pings (no reflection); pre-handshake peers get no keepalive.
- Only stream links create connections; UDP-only servers require a cookie first ([Handshake v4](Handshake%20v4.md)).
- `MaxConnections` is reserved before the approval, so bursts can't overshoot.
- Accept loops survive misbehaving clients and back off on failing accepts ([TcpAcceptLoop](../Code/Transports/TcpAcceptLoop.md)).

## Parsing untrusted data

[NetReader](../Code/Serialization/NetReader.md) reports every truncated or over-long field as `EndOfStreamException` — the one exception message handlers treat as "malformed" ([NetMessages](../Code/Messaging/NetMessages.md), [NetStreams](../Code/Streams/NetStreams.md)).
