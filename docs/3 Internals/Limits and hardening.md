---
tags: [internals, security]
---

# Limits and hardening

**Caller mistakes throw** (unknown channel, oversized message or unreliable payload, invalid config). **Network garbage never throws**: malformed frames, bad tags, replays, unknown channels and hostile lengths are dropped (and logged); limit violations close the connection with `Error`.

| Bound | Value | Where |
|---|---|---|
| TCP frame body | 64 KB; 1 KB before the handshake | [[PacketProtocol]], [[TcpFrameReader]] |
| Reassembled message | `MaxMessageSize` (16 MB) + 128 B wire headroom | [[NetConfig]], [[FragmentBuffer]] |
| Fragments per message | derived from `MaxMessageSize` | [[FragmentBuffer]] |
| Concurrent reassemblies | 16; buffered fragment bytes capped | [[FragmentBuffer]] |
| Decompression | declared size ≤ `MaxMessageSize`, exact length enforced | [[Compressor]] |
| Send queue | 10 000 jobs, 4 MB of bulk bytes | [[SendQueue]] |
| UDP batch queue | 256 KB per destination | [[DatagramBatcher]] |
| Polled events | `MaxPendingEvents` (100 000) | [[EventDispatcher]] |
| Untrusted UDP peers | 1024; ≤ 1 KB datagrams; one packet per batch | [[UdpServerTransport]] |
| Pending handshakes | 1024 | [[NetServer]] |
| Connect data | 4 KB | [[PeerProtocol]] |
| Connectionless replies | ≤ request size, ≤ 1000/s | [[UdpServerTransport]], [[NetQuery]] |
| WebSocket handshake | 8 KB request, 10 s | [[WebSocketHandshake]], [[WebSocketServerTransport]] |
| Open streams per receiver | 16 | [[NetStreams]] |
| Write stalled | `WriteTimeoutMs` (15 s) → close | [[SendQueue]], [[TcpTransport]] |
| Relay | clients per room capped, stale UDP records expire after 60 s | [[NetRelayServer]] |

## Spoofing and floods

- UDP peers are created only by data, never by pings (no reflection); pre-handshake peers get no keepalive.
- Only stream links create connections; UDP-only servers require a cookie first ([[Handshake v3]]).
- `MaxConnections` is reserved before the approval, so bursts can't overshoot.
- Accept loops survive misbehaving clients and back off on failing accepts ([[TcpAcceptLoop]]).

## Parsing untrusted data

[[NetReader]] reports every truncated or over-long field as `EndOfStreamException` — the one exception message handlers treat as "malformed" ([[NetMessages]], [[NetStreams]]).
