---
tags: [internals, protocol]
---

# Wire protocol (version 3)

## Transport layer — [PacketProtocol](../Code/Transports/PacketProtocol.md)

| Packet | Layout |
|---|---|
| UDP datagram | `[type:1][payload]` |
| TCP frame | `[type:1][bodyLen:4 BE][body]`, body ≤ 64 KB (≤ 1 KB before the handshake) |
| WebSocket / relay stream message | `[type:1][payload]` — one binary message per packet |
| Ping / Pong `0x01` / `0x02` | `[timestampMs:8 BE]` |
| Data `0x10` | body = peer message |
| Fragment (stream links) | body = `[0x11][fragId:4][index:4][total:4][chunk ≤ 16 KB]` — [PacketFragmenter](../Code/Pipeline/Fragmentation/PacketFragmenter.md) |
| Batch `0x12` (UDP) | `[len:varint][packet][len][packet]…` ≤ 1200 B — [DatagramBatcher](../Code/Transports/Udp/DatagramBatcher.md) |
| Query `0x30` / QueryReply `0x31` | `[id:4][requestLen:2][request][padding]` / `[id:4][response]`, reply ≤ query — [NetQuery](../Code/Discovery/NetQuery.md) |
| MtuProbe `0x32` / MtuReply `0x33` | `[nonce:4][padding]` / `[nonce:4][receivedSize:2]` — [PathMtuProbe](../Code/Transports/Udp/PathMtuProbe.md) |

## Peer layer — [PeerProtocol](../Code/Peers/PeerProtocol.md)

Inside transport Data; after the handshake inside the envelope on keyed connections.

| Message | Layout |
|---|---|
| Hello `0x01` | `[ver=4][token:8][configHash:4][keyExchange:1][suites:1][appId:8][appVersion:2][features:4][pubLen:1][pub…][cookieLen:1][cookie…][extLen:2][extensions…]` — [Handshake v4](Handshake%20v4.md) |
| Welcome `0x02` | `[connectionId:8][suite:1][features:4][pubLen:1][pub…][idLen:1][identityKey…][extLen:2][extensions…]` |
| Reject `0x03` | `[reason:1]` 1 version · 2 encryption · 3 configuration · 4 authentication · 5 application |
| Bye `0x04` | `[reason:1]` 0 graceful/shutdown · 1 kicked |
| Degrade `0x05` | (empty) — carry the UDP channels over the stream link. From the client while connecting; from either side mid-session when both agreed on feature bit 2 (the receiver follows: its reliability layer moves to the stream link too, unacknowledged packets are resent there before newer messages) |
| Retry `0x06` | `[cookie:16]` |
| Join `0x07` | `[connectData…]` (encrypted when keyed) |
| JoinAccept `0x08` / JoinReject `0x09` | — / `[reason utf8]` |
| TimeReq `0x0A` / TimeResp `0x0B` | `[clientTime:8]` / `[clientTime:8][serverTime:8]` (double bits, BE) |
| PathMtu `0x0C` | `[datagramSize:2]` |
| Data `0x10` | `[channel:1][payload]` |
| RelData `0x20` | `[flags:1][channel:1][seq:2 BE][payload]` (flags bit0 = fragment) — [ReliableEndpoint](../Code/Pipeline/Reliability/ReliableEndpoint.md) |
| RelAck `0x21` | `[channel:1][anchorSeq:2 BE][mask:4 BE]` |
| Secure `0x40` | `[counter:8 BE][tag:16][inner peer message]` — [LinkSecurity](../Code/Pipeline/Security/LinkSecurity.md) |

`0x11` is reserved in the peer namespace: stream and message links recognise a fragment by that first body byte.

## Higher formats

| Format | Layout | Module |
|---|---|---|
| Channel transform | `[flags:1][body]` bit0 compressed, bit1 encrypted; compressed = `[origLen:4 BE][deflate]` | [ChannelTransform](../Code/Peers/ChannelTransform.md), [Compressor](../Code/Pipeline/Compression/Compressor.md) |
| GCM / CBC payload | `[nonce:12][tag:16][ct]` / `[iv:16][ct]` | [ConnectionCrypto](../Code/Pipeline/Security/ConnectionCrypto.md) |
| Typed message | `[id: var-uint][body]` | [NetMessages](../Code/Messaging/NetMessages.md) |
| Stream | `[kind:1][streamId:4 LE]…` 1 begin · 2 chunk · 3 end · 4 abort | [NetStreams](../Code/Streams/NetStreams.md) |
| LAN discovery | `NSD2` magic, app id, port, payload ≤ 1 KB | [DiscoveryMessage](../Code/Discovery/DiscoveryMessage.md) |

## Relay — [RelayProtocol](../Code/Relay/RelayProtocol.md)

Stream frames `[kind:1][len:4][body]`: HostHello `0x01`, HostWelcome `0x02`, ClientHello `0x03`, ClientWelcome `0x04`, Refused `0x05`, Data `0x10`, PeerData `0x11`, PeerOpen `0x12`, PeerClose `0x13`, Keepalive `0x14`. Datagrams: HostRegister `0x80`, HostRegistered `0x81`, ClientRegister `0x82`, ClientRegistered `0x83`, Introduce `0x84`, RelayData `0x85`, Punch `0x86`, DirectData `0x87`.

Bump `PeerProtocol.ProtocolVersion` on any breaking change — old peers get a `Reject`.
