---
tags: [overview]
---

# Glossary

| Term | Meaning | See |
|---|---|---|
| **Channel** | A byte id with a transport slot and options (reliability, compression, encryption, priority). Client and server must have identical channel tables. | [Channels and delivery modes](../2%20Guides/Channels%20and%20delivery%20modes.md), [NetConfig](../Code/Root/NetConfig.md) |
| **Slot** | `TransportType.Tcp` (reliable ordered messages) or `TransportType.Udp` (datagrams). A contract, not a socket type: WebSocket and the relay stream fill the Tcp slot. | [Custom transports](../2%20Guides/Custom%20transports.md) |
| **Link** | One transport connection of one slot between a client and the server. | [ITransport](../Code/Transports/ITransport.md), [TransportPeer](../Code/Transports/TransportPeer.md) |
| **Connection** | The logical connection: one link per slot, grouped by the session token. Server side: [NetConnection](../Code/Peers/NetConnection.md). | [Layers](Layers.md) |
| **Session token** | Random 64-bit value the client sends in Hello on every link. | [Handshake v4](../3%20Internals/Handshake%20v4.md) |
| **Config hash** | FNV-1a over the channel table and MaxMessageSize, in Hello: mismatched tables fail with a clear error. | [Handshake v4](../3%20Internals/Handshake%20v4.md) |
| **Join / approval** | After the handshake the client sends its connect data; `ConnectionApprover` accepts or refuses. | [Connecting and approval](../2%20Guides/Connecting%20and%20approval.md) |
| **Cipher suite** | `AesGcm` or `AesCbcHmacSha256`, negotiated per connection. | [Keys and envelope](../3%20Internals/Keys%20and%20envelope.md), [CipherSuite](../Code/Pipeline/Security/CipherSuite.md) |
| **Envelope** | `[MsgSecure][counter][tag][inner]` — every post-handshake packet on a keyed connection. | [LinkSecurity](../Code/Pipeline/Security/LinkSecurity.md) |
| **Server identity** | Long-term X25519 key mixed into every handshake's session keys; clients pin its public key. | [NetIdentity](../Code/Pipeline/Security/NetIdentity.md), [Security](../2%20Guides/Security.md) |
| **Degraded** | UDP could not be established, or stopped working mid-session; UDP channels ride the stream link. | [Connecting and approval](../2%20Guides/Connecting%20and%20approval.md) |
| **Trust mode** | Pre-handshake peers get tiny limits, no keepalive, a population cap. | [Limits and hardening](../3%20Internals/Limits%20and%20hardening.md) |
| **Cookie / Retry** | Proof of address a UDP-only server demands before it creates keys. | [Handshake v4](../3%20Internals/Handshake%20v4.md) |
| **Batch** | Several small UDP packets in one datagram. | [UDP batching](../3%20Internals/UDP%20batching.md), [DatagramBatcher](../Code/Transports/Udp/DatagramBatcher.md) |
| **Cork** | The receive thread holds back the batcher's wake-up while it handles one datagram, so replies leave together. | [UDP batching](../3%20Internals/UDP%20batching.md) |
| **Collapsed acks** | One ack per channel per received datagram instead of one per packet. | [Reliable UDP](../3%20Internals/Reliable%20UDP.md) |
| **Pooled receive** | Transports hand the peer layer their own buffer (valid during the call) instead of a copy. | [Receive path](../3%20Internals/Receive%20path.md) |
| **Room** | A relay rendezvous identified by a short code; a host owns it, clients join by code. | [Relay and NAT traversal](../2%20Guides/Relay%20and%20NAT%20traversal.md), [RelayRoom](../Code/Relay/RelayRoom.md) |
| **Hole punching** | Both peers send to each other's public endpoint so their NATs open a direct UDP path. | [Relay internals](../3%20Internals/Relay%20internals.md) |
| **Polled / Immediate** | Event delivery on your thread (`PollEvents`) or on network threads. | [Threading and events](../2%20Guides/Threading%20and%20events.md) |
| **Write watchdog** | Closes a stream link whose remote stopped reading. | [SendQueue](../Code/Sending/SendQueue.md) |
