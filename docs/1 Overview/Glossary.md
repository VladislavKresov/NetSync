---
tags: [overview]
---

# Glossary

| Term | Meaning | See |
|---|---|---|
| **Channel** | A byte id with a transport slot and options (reliability, compression, encryption, priority). Client and server must have identical channel tables. | [[Channels and delivery modes]], [[NetConfig]] |
| **Slot** | `TransportType.Tcp` (reliable ordered messages) or `TransportType.Udp` (datagrams). A contract, not a socket type: WebSocket and the relay stream fill the Tcp slot. | [[Custom transports]] |
| **Link** | One transport connection of one slot between a client and the server. | [[ITransport]], [[TransportPeer]] |
| **Connection** | The logical connection: one link per slot, grouped by the session token. Server side: [[NetConnection]]. | [[Layers]] |
| **Session token** | Random 64-bit value the client sends in Hello on every link. | [[Handshake v3]] |
| **Config hash** | FNV-1a over the channel table and MaxMessageSize, in Hello: mismatched tables fail with a clear error. | [[Handshake v3]] |
| **Join / approval** | After the handshake the client sends its connect data; `ConnectionApprover` accepts or refuses. | [[Connecting and approval]] |
| **Cipher suite** | `AesGcm` or `AesCbcHmacSha256`, negotiated per connection. | [[Keys and envelope]], [[CipherSuite]] |
| **Envelope** | `[MsgSecure][counter][tag][inner]` — every post-handshake packet on a keyed connection. | [[LinkSecurity]] |
| **Server identity** | Long-term ECDSA key that signs handshakes; clients pin its public key. | [[NetIdentity]], [[Security]] |
| **Degraded** | UDP could not be established; UDP channels ride the stream link. | [[Connecting and approval]] |
| **Trust mode** | Pre-handshake peers get tiny limits, no keepalive, a population cap. | [[Limits and hardening]] |
| **Cookie / Retry** | Proof of address a UDP-only server demands before it creates keys. | [[Handshake v3]] |
| **Batch** | Several small UDP packets in one datagram. | [[UDP batching]], [[DatagramBatcher]] |
| **Cork** | The receive thread holds back the batcher's wake-up while it handles one datagram, so replies leave together. | [[UDP batching]] |
| **Collapsed acks** | One ack per channel per received datagram instead of one per packet. | [[Reliable UDP]] |
| **Pooled receive** | Transports hand the peer layer their own buffer (valid during the call) instead of a copy. | [[Receive path]] |
| **Room** | A relay rendezvous identified by a short code; a host owns it, clients join by code. | [[Relay and NAT traversal]], [[RelayRoom]] |
| **Hole punching** | Both peers send to each other's public endpoint so their NATs open a direct UDP path. | [[Relay internals]] |
| **Polled / Immediate** | Event delivery on your thread (`PollEvents`) or on network threads. | [[Threading and events]] |
| **Write watchdog** | Closes a stream link whose remote stopped reading. | [[SendQueue]] |
