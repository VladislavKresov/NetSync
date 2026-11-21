---
tags: [guide]
---

# Samples

`samples/` holds console programs. In a sample folder, `dotnet run` without arguments plays every role in one process; with arguments the roles run on separate machines.

## Echo

`samples/Echo` — the smallest client/server: channel 0 TCP, channel 1 UDP, echo. `dotnet run -- server 7777`, `dotnet run -- client 7777 tcp|udp`. `dotnet run -- selftest` runs both ends in one process over an encrypted connection and exits 0 when every echo came back — CI runs it as a NativeAOT binary ([Releasing](../5%20Project/Releasing.md)).

## FileTransfer

`samples/FileTransfer` — a 64 MB file on a `ReliableOrdered` + `Low` channel while 50 position updates a second keep flowing on an `UnreliableSequenced` channel; progress on both ends, SHA-256 check at the end. Uses [NetStreams](../Code/Streams/NetStreams.md) and a progress-counting stream. Typical result on loopback: ~200 MB/s, position round trip ~1 ms during the transfer.

## SecureLogin

`samples/SecureLogin` — key exchange + [NetIdentity](../Code/Pipeline/Security/NetIdentity.md): only the server holding the identity can complete the handshake, the client pins `server.pub`; the login travels as encrypted connect data; `ConnectionApprover` checks it. The demo shows a good login (AES-GCM), a wrong password (`Application` refusal) and an impostor server (`ServerIdentity` refusal). See [Security](Security.md), [Connecting and approval](Connecting%20and%20approval.md).

## Relay

`samples/Relay` — [NetRelayServer](../Code/Relay/NetRelayServer.md), a host with a room ([RelayRoom](../Code/Relay/RelayRoom.md)) that keeps its LAN port, a client joining by code; ECDH end to end; the demo prints how many UDP bytes the relay still forwarded after punching (0 on loopback). See [Relay and NAT traversal](Relay%20and%20NAT%20traversal.md).

## WebSocket

`samples/WebSocket` — one server, a native TCP+UDP client and a WebSocket client without UDP (stands in for a browser), chatting through the same channels; the WebSocket client's UDP channel rides the WebSocket. See [Browsers and WebSocket](Browsers%20and%20WebSocket.md).
