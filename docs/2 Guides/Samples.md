---
tags: [guide]
---

# Samples

`samples/` holds console programs. In a sample folder, `dotnet run` without arguments plays every role in one process; with arguments the roles run on separate machines.

## Echo

`samples/Echo` — the smallest client/server: channel 0 TCP, channel 1 UDP, echo. `dotnet run -- server 7777`, `dotnet run -- client 7777 tcp|udp`. Also the NativeAOT smoke test ([[Releasing]]).

## FileTransfer

`samples/FileTransfer` — a 64 MB file on a `ReliableOrdered` + `Low` channel while 50 position updates a second keep flowing on an `UnreliableSequenced` channel; progress on both ends, SHA-256 check at the end. Uses [[NetStreams]] and a progress-counting stream. Typical result on loopback: ~200 MB/s, position round trip ~1 ms during the transfer.

## SecureLogin

`samples/SecureLogin` — ECDH + [[NetIdentity]]: the server signs handshakes, the client pins `server.pub`; the login travels as encrypted connect data; `ConnectionApprover` checks it. The demo shows a good login (AES-GCM), a wrong password (`Application` refusal) and an impostor server (`ServerIdentity` refusal). See [[Security]], [[Connecting and approval]].

## Relay

`samples/Relay` — [[NetRelayServer]], a host with a room ([[RelayRoom]]) that keeps its LAN port, a client joining by code; ECDH end to end; the demo prints how many UDP bytes the relay still forwarded after punching (0 on loopback). See [[Relay and NAT traversal]].

## WebSocket

`samples/WebSocket` — one server, a native TCP+UDP client and a WebSocket client without UDP (stands in for a browser), chatting through the same channels; the WebSocket client's UDP channel rides the WebSocket. See [[Browsers and WebSocket]].
