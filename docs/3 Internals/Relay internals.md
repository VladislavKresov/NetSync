---
tags: [internals, diagram]
---

# Relay internals

## Registration and hole punching

```mermaid
sequenceDiagram
  autonumber
  participant H as Host (behind NAT A)
  participant R as NetRelayServer
  participant C as Client (behind NAT B)
  H->>R: TCP HostHello [room]
  R-->>H: HostWelcome [secret]
  H->>R: UDP HostRegister [room, secret] (repeated every 5 s: keeps NAT A open)
  R-->>H: HostRegistered
  C->>R: TCP ClientHello [room] (stream slot)
  R-->>H: PeerOpen [client id]
  C->>R: UDP ClientRegister [room, nonce]
  R-->>C: ClientRegistered [client id, host public endpoint]
  R->>H: Introduce [client id, nonce, client public endpoint]
  par both sides punch
    C->>H: Punch [id, nonce] to host public endpoint
  and
    H->>C: Punch [id, nonce] to client public endpoint
  end
  Note over C,H: first datagram straight from the other side proves the path
  C->>H: DirectData [id, nonce, packet]
  H->>C: DirectData [packet]
  Note over C,H: until then (and when the direct path goes quiet): RelayData via the relay
```

- The nonce, introduced by the relay, authenticates direct packets: nobody else can inject data as that client. The host accepts peer-reflexive endpoints (the address the punch really came from).
- Two symmetric NATs can't be punched: UDP stays on the relay, still end to end encrypted.
- The stream slot always goes through the relay: PeerOpen / PeerData / PeerClose multiplex every stream client over the host's single control connection ([[RelayHostSession]], [[FramedConnection]]).
- **Relay restart**: when the host's control connection drops while the room is in use, [[RelayRoom]] reconnects with backoff (0.5 → 30 s) and raises `HostSessionRestored`; the host transports re-attach, the UDP side re-registers under the new secret and drops client records (ids restart on a new relay).

## Allocation-free forwarding

The relay and its transports receive into a reused `SocketAddress` and send to cached ones ([[EndPointCache]]): no allocation per datagram on net8.0 (netstandard2.1 lacks the APIs).

## Testing without real NATs

[[DatagramSocket]] is the seam: tests substitute port-restricted cone and symmetric NAT simulators and assert bytes through the relay (0 B with cone NATs after punching, everything with symmetric ones) — [[Tests]].

Modules: [[NetRelayServer]], [[RelayRoom]], [[RelayHostSession]], [[RelayStreamTransports]], [[RelayDatagramTransports]], [[RelayProtocol]], [[FramedConnection]], [[DatagramSocket]]. Guide: [[Relay and NAT traversal]].
