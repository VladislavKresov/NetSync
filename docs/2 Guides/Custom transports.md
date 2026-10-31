---
tags: [guide]
---

# Custom transports

`TransportType.Tcp` and `TransportType.Udp` are **slots** with contracts, not socket types:

| Slot | Contract | Built-in implementations |
|---|---|---|
| `Tcp` | reliable, ordered messages | [[TcpTransport]], [[WebSocketTransportFactory]], relay stream ([[RelayStreamTransports]]) |
| `Udp` | datagrams — may be lost, duplicated, reordered | [[UdpTransport]], relay datagram ([[RelayDatagramTransports]]) |

```csharp
public sealed class MyTransportFactory : ITransportFactory {
    public ITransport CreateClient(TransportSettings settings) => new MyClientTransport(settings);
    public IServerTransport CreateServer(TransportSettings settings) => new MyServerTransport(settings);
}
config.TransportFactories[TransportType.Tcp] = new MyTransportFactory();   // a client/server slot
server.AddListener(TransportType.Tcp, new MyTransportFactory(), 9000);     // or an extra server listener
```

## Requirements

1. Preserve message boundaries — one send is one receive.
2. Raise each link's events from one thread at a time.
3. Read the caller's memory only before `SendAsync` returns (callers pool their buffers).
4. On the server, create one [[TransportPeer]] per remote link.

Channels, handshake, reliability, compression and security run on top unchanged. A complete public-API example: `tests/NetSync.Tests/InMemoryTransport.cs`.

## Contracts and capabilities

Public contracts: [[ITransport]], [[IServerTransport]], [[ITransportFactory]], optional [[IPrioritizedTransport]]. Built-ins additionally implement internal capabilities in [[TransportCapabilities]] — zero-copy receive hooks, datagram-boundary reports (for [[Reliable UDP#Acks|collapsed acks]]) and trust-awareness ([[Limits and hardening]]). Who implements what: [[Type hierarchy]].
