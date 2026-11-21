---
tags: [guide]
---

# Custom transports

`TransportType.Tcp` and `TransportType.Udp` are **slots** with contracts, not socket types:

| Slot | Contract | Built-in implementations |
|---|---|---|
| `Tcp` | reliable, ordered messages | [TcpTransport](../Code/Transports/Tcp/TcpTransport.md), [WebSocketTransportFactory](../Code/Transports/WebSockets/WebSocketTransportFactory.md), relay stream ([RelayStreamTransports](../Code/Relay/RelayStreamTransports.md)) |
| `Udp` | datagrams — may be lost, duplicated, reordered | [UdpTransport](../Code/Transports/Udp/UdpTransport.md), relay datagram ([RelayDatagramTransports](../Code/Relay/RelayDatagramTransports.md)) |

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
4. On the server, create one [TransportPeer](../Code/Transports/TransportPeer.md) per remote link.

Channels, handshake, reliability, compression and security run on top unchanged. A complete public-API example: `tests/NetSync.Tests/InMemoryTransport.cs`.

## Contracts and capabilities

Public contracts: [ITransport](../Code/Transports/ITransport.md), [IServerTransport](../Code/Transports/IServerTransport.md), [ITransportFactory](../Code/Transports/ITransportFactory.md), optional [IPrioritizedTransport](../Code/Transports/IPrioritizedTransport.md). Built-ins additionally implement internal capabilities in [TransportCapabilities](../Code/Transports/TransportCapabilities.md) — zero-copy receive hooks, datagram-boundary reports (for [collapsed acks](../3%20Internals/Reliable%20UDP.md#acks)) and trust-awareness ([Limits and hardening](../3%20Internals/Limits%20and%20hardening.md)). Who implements what: [Type hierarchy](../4%20Diagrams/Type%20hierarchy.md).
