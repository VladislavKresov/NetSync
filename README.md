# NetSync

A low-level networking library for .NET: one connection between a server and its clients over TCP, UDP, WebSocket or a relay, with delivery guarantees, encryption and authentication built in.

Use it for game servers and engines, desktop and mobile apps, services, streaming and file transfer. It moves bytes and typed messages; what they mean is up to your application.

## Why NetSync

- **No dependencies.** A single DLL for `netstandard2.1` and `net8.0`, both speaking the same protocol.
- **TCP and UDP in one connection.** Each channel picks its own transport, delivery mode and priority, so a file transfer doesn't hold up real-time traffic.
- **Reliable UDP.** Five delivery modes, fragmentation of large messages and congestion control.
- **Secure.** Per-packet authentication, AES encryption, key exchange by ECDH or pre-shared key, pinned server identity and server-side connection approval.
- **Connects through difficult networks.** Relay with room codes, UDP hole punching, UDP→TCP fallback, WebSocket and IPv6.
- **Fast.** About 1.2 M UDP messages per second round-trip with no allocation per message (.NET 8, loopback, [full numbers and conditions](docs/5%20Project/Benchmarks%20and%20performance.md)).
- **Common services included.** Typed messages, file streaming, auto-reconnect, shared server clock, LAN discovery, connectionless queries and a network simulator.

## Getting started

```
dotnet add package NetSync --prerelease
```

A server and a client in one program:

```csharp
using System;
using System.Text;
using NetSync;
using NetSync.Peers;

static NetConfig CreateConfig() {
    var config = new NetConfig { EventDelivery = EventDelivery.Immediate };
    config.Channels[0] = new ChannelConfig(TransportType.Tcp);                                      // commands, chat
    config.Channels[1] = new ChannelConfig(TransportType.Udp, ReliabilityMode.UnreliableSequenced); // live state
    return config; // client and server must use the same channels
}

using var server = new NetServer(CreateConfig());
server.DataReceived += (connection, channel, data) => _ = connection.SendAsync(channel, data); // echo
int port = await server.StartAsync(7777);

using var client = new NetClient(CreateConfig());
client.DataReceived += (channel, data) => Console.WriteLine($"Reply: {Encoding.UTF8.GetString(data)}");
await client.ConnectAsync("127.0.0.1", port);
await client.SendAsync(0, Encoding.UTF8.GetBytes("hello"));

Console.ReadLine();
```

`Immediate` raises events on network threads, which suits services and console apps. In a game loop use the default `Polled` delivery and call `PollEvents()` once per frame.

## Next steps

- [Samples](samples): `Echo`, `SecureLogin`, `FileTransfer`, `Relay`, `WebSocket`. Run one with `dotnet run --project samples/Echo -- server 7777`.
- [Documentation](docs/Home.md): guides, protocol and internals.
- [Security guide](docs/2%20Guides/Security.md): the default configuration has no encryption or authentication, so turn them on for anything beyond a trusted network.
- [NetSync for Unity](https://github.com/VladislavKresov/NetSync.Unity): the Unity adapter.

## Status

Release candidate. Wire protocol v3 is not compatible with earlier versions. See [known limitations](docs/5%20Project/Known%20limitations.md).

MIT License.
