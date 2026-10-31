---
tags: [guide]
---

# Quick start

```csharp
using System.Text;
using NetSync;
using NetSync.Peers;

NetConfig MakeConfig() {
    var config = new NetConfig { EventDelivery = EventDelivery.Immediate };
    config.Channels[0] = new ChannelConfig(TransportType.Tcp);                                     // commands, chat
    config.Channels[1] = new ChannelConfig(TransportType.Udp, ReliabilityMode.UnreliableSequenced); // live state
    config.Channels[2] = new ChannelConfig(TransportType.Udp, ReliabilityMode.ReliableOrdered);     // reliable over UDP
    return config; // client and server must use identical channel tables
}

// Server
using var server = new NetServer(MakeConfig());
server.ConnectionOpened += conn => Console.WriteLine($"+ {conn}");
server.MessageReceived += (conn, channel, data) => _ = conn.SendAsync(channel, data); // echo, zero-copy
int port = await server.StartAsync(7777); // 0 = OS picks a port shared by TCP+UDP

// Client
using var client = new NetClient(MakeConfig());
client.DataReceived += (channel, data) => Console.WriteLine($"ch{channel}: {data.Length} B");
await client.ConnectAsync("192.168.0.10", port);
await client.SendAsync(0, Encoding.UTF8.GetBytes("hello"));
```

## What happens

1. `StartAsync` binds one port for TCP and UDP (dual-stack IPv6+IPv4) — [[NetServer]].
2. `ConnectAsync` opens both links, runs the [[Handshake v3]] on each, then joins; the server raises `ConnectionOpened`.
3. Channel 0 goes over TCP, 1 straight over UDP, 2 through the [[ReliableEndpoint]] over UDP — [[Send path]].

## Two ways to receive

| Event | You get | Cost |
|---|---|---|
| `MessageReceived` | the network buffer itself, valid until the handler returns | zero copies |
| `DataReceived` | an owned `byte[]` | one array per message |

Pass `MessageReceived` data straight to `SendAsync` (it copies before returning) or copy it (`data.ToArray()`) to keep it. **Send contract**: `SendAsync` reads your buffer only before it returns — reuse or pool it immediately ([[Receive path]], [[Send path]]).

## Next

[[Channels and delivery modes]] · [[Security]] · [[Threading and events]] (`Polled` vs `Immediate`) · [[Samples]]
