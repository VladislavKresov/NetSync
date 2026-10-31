---
tags: [guide]
---

# Session services

## Server clock on the client

```csharp
double serverNow = client.ServerTimeMs;       // NaN until the first sync; compare with server.TimeMs
double error = client.ServerTimeAccuracyMs;   // worst case: half the best round trip
```

NTP-style: five quick round trips after joining, then one every 2 s; the estimate uses the minimum-RTT sample of the last eight, so a congested round trip can't skew it ([[ClockSync]], `MsgTimeRequest`/`MsgTimeResponse` in [[PeerProtocol]]). The server answers on the receive thread, prefers the UDP link. Measured error on loopback ≈ 0.001 ms.

## Connectionless queries (server browsers)

```csharp
server.QueryHandler = (from, request) => Encoding.UTF8.GetBytes($"{name}|{map}|{server.ConnectionCount}");
var result = await NetQuery.QueryAsync(serverEndPoint, ReadOnlyMemory<byte>.Empty, maxResponseSize: 512);
```

Safe on the open internet: a reply is never larger than the query (clients pad their queries), answers are rate-limited (1000/s token bucket), the handler runs on the UDP receive thread — keep it fast. [[NetQuery]], [[UdpServerTransport]].

## LAN discovery

```csharp
using var broadcaster = new LanServerBroadcaster("MyGame", serverPort: port) { Payload = Encoding.UTF8.GetBytes("Anna's game") };
broadcaster.Start();
using var listener = new LanServerListener("MyGame");
listener.ServerDiscovered += found => Console.WriteLine($"{found.EndPoint}: {Encoding.UTF8.GetString(found.Payload)}");
listener.Start();
```

Broadcast from every LAN interface every 2 s (falls back to an OS-chosen interface when none is recognised). [[LanServerBroadcaster]], [[LanServerListener]], [[DiscoveryMessage]].

## Path MTU

After connecting, the client probes the path with don't-fragment datagrams (IPv4) and tells the server (`MsgPathMtu`); reliable UDP then uses fragments up to 1453 B instead of 1100 B. `NetConfig.MtuDiscovery` (on by default). [[PathMtuProbe]], [[Reliable UDP]].

## Network simulator

```csharp
config.Simulation = new NetworkSimulation { LatencyMs = 80, JitterMs = 20, LossRate = 0.05, DuplicateRate = 0.01, Seed = 42 };
```

Wraps every link of that peer (datagram links get loss/duplication, stream links keep order). [[NetworkSimulation]], [[SimulatedTransportFactory]].
