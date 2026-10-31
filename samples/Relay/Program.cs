using System;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NetSync;
using NetSync.Peers;
using NetSync.Relay;

// A player-hosted game behind NAT, reached by room code through a relay.
//   dotnet run                                       demo: relay, host and client in one process
//   dotnet run -- relay 7000                          on a machine with a public address
//   dotnet run -- host <relay-address> 7000           prints the room code; LAN players still connect on 7777
//   dotnet run -- client <relay-address> 7000 <room>  type chat lines
//
// Chat rides the stream slot (always through the relay); positions ride UDP, which tries a direct,
// hole-punched path first. Encryption stays end to end: the relay only forwards envelopes.

const byte Chat = 0;
const byte Positions = 1;

if (args.Length == 0) {
    await DemoAsync();
}
else if (args.Length >= 2 && args[0] == "relay") {
    using var relay = new NetRelayServer();
    await relay.StartAsync(int.Parse(args[1]));
    Console.WriteLine($"Relay on port {args[1]}. Enter to stop.");
    Console.ReadLine();
}
else if (args.Length >= 3 && args[0] == "host") {
    var relay = await ResolveAsync(args[1], int.Parse(args[2]));
    using var host = await StartHostAsync(relay, RelayRoom.NewRoomCode(), lanPort: 7777);
    Console.WriteLine("Enter to stop.");
    Console.ReadLine();
}
else if (args.Length >= 4 && args[0] == "client") {
    using var client = await JoinAsync(await ResolveAsync(args[1], int.Parse(args[2])), args[3]);
    Console.WriteLine("Type chat lines, empty line to quit:");
    while (Console.ReadLine() is { Length: > 0 } line) {
        await client.SendAsync(Chat, Encoding.UTF8.GetBytes(line));
    }
}
else {
    Console.WriteLine("Usage: Relay [relay <port> | host <relay> <port> | client <relay> <port> <room>]");
}

static NetConfig MakeConfig() {
    var config = new NetConfig { EventDelivery = EventDelivery.Immediate };
    config.Channels[Chat] = new ChannelConfig(TransportType.Tcp);
    config.Channels[Positions] = new ChannelConfig(TransportType.Udp, ReliabilityMode.UnreliableSequenced);
    config.Encryption = new EncryptionConfig { Mode = KeyExchangeMode.Ecdh }; // the relay can't read anything
    return config;
}

static async Task<NetServer> StartHostAsync(IPEndPoint relay, string roomId, int lanPort) {
    var host = new NetServer(MakeConfig());
    host.ConnectionOpened += connection => Console.WriteLine($"[host] + {connection}");
    host.MessageReceived += (connection, channel, data) => {
        if (channel == Chat) {
            Console.WriteLine($"[host] {Encoding.UTF8.GetString(data.Span)}");
            _ = host.BroadcastAsync(Chat, data.ToArray()); // BroadcastAsync awaits: give it its own copy
        }
        else {
            _ = connection.SendAsync(Positions, data); // echo
        }
    };
    // The same server keeps its own port for LAN players and adds the relay room as extra listeners.
    var room = new RelayRoom(roomId) { RelayEndPoint = relay };
    host.AddListener(TransportType.Tcp, room.Tcp, 0);
    host.AddListener(TransportType.Udp, room.Udp, 0);
    await host.StartAsync(lanPort);
    Console.WriteLine($"[host] room code: {roomId} (relay {relay}), LAN port {host.Port}");
    return host;
}

static async Task<NetClient> JoinAsync(IPEndPoint relay, string roomId) {
    var config = MakeConfig();
    var room = new RelayRoom(roomId);
    config.TransportFactories[TransportType.Tcp] = room.Tcp;
    config.TransportFactories[TransportType.Udp] = room.Udp;
    var client = new NetClient(config);
    client.MessageReceived += (channel, data) => {
        if (channel == Chat) {
            Console.WriteLine($"[client] {Encoding.UTF8.GetString(data.Span)}");
        }
    };
    await client.ConnectAsync(relay);
    Console.WriteLine($"[client] joined room {roomId} ({client.CipherSuite})");
    return client;
}

static async Task<IPEndPoint> ResolveAsync(string host, int port) {
    var addresses = IPAddress.TryParse(host, out var literal) ? new[] { literal } : await Dns.GetHostAddressesAsync(host);
    return new IPEndPoint(addresses[0], port);
}

static async Task DemoAsync() {
    using var relay = new NetRelayServer();
    var relayEndPoint = new IPEndPoint(IPAddress.Loopback, await relay.StartAsync(0));
    string roomId = RelayRoom.NewRoomCode();
    using var host = await StartHostAsync(relayEndPoint, roomId, lanPort: 0);
    using var client = await JoinAsync(relayEndPoint, roomId);

    foreach (var line in new[] { "hello from behind the relay", "the host broadcasts chat back" }) {
        await client.SendAsync(Chat, Encoding.UTF8.GetBytes(line));
    }
    await Task.Delay(1500); // hole punching runs in the background after connecting

    long udpBefore = relay.RelayedUdpBytes;
    for (int i = 0; i < 200; i++) {
        await client.SendAsync(Positions, new byte[100]);
        await Task.Delay(2);
    }
    await Task.Delay(200);
    Console.WriteLine($"Relay forwarded {relay.RelayedTcpBytes} B of stream traffic and " +
                      $"{relay.RelayedUdpBytes - udpBefore} B of the last 40 KB of UDP (0 = the hole-punched path carried it).");
}
