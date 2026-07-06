using System;
using System.Text;
using System.Threading.Tasks;
using NetSync;
using NetSync.Diagnostics;
using NetSync.Peers;

// Console demo of the high-level peer API: one server, one client, two channels
// (0 = TCP, 1 = UDP) over a single port.
//   dotnet run -- server 7777
//   dotnet run -- client 7777 tcp
//   dotnet run -- client 7777 udp

var logger = ConsoleNetLogger.Instance;
logger.MinLevel = NetLogLevel.Debug;

if (args.Length < 2 || !int.TryParse(args[1], out int port)) {
    Console.WriteLine("Usage: Echo <server|client> <port> [tcp|udp]");
    return;
}

static NetConfig MakeConfig(INetLogger logger) {
    var config = new NetConfig {
        EventDelivery = EventDelivery.Immediate, // console app: no game loop to poll from
        Logger = logger
    };
    config.Channels[0] = new ChannelConfig(TransportType.Tcp);
    config.Channels[1] = new ChannelConfig(TransportType.Udp);
    return config;
}

if (args[0] == "server") {
    using var server = new NetServer(MakeConfig(logger));
    server.ConnectionOpened += conn => Console.WriteLine($"+ {conn}");
    server.ConnectionClosed += (conn, reason) => Console.WriteLine($"- {conn} ({reason})");
    server.DataReceived += (conn, channel, data) => {
        Console.WriteLine($"[ch{channel}] {conn.GetEndPoint(channel == 0 ? TransportType.Tcp : TransportType.Udp)}: {Encoding.UTF8.GetString(data)}");
        _ = conn.SendAsync(channel, data); // echo back on the same channel
    };

    await server.StartAsync(port);
    Console.WriteLine($"Echo server on port {port} (channel 0 = TCP, channel 1 = UDP). Press Enter to stop.");
    Console.ReadLine();
    await server.StopAsync();
}
else {
    byte channel = args.Length > 2 && args[2] == "udp" ? (byte)1 : (byte)0;

    using var client = new NetClient(MakeConfig(logger));
    client.Connected += () => Console.WriteLine($"Connected, id={client.ConnectionId}.");
    client.Disconnected += reason => Console.WriteLine($"Disconnected: {reason}");
    client.DataReceived += (ch, data) => Console.WriteLine($"echo ch{ch}> {Encoding.UTF8.GetString(data)} (ping {client.PingMs}ms)");

    await client.ConnectAsync("127.0.0.1", port);
    Console.WriteLine($"Type messages (channel {channel}), empty line to quit:");

    while (Console.ReadLine() is { Length: > 0 } line) {
        await client.SendAsync(channel, Encoding.UTF8.GetBytes(line));
    }
    client.Disconnect();
}
