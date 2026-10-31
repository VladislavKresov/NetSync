using System;
using System.Text;
using System.Threading.Tasks;
using NetSync;
using NetSync.Peers;
using NetSync.Transports;
using NetSync.Transports.WebSockets;

// One server, two kinds of clients: native ones on TCP+UDP, and WebSocket ones — browsers
// (Blazor WebAssembly today, a JavaScript client later) and networks that only let HTTP through.
//   dotnet run                                demo: a native and a WebSocket client chat through one server
//   dotnet run -- server 7777 8080            native on 7777, WebSocket on ws://*:8080/netsync
//   dotnet run -- client <host> 7777          native client
//   dotnet run -- client <host> 8080 ws       WebSocket client
//
// Both kinds land on the same connections, channels and events. A WebSocket client without UDP
// carries its UDP channels over the WebSocket — the channel table stays the same everywhere.

const byte Chat = 0;
const byte State = 1;

if (args.Length == 0) {
    await DemoAsync();
}
else if (args.Length >= 3 && args[0] == "server") {
    var (server, _) = await StartServerAsync(int.Parse(args[1]), int.Parse(args[2]));
    using (server) {
        Console.WriteLine("Enter to stop.");
        Console.ReadLine();
    }
}
else if (args.Length >= 3 && args[0] == "client") {
    using var client = await ConnectAsync(args[1], int.Parse(args[2]), webSocket: args.Length > 3 && args[3] == "ws", "you");
    Console.WriteLine("Type chat lines, empty line to quit:");
    while (Console.ReadLine() is { Length: > 0 } line) {
        await client.SendAsync(Chat, Encoding.UTF8.GetBytes(line));
    }
}
else {
    Console.WriteLine("Usage: WebSocket [server <port> <wsPort> | client <host> <port> [ws]]");
}

static NetConfig MakeConfig() {
    var config = new NetConfig { EventDelivery = EventDelivery.Immediate };
    config.Channels[Chat] = new ChannelConfig(TransportType.Tcp);
    config.Channels[State] = new ChannelConfig(TransportType.Udp, ReliabilityMode.UnreliableSequenced);
    return config;
}

static async Task<(NetServer Server, int WebSocketPort)> StartServerAsync(int port, int webSocketPort) {
    var server = new NetServer(MakeConfig());
    server.ConnectionOpened += connection =>
        Console.WriteLine($"[server] + {connection}{(connection.IsDegraded ? " — no UDP: its UDP channels ride the WebSocket" : "")}");
    server.MessageReceived += (connection, channel, data) => {
        if (channel == Chat) {
            _ = server.BroadcastAsync(Chat, data.ToArray()); // BroadcastAsync awaits: give it its own copy
        }
        else {
            Console.WriteLine($"[server] state from Connection#{connection.Id}: {Encoding.UTF8.GetString(data.Span)}");
        }
    };
    // Production: set ServerCertificate for wss:// and list the pages allowed to connect.
    var webSocket = new WebSocketTransportFactory { Path = "/netsync" };
    // webSocket.AllowedOrigins.Add("https://game.example.com");
    var listener = server.AddListener(TransportType.Tcp, webSocket, webSocketPort);
    await server.StartAsync(port);
    Console.WriteLine($"[server] native clients on port {server.Port}, WebSocket on ws://localhost:{listener.Port}/netsync");
    return (server, listener.Port);
}

static async Task<NetClient> ConnectAsync(string host, int port, bool webSocket, string name) {
    var config = MakeConfig();
    if (webSocket) {
        config.TransportFactories[TransportType.Tcp] = new WebSocketTransportFactory { Path = "/netsync" };
        config.TransportFactories[TransportType.Udp] = new NoUdp(); // stands in for a browser: no UDP sockets there
    }
    var client = new NetClient(config);
    client.MessageReceived += (channel, data) => Console.WriteLine($"[{name}] {Encoding.UTF8.GetString(data.Span)}");
    await client.ConnectAsync(host, port);
    Console.WriteLine($"[{name}] connected over {(webSocket ? "WebSocket" : "TCP+UDP")}{(client.IsDegraded ? ", UDP channels over the WebSocket" : "")}");
    return client;
}

static async Task DemoAsync() {
    var (server, webSocketPort) = await StartServerAsync(0, 0);
    using var serverScope = server;
    using var native = await ConnectAsync("127.0.0.1", server.Port, webSocket: false, "native");
    using var browser = await ConnectAsync("127.0.0.1", webSocketPort, webSocket: true, "websocket");
    await native.SendAsync(Chat, Encoding.UTF8.GetBytes("hi from a native client"));
    await browser.SendAsync(Chat, Encoding.UTF8.GetBytes("hi from a WebSocket client"));
    await browser.SendAsync(State, Encoding.UTF8.GetBytes("a UDP channel, carried over the WebSocket"));
    await Task.Delay(500);
}

/// <summary>A runtime without UDP sockets (a browser): creating the UDP link fails, the client carries UDP over the stream link.</summary>
sealed class NoUdp : ITransportFactory {
    public ITransport CreateClient(TransportSettings settings) => throw new PlatformNotSupportedException("no UDP sockets here");
    public IServerTransport CreateServer(TransportSettings settings) => throw new NotSupportedException();
}
