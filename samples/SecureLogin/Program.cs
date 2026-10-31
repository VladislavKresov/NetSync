using System;
using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NetSync;
using NetSync.Peers;

// A login over the open internet: the client knows it talks to the real server, the password
// never crosses the network in clear, and the server decides who gets in.
//   dotnet run                                    demo: a good login, a wrong password, an impostor server
//   dotnet run -- server 7777                     creates server.key / server.pub on first run
//   dotnet run -- client 7777 alice secret [host] needs server.pub next to it
//
// - ECDH key exchange + a server identity (NetIdentity): the server signs every handshake,
//   the client accepts only the pinned public key — no man-in-the-middle.
// - The login travels as connect data, encrypted with the session keys.
// - ConnectionApprover checks it before the connection opens; refusals reach the client.

if (args.Length == 0) {
    await DemoAsync();
}
else if (args.Length >= 2 && args[0] == "server") {
    using var identity = LoadOrCreateIdentity("server.key", "server.pub");
    using var server = StartServer(identity);
    await server.StartAsync(int.Parse(args[1]));
    Console.WriteLine($"Server {identity.Fingerprint} on port {args[1]}; give clients server.pub. Enter to stop.");
    Console.ReadLine();
}
else if (args.Length >= 4 && args[0] == "client") {
    var pinned = File.ReadAllBytes("server.pub");
    await LoginAsync(args.Length > 4 ? args[4] : "127.0.0.1", int.Parse(args[1]), args[2], args[3], pinned);
}
else {
    Console.WriteLine("Usage: SecureLogin [server <port> | client <port> <user> <password> [host]]");
}

static NetConfig MakeConfig() {
    var config = new NetConfig { EventDelivery = EventDelivery.Immediate };
    config.Channels[0] = new ChannelConfig(TransportType.Tcp, encryption: true);   // account data: confidential
    config.Channels[1] = new ChannelConfig(TransportType.Udp, ReliabilityMode.UnreliableSequenced); // authenticated only
    config.Encryption.Mode = KeyExchangeMode.Ecdh;
    return config;
}

static NetServer StartServer(NetIdentity identity) {
    var config = MakeConfig();
    config.Encryption.ServerIdentity = identity;
    config.MaxConnections = 100;
    var accounts = new Dictionary<string, string> { ["alice"] = "secret", ["bob"] = "hunter2" }; // a database in real life

    var server = new NetServer(config);
    server.ConnectionApprover = request => {
        // "user:password", decrypted for us; only valid during this call — copy what you keep.
        var login = Encoding.UTF8.GetString(request.ConnectData.Span).Split(':', 2);
        if (login.Length != 2 || !accounts.TryGetValue(login[0], out var password) ||
            !CryptographicOperations.FixedTimeEquals(Encoding.UTF8.GetBytes(password), Encoding.UTF8.GetBytes(login[1]))) {
            return ConnectionDecision.Reject("invalid user or password");
        }
        request.Connection.Tag = login[0];
        return ConnectionDecision.Accept();
    };
    server.ConnectionOpened += connection => {
        Console.WriteLine($"[server] {connection.Tag} logged in ({connection.CipherSuite})");
        _ = connection.SendAsync(0, Encoding.UTF8.GetBytes($"Welcome, {connection.Tag}!"));
    };
    return server;
}

static async Task LoginAsync(string host, int port, string user, string password, byte[] pinnedServerKey) {
    var config = MakeConfig();
    config.Encryption.TrustedServerKeys.Add(pinnedServerKey);
    using var client = new NetClient(config);
    var greeting = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
    client.DataReceived += (channel, data) => greeting.TrySetResult(Encoding.UTF8.GetString(data));
    try {
        await client.ConnectAsync(host, port, Encoding.UTF8.GetBytes($"{user}:{password}"));
        Console.WriteLine($"[client] {user}: connected to {NetIdentity.GetFingerprint(client.ServerIdentityKey!)} " +
                          $"({client.CipherSuite}) — server says \"{await greeting.Task.WaitAsync(TimeSpan.FromSeconds(5))}\"");
        client.Disconnect();
    }
    catch (ConnectionRejectedException ex) {
        Console.WriteLine($"[client] {user}: refused — {ex.Rejection}: {ex.Reason}");
    }
}

static NetIdentity LoadOrCreateIdentity(string keyFile, string publicFile) {
    if (File.Exists(keyFile)) {
        return NetIdentity.FromPrivateKey(File.ReadAllBytes(keyFile));
    }
    var identity = NetIdentity.Generate();
    File.WriteAllBytes(keyFile, identity.ExportPrivateKey()); // keep it secret: a vault or secret store in production
    File.WriteAllBytes(publicFile, identity.PublicKey);
    return identity;
}

static async Task DemoAsync() {
    using var identity = NetIdentity.Generate();
    using var server = StartServer(identity);
    int port = await server.StartAsync(0);
    Console.WriteLine($"Server identity {identity.Fingerprint}, pinned by the clients.");

    await LoginAsync("127.0.0.1", port, "alice", "secret", identity.PublicKey);
    await LoginAsync("127.0.0.1", port, "alice", "guess", identity.PublicKey);

    // Someone else answers at an address the client trusts (DNS spoofing, a rogue Wi-Fi hotspot):
    // their own identity can't produce the pinned key's signature.
    using var impostorIdentity = NetIdentity.Generate();
    using var impostor = StartServer(impostorIdentity);
    int impostorPort = await impostor.StartAsync(0);
    await LoginAsync("127.0.0.1", impostorPort, "alice", "secret", identity.PublicKey);

    await impostor.StopAsync();
    await server.StopAsync();
}
