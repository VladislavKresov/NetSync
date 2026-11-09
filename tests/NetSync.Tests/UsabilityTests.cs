using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using NetSync.Messaging;
using NetSync.Peers;
using NetSync.Serialization;
using NetSync.Transports;
using Xunit;

namespace NetSync.Tests {
    /// <summary>Stage 14: typed messages, statistics, Tag, reconnect, IPv6.</summary>
    public class UsabilityTests {
        private sealed class ChatMessage : INetMessage {
            public string Author = "";
            public string Text = "";
            public void Write(NetWriter writer) => writer.WriteString(Author).WriteString(Text);
            public void Read(NetReader reader) {
                Author = reader.ReadString();
                Text = reader.ReadString();
            }
        }

        private struct Move : INetMessage {
            public int PlayerId;
            public float X, Y;
            public void Write(NetWriter writer) => writer.WriteVarInt(PlayerId).WriteSingle(X).WriteSingle(Y);
            public void Read(NetReader reader) {
                PlayerId = reader.ReadVarInt();
                X = reader.ReadSingle();
                Y = reader.ReadSingle();
            }
        }

        private static NetMessageRegistry Registry() => new NetMessageRegistry().Register<ChatMessage>(1).Register<Move>(2);

        private static NetConfig MakeConfig() {
            var config = new NetConfig { EventDelivery = EventDelivery.Immediate, PingIntervalMs = 200, ConnectTimeoutMs = 5000 };
            config.Channels[0] = new ChannelConfig(TransportType.Tcp);
            config.Channels[1] = new ChannelConfig(TransportType.Udp, ReliabilityMode.UnreliableSequenced);
            return config;
        }

        [Fact]
        public async Task Typed_Messages_Roundtrip_Through_Messengers() {
            using var server = new NetServer(MakeConfig());
            var serverMessenger = new NetServerMessenger(server, Registry());
            serverMessenger.On<ChatMessage>((conn, chat) => _ = serverMessenger.BroadcastAsync(0, chat));
            serverMessenger.On<Move>((conn, move) => _ = serverMessenger.SendAsync(conn, 1, move));
            int port = await server.StartAsync(0);

            using var client = new NetClient(MakeConfig());
            var messenger = new NetClientMessenger(client, Registry());
            var chatEcho = new TaskCompletionSource<ChatMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
            var moveEcho = new TaskCompletionSource<Move>(TaskCreationOptions.RunContinuationsAsynchronously);
            messenger.On<ChatMessage>(chat => chatEcho.TrySetResult(chat));
            messenger.On<Move>(move => moveEcho.TrySetResult(move));
            await client.ConnectAsync("127.0.0.1", port);

            await messenger.SendAsync(0, new ChatMessage { Author = "Anna", Text = "привет" });
            await messenger.SendAsync(1, new Move { PlayerId = -7, X = 1.5f, Y = -2.25f });

            var chat = await WithTimeout(chatEcho.Task);
            Assert.Equal("Anna", chat.Author);
            Assert.Equal("привет", chat.Text);
            var move = await WithTimeout(moveEcho.Task);
            Assert.Equal(-7, move.PlayerId);
            Assert.Equal(1.5f, move.X);
            Assert.Equal(-2.25f, move.Y);
            await server.StopAsync();
        }

        [Fact]
        public void Registry_Rejects_Duplicate_Ids_And_Types() {
            var registry = new NetMessageRegistry().Register<ChatMessage>(1);
            Assert.Throws<ArgumentException>(() => registry.Register<Move>(1));
            Assert.Throws<ArgumentException>(() => registry.Register<ChatMessage>(5));
            Assert.Throws<InvalidOperationException>(() => new NetMessageRegistry().Serialize(new Move()));
        }

        [Fact]
        public async Task Unknown_And_Malformed_Messages_Are_Reported_Not_Thrown() {
            using var server = new NetServer(MakeConfig());
            var messenger = new NetServerMessenger(server, Registry());
            var unknown = new TaskCompletionSource<uint>(TaskCreationOptions.RunContinuationsAsynchronously);
            var malformed = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            messenger.On<ChatMessage>((_, _) => { });
            messenger.UnknownMessage += (_, id) => unknown.TrySetResult(id);
            messenger.MalformedMessage += (_, _) => malformed.TrySetResult(true);
            int port = await server.StartAsync(0);

            using var client = new NetClient(MakeConfig());
            await client.ConnectAsync("127.0.0.1", port);
            await client.SendAsync(0, new NetWriter().WriteVarUInt(999).ToArray());          // id nobody registered
            await client.SendAsync(0, new NetWriter().WriteVarUInt(1).WriteVarUInt(50).ToArray()); // chat with a truncated string

            Assert.Equal(999u, await WithTimeout(unknown.Task));
            Assert.True(await WithTimeout(malformed.Task));
            Assert.True(client.IsConnected); // a bad message never takes the connection down
            await server.StopAsync();
        }

        [Fact]
        public async Task Statistics_Tag_And_Connection_List() {
            using var server = new NetServer(MakeConfig());
            server.ConnectionOpened += conn => conn.Tag = $"player-{conn.Id}";
            server.DataReceived += (conn, channel, data) => _ = conn.SendAsync(channel, data);
            int port = await server.StartAsync(0);

            using var client = new NetClient(MakeConfig());
            int echoes = 0;
            client.DataReceived += (_, _) => Interlocked.Increment(ref echoes);
            await client.ConnectAsync("127.0.0.1", port);
            for (int i = 0; i < 10; i++) {
                await client.SendAsync(0, new byte[100]);
            }
            await WaitUntilAsync(() => Volatile.Read(ref echoes) == 10);

            var connection = Assert.Single(server.Connections);
            Assert.Equal($"player-{connection.Id}", connection.Tag);
            Assert.True(connection.IsConnected);
            Assert.Equal(10, connection.Statistics.MessagesReceived);
            Assert.Equal(1000, connection.Statistics.BytesReceived);

            var stats = client.Statistics;
            Assert.Equal(10, stats.MessagesSent);
            Assert.Equal(10, stats.MessagesReceived);
            Assert.Equal(1000, stats.BytesSent);
            Assert.False(stats.IsDegraded);
            await server.StopAsync();
        }

        [Fact]
        public async Task Client_Reconnects_After_Server_Restart() {
            var config = MakeConfig();
            config.PingTimeoutMs = 1000;
            config.Reconnect = new ReconnectPolicy { InitialDelayMs = 100, MaxDelayMs = 300, MaxAttempts = 50 };

            var server = new NetServer(MakeConfig());
            int port = await server.StartAsync(0);

            using var client = new NetClient(config);
            var connectedCount = 0;
            var reconnecting = 0;
            var disconnected = new TaskCompletionSource<DisconnectReason>(TaskCreationOptions.RunContinuationsAsynchronously);
            client.Connected += () => Interlocked.Increment(ref connectedCount);
            client.Reconnecting += _ => Interlocked.Increment(ref reconnecting);
            client.Disconnected += reason => disconnected.TrySetResult(reason);
            await client.ConnectAsync("127.0.0.1", port);
            long firstId = client.ConnectionId;

            await server.StopAsync();          // server goes away…
            server.Dispose();
            Assert.Equal(DisconnectReason.Remote, await WithTimeout(disconnected.Task));
            Assert.True(await Eventually.HoldsAsync(() => Volatile.Read(ref reconnecting) >= 1)); // …a few attempts fail meanwhile…

            using var restarted = new NetServer(MakeConfig()); // …and comes back on the same port
            restarted.DataReceived += (conn, channel, data) => _ = conn.SendAsync(channel, data);
            await restarted.StartAsync(port);

            await WaitUntilAsync(() => Volatile.Read(ref connectedCount) == 2, 10_000);
            Assert.True(client.IsConnected);
            _ = firstId; // ids restart with a new server instance; the echo below proves the new session works

            var echo = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            client.DataReceived += (_, _) => echo.TrySetResult(true);
            await client.SendAsync(0, new byte[] { 1 });
            Assert.True(await WithTimeout(echo.Task));
            await restarted.StopAsync();
        }

        [Fact]
        public async Task Kicked_Client_Does_Not_Reconnect() {
            var config = MakeConfig();
            config.Reconnect = new ReconnectPolicy { InitialDelayMs = 50, MaxAttempts = 5 };
            using var server = new NetServer(MakeConfig());
            var opened = new TaskCompletionSource<NetConnection>(TaskCreationOptions.RunContinuationsAsynchronously);
            int openedCount = 0;
            server.ConnectionOpened += conn => {
                Interlocked.Increment(ref openedCount);
                opened.TrySetResult(conn);
            };
            int port = await server.StartAsync(0);

            using var client = new NetClient(config);
            var reason = new TaskCompletionSource<DisconnectReason>(TaskCreationOptions.RunContinuationsAsynchronously);
            client.Disconnected += r => reason.TrySetResult(r);
            await client.ConnectAsync("127.0.0.1", port);

            await (await WithTimeout(opened.Task)).DisconnectAsync();
            Assert.Equal(DisconnectReason.Kicked, await WithTimeout(reason.Task));
            await Task.Delay(500);
            Assert.Equal(1, Volatile.Read(ref openedCount));
            Assert.False(client.IsConnected);
            await server.StopAsync();
        }

        [Fact]
        public async Task Server_Accepts_IPv6_And_IPv4_Clients() {
            using var server = new NetServer(MakeConfig());
            server.DataReceived += (conn, channel, data) => _ = conn.SendAsync(channel, data);
            int port = await server.StartAsync(0);

            // IPv4 through the dual-stack listener always; IPv6 only where the machine
            // actually allows IPv6 loopback (some sandboxes and CI runners forbid it).
            var addresses = CanUseIPv6Loopback() ? new[] { "::1", "127.0.0.1" } : new[] { "127.0.0.1" };
            foreach (var address in addresses) {
                using var client = new NetClient(MakeConfig());
                var echo = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
                client.DataReceived += (_, _) => echo.TrySetResult(true);
                await client.ConnectAsync(address, port);
                await client.SendAsync(0, new byte[] { 7 });
                await client.SendAsync(1, new byte[] { 8 });
                Assert.True(await WithTimeout(echo.Task));
            }
            // IPv4 clients are presented as plain IPv4, not ::ffff:127.0.0.1
            Assert.DoesNotContain(server.Connections, c => c.GetEndPoint(TransportType.Tcp)!.Address.IsIPv4MappedToIPv6);
            await server.StopAsync();
        }

        private static bool CanUseIPv6Loopback() {
            if (!Socket.OSSupportsIPv6) {
                return false;
            }
            try {
                var listener = new TcpListener(IPAddress.IPv6Loopback, 0);
                listener.Start();
                try {
                    using var probe = new TcpClient(AddressFamily.InterNetworkV6);
                    probe.Connect(IPAddress.IPv6Loopback, ((IPEndPoint)listener.LocalEndpoint).Port);
                    return true;
                }
                finally {
                    listener.Stop();
                }
            }
            catch (SocketException) {
                return false;
            }
        }

        [Fact]
        public void Ecdh_Is_Available_On_This_Runtime() {
            // The probe guards Unity/Mono runtimes that lack P-256; on .NET 8 it must pass.
            Assert.True(Pipeline.Security.EcdhKeyExchange.IsSupported);
        }

        private static async Task<T> WithTimeout<T>(Task<T> task, int timeoutMs = 5000) {
            var completed = await Task.WhenAny(task, Task.Delay(timeoutMs));
            Assert.Same(task, completed);
            return await task;
        }

        private static async Task WaitUntilAsync(Func<bool> condition, int timeoutMs = 5000) {
            var deadline = DateTime.UtcNow.AddMilliseconds(timeoutMs);
            while (!condition()) {
                if (DateTime.UtcNow > deadline) {
                    throw new TimeoutException("Condition not met in time");
                }
                await Task.Delay(10);
            }
        }
    }
}
