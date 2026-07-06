using System;
using System.Threading;
using System.Threading.Tasks;
using NetSync.Peers;
using NetSync.Transports;
using Xunit;

namespace NetSync.Tests {
    public class PeerTests {
        private static NetConfig MakeConfig(EventDelivery delivery = EventDelivery.Immediate) {
            var config = new NetConfig {
                EventDelivery = delivery,
                PingIntervalMs = 200,
                PingTimeoutMs = 3000,
                ConnectTimeoutMs = 5000
            };
            config.Channels[0] = new ChannelConfig(TransportType.Tcp);
            config.Channels[1] = new ChannelConfig(TransportType.Udp);
            return config;
        }

        [Fact]
        public async Task Client_Connects_Over_Tcp_And_Udp_As_Single_Connection() {
            using var server = new NetServer(MakeConfig());
            int opened = 0;
            server.ConnectionOpened += _ => Interlocked.Increment(ref opened);
            int port = await server.StartAsync(0);

            using var client = new NetClient(MakeConfig());
            await client.ConnectAsync("127.0.0.1", port);

            Assert.True(client.IsConnected);
            Assert.True(client.ConnectionId > 0);
            await WaitUntilAsync(() => Volatile.Read(ref opened) == 1);
            Assert.Equal(1, server.ConnectionCount);
            await server.StopAsync();
        }

        [Fact]
        public async Task Data_Routes_By_Channel_Both_Directions() {
            using var server = new NetServer(MakeConfig());
            server.DataReceived += (conn, channel, data) => _ = conn.SendAsync(channel, data); // echo
            int port = await server.StartAsync(0);

            using var client = new NetClient(MakeConfig());
            var tcpEcho = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
            var udpEcho = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
            client.DataReceived += (channel, data) => {
                if (channel == 0) tcpEcho.TrySetResult(data);
                if (channel == 1) udpEcho.TrySetResult(data);
            };

            await client.ConnectAsync("127.0.0.1", port);
            await client.SendAsync(0, new byte[] { 1, 2, 3 });
            await client.SendAsync(1, new byte[] { 4, 5, 6 });

            Assert.Equal(new byte[] { 1, 2, 3 }, await WithTimeout(tcpEcho.Task));
            Assert.Equal(new byte[] { 4, 5, 6 }, await WithTimeout(udpEcho.Task));
            await server.StopAsync();
        }

        [Fact]
        public async Task Large_Payload_Over_Tcp_Channel_Roundtrips() {
            using var server = new NetServer(MakeConfig());
            server.DataReceived += (conn, channel, data) => _ = conn.SendAsync(channel, data);
            int port = await server.StartAsync(0);

            using var client = new NetClient(MakeConfig());
            var echo = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
            client.DataReceived += (channel, data) => echo.TrySetResult(data);
            await client.ConnectAsync("127.0.0.1", port);

            var payload = new byte[200_000];
            new Random(3).NextBytes(payload);
            await client.SendAsync(0, payload);

            Assert.Equal(payload, await WithTimeout(echo.Task, 15000));
            await server.StopAsync();
        }

        [Fact]
        public async Task Client_Disconnect_Closes_Connection_On_Server() {
            using var server = new NetServer(MakeConfig());
            var closed = new TaskCompletionSource<DisconnectReason>(TaskCreationOptions.RunContinuationsAsynchronously);
            server.ConnectionClosed += (conn, reason) => closed.TrySetResult(reason);
            int port = await server.StartAsync(0);

            using var client = new NetClient(MakeConfig());
            await client.ConnectAsync("127.0.0.1", port);
            client.Disconnect();
            Assert.False(client.IsConnected);

            var reason = await WithTimeout(closed.Task);
            Assert.Equal(DisconnectReason.Remote, reason);
            Assert.Equal(0, server.ConnectionCount);
            await server.StopAsync();
        }

        [Fact]
        public async Task Server_Kick_Disconnects_Client() {
            using var server = new NetServer(MakeConfig());
            var opened = new TaskCompletionSource<NetConnection>(TaskCreationOptions.RunContinuationsAsynchronously);
            server.ConnectionOpened += conn => opened.TrySetResult(conn);
            int port = await server.StartAsync(0);

            using var client = new NetClient(MakeConfig());
            var disconnected = new TaskCompletionSource<DisconnectReason>(TaskCreationOptions.RunContinuationsAsynchronously);
            client.Disconnected += reason => disconnected.TrySetResult(reason);
            await client.ConnectAsync("127.0.0.1", port);

            var connection = await WithTimeout(opened.Task);
            await connection.DisconnectAsync();

            var reason = await WithTimeout(disconnected.Task);
            Assert.Equal(DisconnectReason.Remote, reason);
            await server.StopAsync();
        }

        [Fact]
        public async Task Polled_Mode_Delivers_Events_Only_On_Poll() {
            using var server = new NetServer(MakeConfig());
            server.DataReceived += (conn, channel, data) => _ = conn.SendAsync(channel, data);
            int port = await server.StartAsync(0);

            using var client = new NetClient(MakeConfig(EventDelivery.Polled));
            int events = 0;
            client.Connected += () => Interlocked.Increment(ref events);
            client.DataReceived += (_, _) => Interlocked.Increment(ref events);

            await client.ConnectAsync("127.0.0.1", port);
            await client.SendAsync(0, new byte[] { 42 });
            await Task.Delay(500); // echo definitely arrived by now

            Assert.Equal(0, Volatile.Read(ref events)); // nothing until we poll
            int handled = client.PollEvents();
            Assert.True(handled >= 2, $"expected Connected + DataReceived, polled {handled}");
            Assert.Equal(handled, events);
            await server.StopAsync();
        }

        [Fact]
        public async Task UdpOnly_Config_Connects_Via_Handshake_Resend() {
            var config = new NetConfig {
                EventDelivery = EventDelivery.Immediate,
                ConnectTimeoutMs = 5000
            };
            config.Channels[1] = new ChannelConfig(TransportType.Udp);

            using var server = new NetServer(config);
            server.DataReceived += (conn, channel, data) => _ = conn.SendAsync(channel, data);
            int port = await server.StartAsync(0);

            using var client = new NetClient(config);
            var echo = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
            client.DataReceived += (channel, data) => echo.TrySetResult(data);

            await client.ConnectAsync("127.0.0.1", port);
            Assert.True(client.IsConnected);
            await client.SendAsync(1, new byte[] { 7 });
            Assert.Equal(new byte[] { 7 }, await WithTimeout(echo.Task));
            await server.StopAsync();
        }

        [Fact]
        public async Task Connect_To_Dead_Port_Fails() {
            var config = MakeConfig();
            config.ConnectTimeoutMs = 1500;
            using var client = new NetClient(config);

            // Nothing listens on this port: TCP refuses or the handshake times out.
            await Assert.ThrowsAnyAsync<Exception>(async () =>
                await client.ConnectAsync("127.0.0.1", 1));
            Assert.False(client.IsConnected);
        }

        [Fact]
        public async Task Broadcast_Reaches_All_Clients() {
            using var server = new NetServer(MakeConfig());
            int port = await server.StartAsync(0);

            using var client1 = new NetClient(MakeConfig());
            using var client2 = new NetClient(MakeConfig());
            var received1 = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
            var received2 = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
            client1.DataReceived += (_, data) => received1.TrySetResult(data);
            client2.DataReceived += (_, data) => received2.TrySetResult(data);

            await client1.ConnectAsync("127.0.0.1", port);
            await client2.ConnectAsync("127.0.0.1", port);
            await WaitUntilAsync(() => server.ConnectionCount == 2);

            await server.BroadcastAsync(0, new byte[] { 99 });

            Assert.Equal(new byte[] { 99 }, await WithTimeout(received1.Task));
            Assert.Equal(new byte[] { 99 }, await WithTimeout(received2.Task));
            await server.StopAsync();
        }

        [Fact]
        public async Task Client_Is_Reusable_After_Disconnect() {
            using var server = new NetServer(MakeConfig());
            int port = await server.StartAsync(0);

            using var client = new NetClient(MakeConfig());
            await client.ConnectAsync("127.0.0.1", port);
            long firstId = client.ConnectionId;
            client.Disconnect();

            await client.ConnectAsync("127.0.0.1", port);
            Assert.True(client.IsConnected);
            Assert.NotEqual(firstId, client.ConnectionId);
            await server.StopAsync();
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
