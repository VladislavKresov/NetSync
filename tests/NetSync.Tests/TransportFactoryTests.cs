using System;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using NetSync.Peers;
using NetSync.Transports;
using NetSync.Transports.Udp;
using Xunit;

namespace NetSync.Tests {
    /// <summary>Stage 11: pluggable transports and the UDP → TCP fallback.</summary>
    public class TransportFactoryTests {
        [Fact]
        public async Task Custom_Transports_Carry_The_Full_Stack_Including_Reliable_Udp() {
            // Both slots replaced by in-memory networks; the datagram slot loses 10%.
            var streamNet = new InMemoryNetwork();
            var datagramNet = new InMemoryNetwork(lossRate: 0.10, seed: 7);
            NetConfig Make() {
                var config = new NetConfig { EventDelivery = EventDelivery.Immediate, ConnectTimeoutMs = 10_000 };
                config.TransportFactories[TransportType.Tcp] = streamNet.Factory;
                config.TransportFactories[TransportType.Udp] = datagramNet.Factory;
                config.Channels[0] = new ChannelConfig(TransportType.Tcp);
                config.Channels[2] = new ChannelConfig(TransportType.Udp, ReliabilityMode.ReliableOrdered);
                return config;
            }

            using var server = new NetServer(Make());
            server.DataReceived += (conn, channel, data) => _ = conn.SendAsync(channel, data);
            int port = await server.StartAsync(0);

            using var client = new NetClient(Make());
            var tcpEcho = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
            var udpEcho = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
            client.DataReceived += (channel, data) => {
                if (channel == 0) tcpEcho.TrySetResult(data);
                if (channel == 2) udpEcho.TrySetResult(data);
            };
            await client.ConnectAsync("127.0.0.1", port);
            Assert.False(client.IsDegraded);

            var big = new byte[200_000];
            new Random(9).NextBytes(big);
            await client.SendAsync(0, new byte[] { 1, 2, 3 });
            await client.SendAsync(2, big);

            Assert.Equal(new byte[] { 1, 2, 3 }, await WithTimeout(tcpEcho.Task, 10_000));
            Assert.Equal(big, await WithTimeout(udpEcho.Task, 30_000));
            Assert.True(datagramNet.Dropped > 0, "the in-memory datagram network should have dropped packets");
            await server.StopAsync();
        }

        [Fact]
        public async Task Blocked_Udp_Falls_Back_To_Tcp_Transparently() {
            using var server = new NetServer(MakeConfig());
            server.DataReceived += (conn, channel, data) => _ = conn.SendAsync(channel, data);
            var opened = new TaskCompletionSource<NetConnection>(TaskCreationOptions.RunContinuationsAsynchronously);
            server.ConnectionOpened += conn => opened.TrySetResult(conn);
            int port = await server.StartAsync(0);

            var clientConfig = MakeConfig();
            clientConfig.TransportFactories[TransportType.Udp] = new BlackholeTransportFactory(); // UDP "blocked"
            using var client = new NetClient(clientConfig);
            var echoes = new TaskCompletionSource<bool>[3];
            for (int i = 0; i < echoes.Length; i++) {
                echoes[i] = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            }
            client.DataReceived += (channel, data) => echoes[channel].TrySetResult(true);

            await client.ConnectAsync("127.0.0.1", port);

            Assert.True(client.IsConnected);
            Assert.True(client.IsDegraded);
            var connection = await WithTimeout(opened.Task, 5000);
            Assert.True(connection.IsDegraded);

            // Every channel works — the UDP ones (unreliable and reliable) now ride TCP.
            for (byte channel = 0; channel < 3; channel++) {
                await client.SendAsync(channel, new[] { channel });
            }
            for (int i = 0; i < 3; i++) {
                Assert.True(await WithTimeout(echoes[i].Task, 5000));
            }
            await server.StopAsync();
        }

        [Fact]
        public async Task RequireAllTransports_Fails_Instead_Of_Falling_Back() {
            var serverConfig = MakeConfig();
            serverConfig.RequireAllTransports = true;
            using var server = new NetServer(serverConfig);
            int port = await server.StartAsync(0);

            var clientConfig = MakeConfig();
            clientConfig.RequireAllTransports = true;
            clientConfig.ConnectTimeoutMs = 1500;
            clientConfig.TransportFactories[TransportType.Udp] = new BlackholeTransportFactory();
            using var client = new NetClient(clientConfig);

            await Assert.ThrowsAsync<TimeoutException>(async () => await client.ConnectAsync("127.0.0.1", port));
            Assert.False(client.IsConnected);
            await server.StopAsync();
        }

        [Fact]
        public async Task Udp_Link_Survives_Icmp_Port_Unreachable() {
            // Datagrams to a port nobody listens on come back as ICMP "unreachable"; on
            // Windows that surfaces as ConnectionReset on the next receive. It must not
            // kill the link (real death is the ping timeout's job).
            int deadPort;
            using (var probe = new System.Net.Sockets.UdpClient(0)) {
                deadPort = ((IPEndPoint)probe.Client.LocalEndPoint!).Port;
            }

            using var client = new UdpTransport(pingIntervalMs: 50, pingTimeoutMs: 0);
            int disconnects = 0;
            client.Disconnected += _ => Interlocked.Increment(ref disconnects);
            await client.ConnectAsync(new IPEndPoint(IPAddress.Loopback, deadPort));
            for (int i = 0; i < 5; i++) {
                await client.SendAsync(new byte[] { 1 });
                await Task.Delay(50);
            }
            await Task.Delay(300);

            Assert.True(client.IsConnected);
            Assert.Equal(0, Volatile.Read(ref disconnects));
        }

        private static NetConfig MakeConfig() {
            var config = new NetConfig {
                EventDelivery = EventDelivery.Immediate,
                PingIntervalMs = 200,
                ConnectTimeoutMs = 5000,
                TransportFallbackTimeoutMs = 500
            };
            config.Channels[0] = new ChannelConfig(TransportType.Tcp);
            config.Channels[1] = new ChannelConfig(TransportType.Udp);
            config.Channels[2] = new ChannelConfig(TransportType.Udp, ReliabilityMode.ReliableOrdered);
            return config;
        }

        private static async Task<T> WithTimeout<T>(Task<T> task, int timeoutMs) {
            var completed = await Task.WhenAny(task, Task.Delay(timeoutMs));
            Assert.Same(task, completed);
            return await task;
        }
    }
}
