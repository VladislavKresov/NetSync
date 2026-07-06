using System;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using NetSync.Peers;
using Xunit;

namespace NetSync.Tests {
    public class PeerReliabilityTests {
        [Fact]
        public async Task ReliableOrdered_Udp_Channel_Roundtrips_Large_Payload() {
            // 300 KB over UDP: far beyond a single datagram — proves reliable
            // fragmentation end-to-end through the real transports.
            var config = MakeTcpUdpConfig();
            using var server = new NetServer(MakeTcpUdpConfig());
            server.DataReceived += (conn, channel, data) => _ = conn.SendAsync(channel, data);
            int port = await server.StartAsync(0);

            using var client = new NetClient(config);
            var echo = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
            client.DataReceived += (channel, data) => {
                if (channel == 2) echo.TrySetResult(data);
            };
            await client.ConnectAsync("127.0.0.1", port);

            var payload = new byte[300_000];
            new Random(11).NextBytes(payload);
            await client.SendAsync(2, payload);

            var completed = await Task.WhenAny(echo.Task, Task.Delay(30_000));
            Assert.Same(echo.Task, completed);
            Assert.Equal(payload, await echo.Task);
            await server.StopAsync();
        }

        [Fact]
        public async Task Sequenced_Udp_Channel_Delivers() {
            var config = MakeTcpUdpConfig();
            using var server = new NetServer(MakeTcpUdpConfig());
            server.DataReceived += (conn, channel, data) => _ = conn.SendAsync(channel, data);
            int port = await server.StartAsync(0);

            using var client = new NetClient(config);
            var echo = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
            client.DataReceived += (channel, data) => {
                if (channel == 3) echo.TrySetResult(data);
            };
            await client.ConnectAsync("127.0.0.1", port);

            await client.SendAsync(3, new byte[] { 42, 43 });
            var completed = await Task.WhenAny(echo.Task, Task.Delay(10_000));
            Assert.Same(echo.Task, completed);
            Assert.Equal(new byte[] { 42, 43 }, await echo.Task);
            await server.StopAsync();
        }

        [Fact]
        public async Task File_Transfer_Survives_Lossy_Network() {
            // PLAN.md stage 4 acceptance: a file sent over UDP with ~5% packet loss
            // arrives intact. The proxy drops datagrams in both directions.
            var config = MakeUdpOnlyConfig();
            using var server = new NetServer(MakeUdpOnlyConfig());
            var received = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
            server.DataReceived += (conn, channel, data) => {
                if (channel == 2) received.TrySetResult(data);
            };
            int serverPort = await server.StartAsync(0);

            using var proxy = new LossyUdpProxy(serverPort, lossRate: 0.05, seed: 99);
            using var client = new NetClient(config);
            await client.ConnectAsync("127.0.0.1", proxy.Port);

            var file = new byte[1_000_000]; // ~910 fragments
            new Random(23).NextBytes(file);
            await client.SendAsync(2, file);

            var completed = await Task.WhenAny(received.Task, Task.Delay(60_000));
            Assert.Same(received.Task, completed);
            Assert.Equal(file, await received.Task);
            Assert.True(proxy.Dropped > 0, $"proxy should have dropped packets, dropped={proxy.Dropped}");
            await server.StopAsync();
        }

        private static NetConfig MakeTcpUdpConfig() {
            var config = new NetConfig {
                EventDelivery = EventDelivery.Immediate,
                PingIntervalMs = 200,
                ConnectTimeoutMs = 5000
            };
            config.Channels[0] = new ChannelConfig(TransportType.Tcp);
            config.Channels[1] = new ChannelConfig(TransportType.Udp);
            config.Channels[2] = new ChannelConfig(TransportType.Udp, ReliabilityMode.ReliableOrdered);
            config.Channels[3] = new ChannelConfig(TransportType.Udp, ReliabilityMode.UnreliableSequenced);
            return config;
        }

        private static NetConfig MakeUdpOnlyConfig() {
            var config = new NetConfig {
                EventDelivery = EventDelivery.Immediate,
                PingIntervalMs = 200,
                ConnectTimeoutMs = 10_000
            };
            config.Channels[2] = new ChannelConfig(TransportType.Udp, ReliabilityMode.ReliableOrdered);
            return config;
        }

        /// <summary>
        /// UDP forwarder with seeded random drops in both directions. Single-client:
        /// remembers the last client endpoint seen on the front socket.
        /// </summary>
        private sealed class LossyUdpProxy : IDisposable {
            private readonly UdpClient _front;
            private readonly UdpClient _back;
            private readonly IPEndPoint _serverEndPoint;
            private readonly Random _random = new Random();
            private readonly double _lossRate;
            private volatile IPEndPoint? _clientEndPoint;
            private int _dropped;
            private bool _disposed;

            public int Port { get; }
            public int Dropped => Volatile.Read(ref _dropped);

            public LossyUdpProxy(int serverPort, double lossRate, int seed) {
                _lossRate = lossRate;
                _random = new Random(seed);
                _serverEndPoint = new IPEndPoint(IPAddress.Loopback, serverPort);
                _front = new UdpClient(0);
                _back = new UdpClient(0);
                Port = ((IPEndPoint)_front.Client.LocalEndPoint!).Port;
                _ = Task.Run(FrontLoopAsync);
                _ = Task.Run(BackLoopAsync);
            }

            private async Task FrontLoopAsync() {
                try {
                    while (!_disposed) {
                        var result = await _front.ReceiveAsync();
                        _clientEndPoint = result.RemoteEndPoint;
                        if (ShouldDrop()) {
                            continue;
                        }
                        await _back.SendAsync(result.Buffer, result.Buffer.Length, _serverEndPoint);
                    }
                }
                catch (Exception) when (_disposed) { }
                catch (SocketException) { }
                catch (ObjectDisposedException) { }
            }

            private async Task BackLoopAsync() {
                try {
                    while (!_disposed) {
                        var result = await _back.ReceiveAsync();
                        var client = _clientEndPoint;
                        if (client == null || ShouldDrop()) {
                            continue;
                        }
                        await _front.SendAsync(result.Buffer, result.Buffer.Length, client);
                    }
                }
                catch (Exception) when (_disposed) { }
                catch (SocketException) { }
                catch (ObjectDisposedException) { }
            }

            private bool ShouldDrop() {
                bool drop;
                lock (_random) {
                    drop = _random.NextDouble() < _lossRate;
                }
                if (drop) {
                    Interlocked.Increment(ref _dropped);
                }
                return drop;
            }

            public void Dispose() {
                _disposed = true;
                _front.Close();
                _back.Close();
            }
        }
    }
}
