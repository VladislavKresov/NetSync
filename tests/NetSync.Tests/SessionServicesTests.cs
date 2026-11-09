using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NetSync.Discovery;
using NetSync.Peers;
using NetSync.Pipeline.Reliability;
using NetSync.Transports;
using NetSync.Transports.Simulation;
using Xunit;
using Xunit.Abstractions;

namespace NetSync.Tests {
    /// <summary>Stage 17: time sync, connectionless queries, discovery payload, network simulator, path MTU.</summary>
    public class SessionServicesTests {
        private readonly ITestOutputHelper _output;

        public SessionServicesTests(ITestOutputHelper output) => _output = output;

        private static NetConfig Config() {
            var config = new NetConfig { EventDelivery = EventDelivery.Immediate, ConnectTimeoutMs = 5000, PingIntervalMs = 200 };
            config.Channels[0] = new ChannelConfig(TransportType.Tcp);
            config.Channels[1] = new ChannelConfig(TransportType.Udp, ReliabilityMode.ReliableOrdered);
            config.Channels[2] = new ChannelConfig(TransportType.Udp);
            return config;
        }

        // ------------------------------------------------------------ time sync

        [Fact]
        public async Task Client_Sees_The_Server_Clock() {
            using var server = new NetServer(Config());
            int port = await server.StartAsync(0);
            await Task.Delay(200); // let the server clock run ahead of zero

            using var client = new NetClient(Config());
            Assert.True(double.IsNaN(client.ServerTimeMs));
            await client.ConnectAsync("127.0.0.1", port);
            await WaitUntilAsync(() => !double.IsNaN(client.ServerTimeMs));

            // Same process, same physical clock: the estimate must match the server's own reading.
            // Eventually: the estimate improves with samples, and a thread can be preempted between the two reads.
            double error = double.NaN;
            bool settled = await Eventually.HoldsAsync(() => {
                error = client.ServerTimeMs - server.TimeMs;
                return Math.Abs(error) < 5 && client.ServerTimeAccuracyMs < 5;
            });
            _output.WriteLine($"server {server.TimeMs:F2} ms, error {error:F3} ms, accuracy ±{client.ServerTimeAccuracyMs:F3} ms");
            Assert.True(settled, $"clock estimate off by {error:F3} ms, accuracy ±{client.ServerTimeAccuracyMs:F3} ms");
            await server.StopAsync();
        }

        // ------------------------------------------------------------ queries

        [Fact]
        public async Task Query_Gets_An_Answer_Without_Connecting() {
            using var server = new NetServer(Config());
            server.QueryHandler = (endPoint, request) =>
                Encoding.UTF8.GetBytes($"name=Test;players={server.ConnectionCount};asked={Encoding.UTF8.GetString(request.Span)}");
            int port = await server.StartAsync(0);

            var result = await NetQuery.QueryAsync(new IPEndPoint(IPAddress.Loopback, port), Encoding.UTF8.GetBytes("info"));
            Assert.Equal("name=Test;players=0;asked=info", Encoding.UTF8.GetString(result.Response));
            Assert.True(result.RoundTripMs > 0);
            Assert.Equal(0, server.ConnectionCount); // nobody connected
            await server.StopAsync();
        }

        [Fact]
        public async Task Query_Replies_Never_Exceed_The_Request() {
            using var server = new NetServer(Config());
            server.QueryHandler = (_, _) => new byte[300];
            int port = await server.StartAsync(0);
            var endPoint = new IPEndPoint(IPAddress.Loopback, port);

            // Asking for room for 512 bytes: answered.
            var result = await NetQuery.QueryAsync(endPoint, ReadOnlyMemory<byte>.Empty, maxResponseSize: 512);
            Assert.Equal(300, result.Response.Length);
            // A small query can't be turned into a big reply (no amplification): silence.
            await Assert.ThrowsAsync<TimeoutException>(() =>
                NetQuery.QueryAsync(endPoint, ReadOnlyMemory<byte>.Empty, maxResponseSize: 16, timeoutMs: 200, attempts: 2));
            await server.StopAsync();
        }

        [Fact]
        public async Task Query_Without_A_Handler_Times_Out() {
            using var server = new NetServer(Config());
            int port = await server.StartAsync(0);
            await Assert.ThrowsAsync<TimeoutException>(() =>
                NetQuery.QueryAsync(new IPEndPoint(IPAddress.Loopback, port), ReadOnlyMemory<byte>.Empty, timeoutMs: 200, attempts: 1));
            await server.StopAsync();
        }

        // ------------------------------------------------------------ discovery

        [Fact]
        public async Task Discovery_Carries_The_Server_Description() {
            using var listener = new LanServerListener("PayloadApp", discoveryPort: 0);
            var found = new TaskCompletionSource<DiscoveredServer>(TaskCreationOptions.RunContinuationsAsynchronously);
            listener.ServerDiscovered += server => found.TrySetResult(server);
            listener.Start();

            using var broadcaster = new LanServerBroadcaster("PayloadApp", serverPort: 7777, intervalMs: 100) {
                TargetOverride = new IPEndPoint(IPAddress.Loopback, listener.Port),
                Payload = Encoding.UTF8.GetBytes("Arena 3 / 5 players")
            };
            broadcaster.Start();

            var server = await WithTimeout(found.Task);
            Assert.Equal(7777, server.EndPoint.Port);
            Assert.Equal("Arena 3 / 5 players", Encoding.UTF8.GetString(server.Payload));
        }

        [Fact]
        public void Discovery_Still_Understands_Version_1_Announcements() {
            var v1 = new byte[] { (byte)'N', (byte)'S', (byte)'D', (byte)'1', 0x1E, 0x61, 3, (byte)'a', (byte)'p', (byte)'p' };
            Assert.True(DiscoveryMessage.TryDecode(v1, out var appId, out var port, out var payload));
            Assert.Equal("app", appId);
            Assert.Equal(7777, port);
            Assert.Empty(payload);
        }

        // ------------------------------------------------------------ simulation

        [Fact]
        public async Task Simulated_Latency_Shows_Up_In_The_Round_Trip() {
            using var server = new NetServer(Config());
            server.MessageReceived += (connection, channel, data) => _ = connection.SendAsync(channel, data);
            int port = await server.StartAsync(0);

            var clientConfig = Config();
            clientConfig.Simulation = new NetworkSimulation { LatencyMs = 40 };
            using var client = new NetClient(clientConfig);
            var echoed = new SemaphoreSlim(0);
            client.MessageReceived += (_, _) => echoed.Release();
            await client.ConnectAsync("127.0.0.1", port);

            foreach (byte channel in new byte[] { 0, 2 }) {
                var stopwatch = Stopwatch.StartNew();
                await client.SendAsync(channel, new byte[] { 1 });
                Assert.True(await echoed.WaitAsync(5000));
                _output.WriteLine($"channel {channel}: RTT {stopwatch.Elapsed.TotalMilliseconds:F1} ms");
                Assert.True(stopwatch.Elapsed.TotalMilliseconds >= 75, "the simulated 2 × 40 ms were not applied"); // a busy machine only adds to it
            }
            await server.StopAsync();
        }

        [Fact]
        public async Task Reliable_Channel_Survives_A_Simulated_Lossy_Jittery_Network() {
            using var server = new NetServer(Config());
            var arrivals = new ConcurrentQueue<int>();
            server.MessageReceived += (_, channel, data) => {
                if (channel == 1) {
                    arrivals.Enqueue(BitConverter.ToInt32(data.Span));
                }
            };
            int port = await server.StartAsync(0);

            var clientConfig = Config();
            clientConfig.Simulation = new NetworkSimulation { LatencyMs = 5, JitterMs = 10, LossRate = 0.2, DuplicateRate = 0.1, Seed = 7 };
            using var client = new NetClient(clientConfig);
            await client.ConnectAsync("127.0.0.1", port);

            const int count = 300;
            for (int i = 0; i < count; i++) {
                await client.SendAsync(1, BitConverter.GetBytes(i));
            }
            await WaitUntilAsync(() => arrivals.Count >= count, 30_000);
            Assert.Equal(Enumerable.Range(0, count), arrivals.ToArray()); // all, once, in order
            await server.StopAsync();
        }

        [Fact]
        public async Task Simulated_Stream_Link_Keeps_Order_Despite_Jitter() {
            using var server = new NetServer(Config());
            var arrivals = new ConcurrentQueue<int>();
            server.MessageReceived += (_, channel, data) => arrivals.Enqueue(BitConverter.ToInt32(data.Span));
            int port = await server.StartAsync(0);

            var clientConfig = Config();
            clientConfig.Simulation = new NetworkSimulation { JitterMs = 20, Seed = 3 };
            using var client = new NetClient(clientConfig);
            await client.ConnectAsync("127.0.0.1", port);
            for (int i = 0; i < 200; i++) {
                await client.SendAsync(0, BitConverter.GetBytes(i));
            }
            await WaitUntilAsync(() => arrivals.Count >= 200);
            Assert.Equal(Enumerable.Range(0, 200), arrivals.ToArray()); // TCP stays TCP
            await server.StopAsync();
        }

        // ------------------------------------------------------------ path MTU

        [Fact]
        public async Task Path_Mtu_Raises_The_Fragment_Size_On_Both_Sides() {
            using var server = new NetServer(Config());
            var received = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
            server.DataReceived += (_, channel, data) => {
                if (channel == 1) {
                    received.TrySetResult(data);
                }
            };
            int port = await server.StartAsync(0);
            using var client = new NetClient(Config());
            await client.ConnectAsync("127.0.0.1", port);

            // Loopback carries 1472-byte datagrams: fragments grow from 1100 to 1453 bytes.
            int expected = ReliableEndpoint.FragmentSizeForDatagram(1472, enveloped: false);
            var connection = Assert.Single(server.Connections);
            await WaitUntilAsync(() => connection.Reliable!.FragmentSize == expected);
            _output.WriteLine($"fragment size after MTU discovery: {connection.Reliable!.FragmentSize}");

            var payload = new byte[100_000];
            new Random(5).NextBytes(payload);
            await client.SendAsync(1, payload);
            Assert.Equal(payload, await WithTimeout(received.Task));
            await server.StopAsync();
        }

        [Fact]
        public async Task Mtu_Probe_Gets_An_Answer_Only_Within_The_Rate_Limit() {
            using var server = new NetServer(Config());
            int port = await server.StartAsync(0);
            using var raw = new UdpClient(AddressFamily.InterNetwork);
            raw.Connect(IPAddress.Loopback, port);
            var probe = new byte[600];
            probe[0] = (byte)PacketType.MtuProbe;
            var reply = await TestUdp.RequestAsync(raw, probe);
            Assert.Equal((byte)PacketType.MtuReply, reply.Buffer[0]);
            Assert.Equal(600, (reply.Buffer[5] << 8) | reply.Buffer[6]);
            Assert.True(reply.Buffer.Length < probe.Length); // never amplifies
            await server.StopAsync();
        }

        // ------------------------------------------------------------ helpers

        private static async Task WaitUntilAsync(Func<bool> condition, int timeoutMs = 10_000) {
            var deadline = DateTime.UtcNow.AddMilliseconds(timeoutMs);
            while (!condition()) {
                if (DateTime.UtcNow > deadline) {
                    throw new TimeoutException("Condition not met in time");
                }
                await Task.Delay(10);
            }
        }

        private static async Task<T> WithTimeout<T>(Task<T> task, int timeoutMs = 10_000) {
            Assert.Same(task, await Task.WhenAny(task, Task.Delay(timeoutMs)));
            return await task;
        }
    }
}
