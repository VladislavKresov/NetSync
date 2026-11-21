using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NetSync.Peers;
using NetSync.Transports;
using Xunit;
using Xunit.Abstractions;

namespace NetSync.Tests {
    /// <summary>
    /// UDP stops working mid-session (a firewall, a NAT that dropped the mapping, a network
    /// switch): the UDP channels move to TCP and the connection stays — nothing lost,
    /// duplicated or reordered on the reliable channels.
    /// </summary>
    public class UdpHandoverTests {
        private const byte Ordered = 1;
        private const byte Unordered = 2;
        private readonly ITestOutputHelper _output;

        public UdpHandoverTests(ITestOutputHelper output) => _output = output;

        private static NetConfig Config(bool requireAllTransports = false, bool encrypted = false) {
            var config = new NetConfig {
                EventDelivery = EventDelivery.Immediate,
                PingIntervalMs = 200,
                PingTimeoutMs = 1500,
                RequireAllTransports = requireAllTransports
            };
            config.Channels[0] = new ChannelConfig(TransportType.Tcp);
            config.Channels[Ordered] = new ChannelConfig(TransportType.Udp, ReliabilityMode.ReliableOrdered);
            config.Channels[Unordered] = new ChannelConfig(TransportType.Udp, ReliabilityMode.Reliable);
            if (encrypted) {
                config.Encryption = new EncryptionConfig {
                    Mode = KeyExchangeMode.PresharedKey,
                    PresharedKey = System.Text.Encoding.ASCII.GetBytes("udp-handover-test-psk-32-bytes!!")
                };
            }
            return config;
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)] // after the switch the reliable packets ride the TCP link's envelope
        public async Task Reliable_Udp_Channels_Continue_Over_Tcp_When_Udp_Is_Lost(bool encrypted) {
            const int count = 3000;
            using var server = new NetServer(Config(encrypted: encrypted));
            var atServer = new ConcurrentQueue<int>();
            var atServerUnordered = new ConcurrentDictionary<int, int>();
            server.MessageReceived += (_, channel, data) => {
                int value = BitConverter.ToInt32(data.Span);
                if (channel == Ordered) {
                    atServer.Enqueue(value);
                }
                else if (channel == Unordered) {
                    atServerUnordered.AddOrUpdate(value, 1, (_, seen) => seen + 1);
                }
            };
            var opened = new TaskCompletionSource<NetConnection>(TaskCreationOptions.RunContinuationsAsynchronously);
            server.ConnectionOpened += connection => opened.TrySetResult(connection);
            int port = await server.StartAsync(0);

            using var network = new TransportProxy(port);
            using var client = new NetClient(Config(encrypted: encrypted));
            var atClient = new ConcurrentQueue<int>();
            DisconnectReason? disconnected = null;
            client.MessageReceived += (channel, data) => {
                if (channel == Ordered) {
                    atClient.Enqueue(BitConverter.ToInt32(data.Span));
                }
            };
            client.Disconnected += reason => disconnected = reason;
            await client.ConnectAsync("127.0.0.1", network.Port);
            var connection = await opened.Task;
            Assert.False(client.IsDegraded);

            // Both sides stream; a third of the way into the client's stream every datagram is
            // lost from then on — mid-flight, with reliable packets still unacknowledged.
            async Task Stream(Func<byte, byte[], ValueTask> send) {
                for (int i = 0; i < count; i++) {
                    await send(Ordered, BitConverter.GetBytes(i));
                    if (i % 3 == 0) {
                        await send(Unordered, BitConverter.GetBytes(i));
                    }
                }
            }

            var fromClient = Task.Run(async () => {
                for (int i = 0; i < count; i++) {
                    if (i == count / 3) {
                        network.DropUdp = true; // UDP is gone, both directions
                    }
                    await client.SendAsync(Ordered, BitConverter.GetBytes(i));
                    if (i % 3 == 0) {
                        await client.SendAsync(Unordered, BitConverter.GetBytes(i));
                    }
                }
            });
            var fromServer = Task.Run(() => Stream((channel, data) => connection.SendAsync(channel, data)));
            await Task.WhenAll(fromClient, fromServer).WaitAsync(TimeSpan.FromSeconds(60));

            Assert.True(await Eventually.HoldsAsync(() => atServer.Count >= count && atClient.Count >= count, 30_000),
                        $"server got {atServer.Count}, client got {atClient.Count} of {count}; disconnected: {disconnected}");
            Assert.True(await Eventually.HoldsAsync(() => atServerUnordered.Count >= (count + 2) / 3, 10_000));
            await Task.Delay(300); // anything extra (a duplicate) would show up by now

            Assert.Equal(Enumerable.Range(0, count), atServer.ToArray());  // all, once, in order
            Assert.Equal(Enumerable.Range(0, count), atClient.ToArray());
            Assert.Equal(Enumerable.Range(0, count).Where(i => i % 3 == 0), atServerUnordered.Keys.OrderBy(i => i));
            Assert.All(atServerUnordered.Values, seen => Assert.Equal(1, seen));
            Assert.Null(disconnected);
            Assert.True(client.IsConnected);
            Assert.True(client.IsDegraded);
            Assert.True(connection.IsDegraded);

            // And later messages keep flowing both ways.
            await client.SendAsync(Ordered, BitConverter.GetBytes(count));
            await connection.SendAsync(Ordered, BitConverter.GetBytes(count));
            Assert.True(await Eventually.HoldsAsync(() => atServer.Count == count + 1 && atClient.Count == count + 1));
            _output.WriteLine($"client: {client.Statistics}");
        }

        [Fact]
        public async Task With_RequireAllTransports_Losing_Udp_Disconnects() {
            using var server = new NetServer(Config(requireAllTransports: true));
            int port = await server.StartAsync(0);
            using var network = new TransportProxy(port);
            using var client = new NetClient(Config(requireAllTransports: true));
            var disconnected = new TaskCompletionSource<DisconnectReason>(TaskCreationOptions.RunContinuationsAsynchronously);
            client.Disconnected += reason => disconnected.TrySetResult(reason);
            await client.ConnectAsync("127.0.0.1", network.Port);

            network.DropUdp = true;
            Assert.Same(disconnected.Task, await Task.WhenAny(disconnected.Task, Task.Delay(20_000)));
            Assert.False(client.IsDegraded);
        }
    }
}
