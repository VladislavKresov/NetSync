using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading.Tasks;
using NetSync.Peers;
using NetSync.Transports;
using NetSync.Transports.Simulation;
using Xunit;

namespace NetSync.Tests {
    /// <summary>
    /// An ordered channel keeps the order the application sent in — also when it doesn't await
    /// each send and the window is full, so sends have to wait for room.
    /// </summary>
    public class SendOrderTests {
        private const byte Ordered = 1;

        private static NetConfig Config() {
            var config = new NetConfig { EventDelivery = EventDelivery.Immediate };
            config.Channels[0] = new ChannelConfig(TransportType.Tcp);
            config.Channels[Ordered] = new ChannelConfig(TransportType.Udp, ReliabilityMode.ReliableOrdered);
            return config;
        }

        [Fact]
        public async Task Unawaited_Sends_On_A_Full_Window_Arrive_In_Send_Order() {
            using var server = new NetServer(Config());
            var arrived = new ConcurrentQueue<(int Number, int Length)>();
            server.MessageReceived += (_, channel, data) => {
                if (channel == Ordered) {
                    arrived.Enqueue((BitConverter.ToInt32(data.Span), data.Length));
                }
            };
            int port = await server.StartAsync(0);

            var clientConfig = Config();
            clientConfig.Simulation = new NetworkSimulation { LatencyMs = 20, Seed = 1 }; // acks take a while: the window fills
            using var client = new NetClient(clientConfig);
            await client.ConnectAsync("127.0.0.1", port);

            // Every tenth message is fragmented: its fragments must not let later messages in between.
            const int count = 3000;
            static int LengthOf(int number) => number % 10 == 0 ? 5000 : 8;
            var sends = new ValueTask[count];
            for (int i = 0; i < count; i++) {
                var message = new byte[LengthOf(i)];
                BitConverter.GetBytes(i).CopyTo(message, 0);
                sends[i] = client.SendAsync(Ordered, message); // not awaited one by one
            }
            foreach (var send in sends) {
                await send;
            }

            Assert.True(await Eventually.HoldsAsync(() => arrived.Count >= count, 30_000), $"{arrived.Count} of {count} arrived");
            var received = arrived.ToArray();
            Assert.Equal(Enumerable.Range(0, count), received.Select(message => message.Number));
            Assert.All(received, message => Assert.Equal(LengthOf(message.Number), message.Length));
        }
    }
}
