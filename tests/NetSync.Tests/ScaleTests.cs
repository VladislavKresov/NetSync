using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NetSync.Peers;
using NetSync.Pipeline.Reliability;
using NetSync.Transports;
using Xunit;
using Xunit.Abstractions;

namespace NetSync.Tests {
    /// <summary>Stage 20: many connections on one server — delivery, memory per connection, no leaked timers.</summary>
    [Collection(nameof(AllocationCollection))] // measures the whole process: run alone
    public class ScaleTests {
        private readonly ITestOutputHelper _output;

        public ScaleTests(ITestOutputHelper output) => _output = output;

        [Fact]
        public async Task Two_Hundred_Clients_Connect_Exchange_And_Leave_Cleanly() {
            const int count = 200;
            NetConfig Config() {
                var config = new NetConfig { EventDelivery = EventDelivery.Immediate, ConnectTimeoutMs = 30_000, TransportFallbackTimeoutMs = 20_000 };
                config.Channels[0] = new ChannelConfig(TransportType.Tcp);
                config.Channels[1] = new ChannelConfig(TransportType.Udp, ReliabilityMode.ReliableOrdered);
                return config;
            }

            var server = new NetServer(Config());
            long atServer = 0;
            server.MessageReceived += (_, _, _) => Interlocked.Increment(ref atServer);
            int port = await server.StartAsync(0);

            long atClients = 0;
            var clients = Enumerable.Range(0, count).Select(_ => {
                var client = new NetClient(Config());
                client.MessageReceived += (_, _) => Interlocked.Increment(ref atClients);
                return client;
            }).ToList();

            long before = GC.GetTotalMemory(forceFullCollection: true);
            for (int i = 0; i < count; i += 25) {
                await Task.WhenAll(clients.Skip(i).Take(25).Select(c => c.ConnectAsync("127.0.0.1", port)));
            }
            Assert.Equal(count, server.ConnectionCount);
            Assert.DoesNotContain(clients, c => c.IsDegraded);
            double perConnectionKb = (GC.GetTotalMemory(forceFullCollection: true) - before) / 1024.0 / count;
            _output.WriteLine($"{perConnectionKb:F1} KB managed memory per connection (client + server side)");
            // Guards against per-connection buffers sized for the worst case (once ~600 KB).
            Assert.True(perConnectionKb < 200, $"{perConnectionKb:F1} KB per connection");

            for (int i = 0; i < 5; i++) {
                await server.BroadcastAsync(1, new byte[64]);
            }
            await Task.WhenAll(clients.Select(async c => {
                for (int i = 0; i < 5; i++) {
                    await c.SendAsync(0, new byte[64]);
                }
            }));
            await WaitUntilAsync(() => Interlocked.Read(ref atClients) == 5L * count && Interlocked.Read(ref atServer) == 5L * count);

            foreach (var client in clients) {
                client.Dispose();
            }
            await WaitUntilAsync(() => server.ConnectionCount == 0);
            await server.StopAsync();
            // Every reliability engine unregistered from the shared clock: nothing leaks per connection.
            await WaitUntilAsync(() => ReliabilityClock.Count == 0);
        }

        private static async Task WaitUntilAsync(Func<bool> condition, int timeoutMs = 30_000) {
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
