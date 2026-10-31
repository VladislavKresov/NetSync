using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NetSync.Peers;

namespace NetSync.Benchmarks {
    /// <summary>
    /// Many clients, one server, one process: connect time, memory, broadcast and fan-in
    /// delivery, idle CPU. `dotnet run -c Release -- load [clients]` (default 1000).
    /// Each client is a full NetClient (TCP + UDP links, its own UDP receive thread), so the
    /// client side dominates the process numbers; the server side is what scales here.
    /// </summary>
    internal static class LoadHarness {
        public static async Task RunAsync(int clientCount) {
            Console.WriteLine($"NetSync load: {clientCount} clients (TCP + reliable UDP) on one server, one process");
            ThreadPool.GetMinThreads(out int workers, out int io);
            ThreadPool.SetMinThreads(Math.Max(workers, 64), Math.Max(io, 64));

            NetConfig Config() {
                var config = new NetConfig { EventDelivery = EventDelivery.Immediate, ConnectTimeoutMs = 30_000, TransportFallbackTimeoutMs = 20_000 };
                config.Channels[0] = new ChannelConfig(TransportType.Tcp);
                config.Channels[1] = new ChannelConfig(TransportType.Udp, ReliabilityMode.ReliableOrdered);
                return config;
            }

            using var server = new NetServer(Config());
            long serverReceived = 0;
            server.MessageReceived += (connection, channel, data) => Interlocked.Increment(ref serverReceived);
            int port = await server.StartAsync(0);

            long clientReceived = 0;
            var clients = new List<NetClient>(clientCount);
            for (int i = 0; i < clientCount; i++) {
                var client = new NetClient(Config());
                client.MessageReceived += (channel, data) => Interlocked.Increment(ref clientReceived);
                clients.Add(client);
            }

            long memoryBefore = GC.GetTotalMemory(forceFullCollection: true);
            var stopwatch = Stopwatch.StartNew();
            const int batch = 50;
            for (int i = 0; i < clientCount; i += batch) {
                await Task.WhenAll(clients.Skip(i).Take(batch).Select(c => c.ConnectAsync("127.0.0.1", port)));
            }
            stopwatch.Stop();
            Console.WriteLine($"  connected {server.ConnectionCount}/{clientCount} in {stopwatch.Elapsed.TotalSeconds:F1} s " +
                              $"({clientCount / stopwatch.Elapsed.TotalSeconds:F0} connects/s), degraded: {clients.Count(c => c.IsDegraded)}");
            long memoryAfter = GC.GetTotalMemory(forceFullCollection: true);
            Console.WriteLine($"  managed heap: +{(memoryAfter - memoryBefore) / 1024.0 / clientCount:F1} KB per connection (client + server side), " +
                              $"{Process.GetCurrentProcess().Threads.Count} threads");

            // Broadcast: the server sends 10 reliable-UDP messages to everyone.
            var payload = new byte[64];
            stopwatch.Restart();
            for (int i = 0; i < 10; i++) {
                await server.BroadcastAsync(1, payload);
            }
            long expected = 10L * clientCount;
            await WaitAsync(() => Interlocked.Read(ref clientReceived) >= expected, 60_000);
            stopwatch.Stop();
            Console.WriteLine($"  broadcast: {Interlocked.Read(ref clientReceived)}/{expected} delivered in {stopwatch.Elapsed.TotalMilliseconds:F0} ms " +
                              $"({expected / stopwatch.Elapsed.TotalSeconds:F0} msg/s)");

            // Fan-in: every client sends 10 TCP messages.
            stopwatch.Restart();
            await Task.WhenAll(clients.Select(async c => {
                for (int i = 0; i < 10; i++) {
                    await c.SendAsync(0, payload);
                }
            }));
            await WaitAsync(() => Interlocked.Read(ref serverReceived) >= expected, 60_000);
            stopwatch.Stop();
            Console.WriteLine($"  fan-in: {Interlocked.Read(ref serverReceived)}/{expected} received in {stopwatch.Elapsed.TotalMilliseconds:F0} ms " +
                              $"({expected / stopwatch.Elapsed.TotalSeconds:F0} msg/s)");

            // Idle: keepalives, time sync, retransmit clock — what does doing nothing cost?
            var process = Process.GetCurrentProcess();
            var cpuBefore = process.TotalProcessorTime;
            await Task.Delay(5000);
            process.Refresh();
            double cpu = (process.TotalProcessorTime - cpuBefore).TotalMilliseconds / 5000 * 100;
            Console.WriteLine($"  idle: {cpu:F1}% of one core for {clientCount} connections on both ends");

            foreach (var client in clients) {
                client.Dispose();
            }
            await server.StopAsync();
        }

        private static async Task WaitAsync(Func<bool> condition, int timeoutMs) {
            var deadline = Stopwatch.StartNew();
            while (!condition() && deadline.ElapsedMilliseconds < timeoutMs) {
                await Task.Delay(5);
            }
        }
    }
}
