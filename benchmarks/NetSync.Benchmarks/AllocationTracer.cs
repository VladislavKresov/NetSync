using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Tracing;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NetSync.Peers;

namespace NetSync.Benchmarks {
    /// <summary>
    /// Finds what allocates on the hot path: runs a TCP and a UDP echo loop and listens to
    /// the runtime's sampled allocation events (one per ~100 KB allocated), grouped by type.
    /// `dotnet run -c Release -- alloc` (direct) or `-- alloc relay` (through a relay, no punching)
    /// </summary>
    internal static class AllocationTracer {
        private sealed class Listener : EventListener {
            public readonly ConcurrentDictionary<string, long> BytesByType = new ConcurrentDictionary<string, long>();
            public volatile bool Recording;

            protected override void OnEventSourceCreated(EventSource source) {
                if (source.Name == "Microsoft-Windows-DotNETRuntime") {
                    EnableEvents(source, EventLevel.Verbose, (EventKeywords)0x1); // GC keyword
                }
            }

            protected override void OnEventWritten(EventWrittenEventArgs e) {
                if (!Recording || e.EventName == null || !e.EventName.StartsWith("GCAllocationTick", StringComparison.Ordinal) || e.Payload == null) {
                    return;
                }
                int typeIndex = e.PayloadNames!.IndexOf("TypeName");
                int amountIndex = e.PayloadNames.IndexOf("AllocationAmount");
                string type = typeIndex >= 0 ? e.Payload[typeIndex]?.ToString() ?? "?" : "?";
                long amount = amountIndex >= 0 ? Convert.ToInt64(e.Payload[amountIndex]) : 0;
                BytesByType.AddOrUpdate(type, amount, (_, total) => total + amount);
            }
        }

        public static async Task RunAsync(bool relayed) {
            using var listener = new Listener();
            foreach (byte channel in new byte[] { 0, 1 }) {
                NetConfig Config() {
                    var config = new NetConfig { EventDelivery = EventDelivery.Immediate };
                    config.Channels[0] = new ChannelConfig(TransportType.Tcp);
                    config.Channels[1] = new ChannelConfig(TransportType.Udp);
                    return config;
                }
                using var relay = new NetSync.Relay.NetRelayServer();
                var room = NetSync.Relay.RelayRoom.NewRoomCode();
                var relayEndPoint = new System.Net.IPEndPoint(System.Net.IPAddress.Loopback, relayed ? await relay.StartAsync(0) : 0);
                using var server = new NetServer(Config());
                server.MessageReceived += (connection, ch, data) => _ = connection.SendAsync(ch, data);
                if (relayed) {
                    var hostRoom = new NetSync.Relay.RelayRoom(room) { RelayEndPoint = relayEndPoint, AllowDirect = false };
                    server.AddListener(TransportType.Tcp, hostRoom.Tcp, 0);
                    server.AddListener(TransportType.Udp, hostRoom.Udp, 0);
                }
                int port = await server.StartAsync(0);
                var clientConfig = Config();
                if (relayed) {
                    var clientRoom = new NetSync.Relay.RelayRoom(room) { AllowDirect = false };
                    clientConfig.TransportFactories[TransportType.Tcp] = clientRoom.Tcp;
                    clientConfig.TransportFactories[TransportType.Udp] = clientRoom.Udp;
                }
                using var client = new NetClient(clientConfig);
                int echoes = 0;
                client.MessageReceived += (ch, data) => Interlocked.Increment(ref echoes);
                if (relayed) {
                    await client.ConnectAsync(relayEndPoint);
                }
                else {
                    await client.ConnectAsync("127.0.0.1", port);
                }

                var payload = new byte[64];
                void PingPong(int count) {
                    for (int i = 0; i < count; i++) {
                        int target = Volatile.Read(ref echoes) + 1;
                        _ = client.SendAsync(channel, payload);
                        long deadline = Stopwatch.GetTimestamp() + Stopwatch.Frequency;
                        var spin = new SpinWait();
                        while (Volatile.Read(ref echoes) < target && Stopwatch.GetTimestamp() < deadline) {
                            spin.SpinOnce(sleep1Threshold: -1);
                        }
                    }
                }
                PingPong(5000);
                listener.BytesByType.Clear();
                listener.Recording = true;
                long before = GC.GetTotalAllocatedBytes(precise: true);
                const int rounds = 50_000;
                PingPong(rounds);
                long total = GC.GetTotalAllocatedBytes(precise: true) - before;
                listener.Recording = false;

                Console.WriteLine($"{(channel == 0 ? "TCP" : "UDP")}: {total / (double)rounds:F1} B per round trip; sampled allocations by type:");
                foreach (var kvp in listener.BytesByType.OrderByDescending(k => k.Value).Take(12)) {
                    Console.WriteLine($"  {kvp.Value,12:N0} B  {kvp.Key}");
                }
                await server.StopAsync();
            }
        }
    }
}
