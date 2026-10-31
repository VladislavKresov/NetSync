using System;
using System.Diagnostics;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using LiteNetLib;
using NetSync.Peers;

namespace NetSync.Benchmarks {
    /// <summary>
    /// NetSync next to LiteNetLib under one methodology: loopback echo with 64 messages in
    /// flight (rate + allocations per message, both ends in-process), then a strict
    /// ping-pong for latency. LiteNetLib runs in its documented low-latency configuration:
    /// events on its network thread (UnsyncedEvents), TriggerUpdate() after every send (it
    /// otherwise sends on its update tick — ~15 ms on Windows), auto-recycled readers.
    /// `dotnet run -c Release -- compare`
    /// </summary>
    internal static class ComparisonHarness {
        private interface IEchoPair : IDisposable {
            string Name { get; }
            Action? Echoed { get; set; }
            Task StartAsync();
            void Send(byte[] payload);
        }

        public static async Task RunAsync() {
            Console.WriteLine("NetSync vs LiteNetLib 2.1.4 — loopback echo, client and server in one process");
            Console.WriteLine($"OS: {Environment.OSVersion}, .NET {Environment.Version}, ServerGC: {System.Runtime.GCSettings.IsServerGC}");
            Console.WriteLine();
            Console.WriteLine($"{"",-42}{"msg/s",12}{"B/msg",9}{"p50 ms",9}{"p99 ms",9}");

            await MeasureAsync(new NetSyncPair("NetSync UDP unreliable", 1));
            await MeasureAsync(new LitePair("LiteNetLib unreliable", DeliveryMethod.Unreliable));
            await MeasureAsync(new NetSyncPair("NetSync UDP reliable-ordered", 2));
            await MeasureAsync(new LitePair("LiteNetLib reliable-ordered", DeliveryMethod.ReliableOrdered));
            await MeasureAsync(new NetSyncPair("NetSync TCP (no LiteNetLib equivalent)", 0));
        }

        private static async Task MeasureAsync(IEchoPair pair) {
            using (pair) {
                await pair.StartAsync();
                var payload = new byte[64];
                await RateAsync(pair, payload, 20_000); // warm-up
                var (rate, allocated) = await RateAsync(pair, payload, 100_000);
                var (p50, p99) = await LatencyAsync(pair, new byte[1], 3000);
                Console.WriteLine($"{pair.Name,-42}{rate,12:N0}{allocated,9:N0}{p50,9:F3}{p99,9:F3}");
            }
        }

        private static async Task<(double Rate, double BytesPerMessage)> RateAsync(IEchoPair pair, byte[] payload, int messages) {
            int received = 0;
            var done = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            pair.Echoed = () => {
                if (Interlocked.Increment(ref received) >= messages) {
                    done.TrySetResult(true);
                }
            };
            const int window = 64;
            long allocatedBefore = GC.GetTotalAllocatedBytes(precise: true);
            var stopwatch = Stopwatch.StartNew();
            var pump = Task.Run(async () => {
                int sent = 0;
                long lastProgress = Environment.TickCount64;
                int lastReceived = 0;
                while (sent < messages && !done.Task.IsCompleted) {
                    int now = Volatile.Read(ref received);
                    if (now != lastReceived) {
                        lastReceived = now;
                        lastProgress = Environment.TickCount64;
                    }
                    // Unreliable echoes can be lost: when the window stalls for 500 ms, move on.
                    if (sent - now >= window && Environment.TickCount64 - lastProgress < 500) {
                        await Task.Yield();
                        continue;
                    }
                    pair.Send(payload);
                    sent++;
                }
            });
            await pump;
            await Task.WhenAny(done.Task, Task.Delay(3000));
            stopwatch.Stop();
            long allocated = GC.GetTotalAllocatedBytes(precise: true) - allocatedBefore;
            int completed = Math.Min(Volatile.Read(ref received), messages);
            pair.Echoed = null;
            return (completed / stopwatch.Elapsed.TotalSeconds, allocated / (double)(2L * completed));
        }

        private static async Task<(double P50, double P99)> LatencyAsync(IEchoPair pair, byte[] payload, int samples) {
            var echoed = new SemaphoreSlim(0);
            pair.Echoed = () => echoed.Release();
            var rtts = new double[samples];
            var stopwatch = new Stopwatch();
            for (int i = 0; i < samples; i++) {
                stopwatch.Restart();
                pair.Send(payload);
                if (!await echoed.WaitAsync(2000)) {
                    rtts[i] = double.MaxValue;
                    continue;
                }
                rtts[i] = stopwatch.Elapsed.TotalMilliseconds;
            }
            pair.Echoed = null;
            Array.Sort(rtts);
            return (rtts[samples / 2], rtts[(int)(samples * 0.99)]);
        }

        private sealed class NetSyncPair : IEchoPair {
            private readonly byte _channel;
            private NetServer? _server;
            private NetClient? _client;

            public NetSyncPair(string name, byte channel) {
                Name = name;
                _channel = channel;
            }

            public string Name { get; }
            public Action? Echoed { get; set; }

            private static NetConfig Config() {
                var config = new NetConfig { EventDelivery = EventDelivery.Immediate };
                config.Channels[0] = new ChannelConfig(TransportType.Tcp);
                config.Channels[1] = new ChannelConfig(TransportType.Udp);
                config.Channels[2] = new ChannelConfig(TransportType.Udp, ReliabilityMode.ReliableOrdered);
                return config;
            }

            public async Task StartAsync() {
                _server = new NetServer(Config());
                _server.MessageReceived += (connection, channel, data) => _ = connection.SendAsync(channel, data);
                int port = await _server.StartAsync(0);
                _client = new NetClient(Config());
                _client.MessageReceived += (channel, data) => Echoed?.Invoke();
                await _client.ConnectAsync("127.0.0.1", port);
            }

            public void Send(byte[] payload) => _ = _client!.SendAsync(_channel, payload);

            public void Dispose() {
                _client?.Dispose();
                _server?.Dispose();
            }
        }

        private sealed class LitePair : IEchoPair {
            private readonly DeliveryMethod _method;
            private NetManager? _server;
            private NetManager? _client;
            private NetPeer? _peer;

            public LitePair(string name, DeliveryMethod method) {
                Name = name;
                _method = method;
            }

            public string Name { get; }
            public Action? Echoed { get; set; }

            public async Task StartAsync() {
                var serverListener = new EventBasedNetListener();
                _server = new NetManager(serverListener) { UnsyncedEvents = true, AutoRecycle = true, UpdateTime = 1 };
                serverListener.ConnectionRequestEvent += request => request.AcceptIfKey("bench");
                serverListener.NetworkReceiveEvent += (peer, reader, channel, method) => {
                    peer.Send(reader.GetRemainingBytesSpan(), method);
                    _server!.TriggerUpdate(); // send now, not on the next update tick
                };
                _server.Start(0);

                var clientListener = new EventBasedNetListener();
                _client = new NetManager(clientListener) { UnsyncedEvents = true, AutoRecycle = true, UpdateTime = 1 };
                clientListener.NetworkReceiveEvent += (peer, reader, channel, method) => Echoed?.Invoke();
                _client.Start();
                _peer = _client.Connect("127.0.0.1", _server.LocalPort, "bench");
                var deadline = Stopwatch.StartNew();
                while (_peer.ConnectionState != ConnectionState.Connected) {
                    if (deadline.ElapsedMilliseconds > 5000) {
                        throw new TimeoutException("LiteNetLib did not connect");
                    }
                    await Task.Delay(5);
                }
            }

            public void Send(byte[] payload) {
                _peer!.Send(payload, _method);
                _client!.TriggerUpdate();
            }

            public void Dispose() {
                _client?.Stop();
                _server?.Stop();
            }
        }
    }
}
