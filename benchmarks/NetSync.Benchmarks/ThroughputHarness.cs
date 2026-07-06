using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using NetSync;
using NetSync.Peers;

namespace NetSync.Benchmarks {
    /// <summary>
    /// End-to-end loopback measurement over the real peer API (both endpoints in-process):
    /// message rate, bulk throughput, RTT percentiles and GC allocations per message.
    /// Numbers include BOTH client and server sides.
    /// </summary>
    internal static class ThroughputHarness {
        public static async Task RunAsync() {
            var config = MakeConfig();
            using var server = new NetServer(MakeConfig());
            server.DataReceived += (conn, channel, data) => _ = conn.SendAsync(channel, data);
            int port = await server.StartAsync(0);

            using var client = new NetClient(config);
            await client.ConnectAsync("127.0.0.1", port);

            Console.WriteLine("NetSync loopback throughput (client+server in one process)");
            Console.WriteLine($"OS: {Environment.OSVersion}, .NET {Environment.Version}, ServerGC: {System.Runtime.GCSettings.IsServerGC}");
            Console.WriteLine();

            await MeasureEchoRateAsync(client, channel: 0, payloadSize: 64, messages: 20_000, "TCP small packets (64 B echo)");
            await MeasureEchoRateAsync(client, channel: 1, payloadSize: 64, messages: 20_000, "UDP small packets (64 B echo)");
            await MeasureEchoRateAsync(client, channel: 1, payloadSize: 1150, messages: 20_000, "UDP MTU-size packets (1150 B echo)");
            await MeasureEchoRateAsync(client, channel: 2, payloadSize: 64, messages: 20_000, "UDP reliable-ordered small packets (64 B echo)");
            await MeasureEchoRateAsync(client, channel: 3, payloadSize: 64, messages: 20_000, "TCP encrypted small packets (64 B echo, AES-CBC+HMAC)");
            await MeasureBulkAsync(client, channel: 0, totalBytes: 256L * 1024 * 1024, chunk: 512 * 1024, "TCP bulk transfer (256 MB, fragmented)");
            await MeasureBulkAsync(client, channel: 2, totalBytes: 32L * 1024 * 1024, chunk: 256 * 1024, "UDP reliable-ordered bulk (32 MB, fragmented)");
            await MeasureLatencyAsync(client, channel: 1, samples: 2_000, "UDP RTT (1-byte echo)");
            await MeasureLatencyAsync(client, channel: 2, samples: 2_000, "UDP reliable-ordered RTT (1-byte echo)");

            client.Disconnect();
            await server.StopAsync();
        }

        private static NetConfig MakeConfig() {
            var config = new NetConfig {
                EventDelivery = EventDelivery.Immediate,
                PingIntervalMs = 1000
            };
            config.Channels[0] = new ChannelConfig(TransportType.Tcp);
            config.Channels[1] = new ChannelConfig(TransportType.Udp);
            config.Channels[2] = new ChannelConfig(TransportType.Udp, ReliabilityMode.ReliableOrdered);
            config.Channels[3] = new ChannelConfig(TransportType.Tcp, encryption: true);
            config.Encryption = new EncryptionConfig {
                Mode = KeyExchangeMode.PresharedKey,
                PresharedKey = System.Text.Encoding.ASCII.GetBytes("benchmark-psk-benchmark-psk-32b!")
            };
            return config;
        }

        private static async Task MeasureEchoRateAsync(NetClient client, byte channel, int payloadSize, int messages, string label) {
            var payload = new byte[payloadSize];
            int received = 0;
            var done = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

            void OnData(byte ch, byte[] data) {
                if (ch == channel && Interlocked.Increment(ref received) >= messages) {
                    done.TrySetResult(true);
                }
            }

            client.DataReceived += OnData;
            // Window of in-flight messages: keeps pipes full without an unbounded queue.
            const int window = 64;
            int sent = 0;
            long allocatedBefore = GC.GetTotalAllocatedBytes(precise: true);
            var stopwatch = Stopwatch.StartNew();

            var pump = Task.Run(async () => {
                long lastProgressAt = Environment.TickCount64;
                int lastReceived = 0;
                while (Volatile.Read(ref sent) < messages && !done.Task.IsCompleted) {
                    int rec = Volatile.Read(ref received);
                    if (rec != lastReceived) {
                        lastReceived = rec;
                        lastProgressAt = Environment.TickCount64;
                    }
                    // UDP echoes can be lost: when the window is stuck for 500 ms,
                    // push on instead of waiting for replies that will never come.
                    if (Volatile.Read(ref sent) - rec >= window &&
                        Environment.TickCount64 - lastProgressAt < 500) {
                        await Task.Yield();
                        continue;
                    }
                    await client.SendAsync(channel, payload);
                    Interlocked.Increment(ref sent);
                }
            });

            await pump;
            // Grace period for the tail, then accept whatever loss UDP produced
            // (reliability is a stage-4 feature; the raw transport is fire-and-forget).
            await Task.WhenAny(done.Task, Task.Delay(2000));
            stopwatch.Stop();
            long allocatedAfter = GC.GetTotalAllocatedBytes(precise: true);
            client.DataReceived -= OnData;

            int completed = Math.Min(Volatile.Read(ref received), messages);
            double lossPercent = 100.0 * (messages - completed) / messages;
            double seconds = stopwatch.Elapsed.TotalSeconds;
            double msgPerSec = completed / seconds;
            double allocPerMsg = (allocatedAfter - allocatedBefore) / (double)(completed * 2L); // send + echo
            Console.WriteLine($"{label}:");
            Console.WriteLine($"  {msgPerSec,12:N0} msg/s round-trip   {msgPerSec * payloadSize / 1024 / 1024,8:N2} MB/s   {allocPerMsg,8:N0} B allocated/message (both sides)   loss {lossPercent:F2}%");
        }

        private static async Task MeasureBulkAsync(NetClient client, byte channel, long totalBytes, int chunk, string label) {
            var payload = new byte[chunk];
            new Random(1).NextBytes(payload);
            long receivedBytes = 0;
            var done = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

            void OnData(byte ch, byte[] data) {
                if (ch == channel && Interlocked.Add(ref receivedBytes, data.Length) >= totalBytes) {
                    done.TrySetResult(true);
                }
            }

            client.DataReceived += OnData;
            var stopwatch = Stopwatch.StartNew();
            var pump = Task.Run(async () => {
                for (long sent = 0; sent < totalBytes; sent += chunk) {
                    // Simple backpressure: don't run more than 8 chunks ahead of the echo.
                    while (sent - Interlocked.Read(ref receivedBytes) > 8L * chunk) {
                        await Task.Delay(1);
                    }
                    await client.SendAsync(channel, payload);
                }
            });

            bool finished = await Task.WhenAny(done.Task, Task.Delay(300_000)) == done.Task;
            stopwatch.Stop();
            await pump;
            client.DataReceived -= OnData;

            if (!finished) {
                Console.WriteLine($"{label}: TIMEOUT after {receivedBytes / 1024 / 1024} MB");
                return;
            }
            // Bytes crossed the loopback twice (there and back).
            double mbPerSec = totalBytes * 2 / stopwatch.Elapsed.TotalSeconds / 1024 / 1024;
            Console.WriteLine($"{label}:");
            Console.WriteLine($"  {mbPerSec,12:N0} MB/s through the stack (send+echo)");
        }

        private static async Task MeasureLatencyAsync(NetClient client, byte channel, int samples, string label) {
            var payload = new byte[1];
            var rtts = new double[samples];
            var echoed = new SemaphoreSlim(0);

            void OnData(byte ch, byte[] data) {
                if (ch == channel) {
                    echoed.Release();
                }
            }

            client.DataReceived += OnData;
            var stopwatch = new Stopwatch();
            for (int i = 0; i < samples; i++) {
                stopwatch.Restart();
                await client.SendAsync(channel, payload);
                await echoed.WaitAsync(5000);
                rtts[i] = stopwatch.Elapsed.TotalMilliseconds;
            }
            client.DataReceived -= OnData;

            Array.Sort(rtts);
            Console.WriteLine($"{label}:");
            Console.WriteLine($"  p50 {rtts[samples / 2]:F3} ms   p99 {rtts[(int)(samples * 0.99)]:F3} ms   max {rtts[samples - 1]:F3} ms");
        }
    }
}
