using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using NetSync.Peers;
using NetSync.Transports;
using Xunit;
using Xunit.Abstractions;

namespace NetSync.Tests {
    /// <summary>Allocation measurements are process-wide: run them alone, after the parallel tests.</summary>
    [CollectionDefinition(nameof(AllocationCollection), DisableParallelization = true)]
    public class AllocationCollection { }

    /// <summary>Stage 15: the steady-state data path allocates nothing; send buffers are reusable at once.</summary>
    [Collection(nameof(AllocationCollection))]
    public class ZeroAllocationTests {
        private readonly ITestOutputHelper _output;

        public ZeroAllocationTests(ITestOutputHelper output) => _output = output;

        /// <summary>A Debug build compiles every async method's state machine as a class: it allocates per call.</summary>
        private static readonly string BuildNote =
            System.Reflection.CustomAttributeExtensions.GetCustomAttribute<System.Diagnostics.DebuggableAttribute>(typeof(NetConfig).Assembly)
                ?.IsJITOptimizerDisabled == true
                ? " — this is a Debug build, whose async methods allocate; measure with -c Release"
                : "";

        private static NetConfig MakeConfig(bool authenticated = false) {
            var config = new NetConfig { EventDelivery = EventDelivery.Immediate, PingIntervalMs = 1000 };
            config.Channels[0] = new ChannelConfig(TransportType.Tcp);
            config.Channels[1] = new ChannelConfig(TransportType.Udp);
            config.Channels[2] = new ChannelConfig(TransportType.Udp, ReliabilityMode.ReliableOrdered);
            config.Channels[3] = new ChannelConfig(TransportType.Udp, ReliabilityMode.UnreliableSequenced);
            if (authenticated) {
                config.Encryption = new EncryptionConfig {
                    Mode = KeyExchangeMode.PresharedKey,
                    PresharedKey = System.Text.Encoding.ASCII.GetBytes("allocation-test-psk-32-bytes!!!!")
                };
            }
            return config;
        }

        [Theory]
        [InlineData(0, false)] // TCP
        [InlineData(1, false)] // UDP unreliable
        [InlineData(2, false)] // UDP reliable-ordered
        [InlineData(3, false)] // UDP sequenced
        [InlineData(0, true)]  // TCP, every packet authenticated
        [InlineData(2, true)]  // reliable UDP, every packet authenticated
        public async Task Echo_Steady_State_Allocates_Nothing_Per_Message(byte channel, bool authenticated) {
            using var server = new NetServer(MakeConfig(authenticated));
            server.MessageReceived += (connection, ch, data) => _ = connection.SendAsync(ch, data);
            int port = await server.StartAsync(0);

            using var client = new NetClient(MakeConfig(authenticated));
            using var echoes = new EchoCounter();
            client.MessageReceived += (ch, data) => echoes.Add();
            await client.ConnectAsync("127.0.0.1", port);

            double best = MeasureRoundTrip(client, channel, echoes, $"channel {channel}, authenticated={authenticated}");
            // Two messages per round trip (request + echo) through client and server. A single
            // per-message array would be >= 90 B; the budget only absorbs background timers.
            double budget = 16;
#if CORE_NETSTANDARD
            // netstandard2.1 has no allocation-free ReceiveFrom/SendTo: a UDP *server* pays for
            // an endpoint object per datagram (~150 B). Clients and TCP stay allocation-free.
            if (channel != 0) {
                int datagramsPerRoundTrip = channel == 2 ? 4 : 2; // reliable: data + ack each way
                budget = 160 * datagramsPerRoundTrip;
            }
#endif
            Assert.True(best < budget, $"{best:F1} B allocated per round trip (budget {budget}){BuildNote}");
            await server.StopAsync();
        }

        /// <summary>
        /// Bytes allocated per synchronous ping-pong in steady state — the best of a few
        /// windows: the first window of a fresh process can still catch one-time costs
        /// (pool buckets filled on new threads, tiering).
        /// </summary>
        /// <summary>Counts echoes and wakes the waiting measurement loop (without allocating).</summary>
        private sealed class EchoCounter : IDisposable {
            private readonly ManualResetEventSlim _arrived = new ManualResetEventSlim(false, spinCount: 0);
            private int _count;

            public int Count => Volatile.Read(ref _count);

            public void Add() {
                Interlocked.Increment(ref _count);
                _arrived.Set();
            }

            /// <summary>Waits until the count reaches <paramref name="target"/>; false after the timeout.</summary>
            public bool WaitFor(int target, int timeoutMs) {
                long deadline = Stopwatch.GetTimestamp() + timeoutMs * Stopwatch.Frequency / 1000;
                while (Count < target) {
                    long left = (deadline - Stopwatch.GetTimestamp()) * 1000 / Stopwatch.Frequency;
                    if (left <= 0) {
                        return false;
                    }
                    _arrived.Wait((int)left);
                    _arrived.Reset();
                }
                return true;
            }

            public void Dispose() => _arrived.Dispose();
        }

        private double MeasureRoundTrip(NetClient client, byte channel, EchoCounter echoes, string label) {
            var payload = new byte[64];
            return MeasureRoundTrip(() => client.SendAsync(channel, payload), echoes, label);
        }

        private double MeasureRoundTrip(Func<ValueTask> sendOne, EchoCounter echoes, string label) {
            // Blocking waits: the measuring loop itself must not allocate (an awaited timeout would),
            // and must not spin either — other test processes share the machine's cores.
            void PingPong(int count) {
                for (int i = 0; i < count; i++) {
                    int target = echoes.Count + 1;
                    var send = sendOne();
                    if (!send.IsCompletedSuccessfully) {
                        send.AsTask().GetAwaiter().GetResult();
                    }
                    echoes.WaitFor(target, 1000); // 1 s: a lost datagram
                }
            }

            PingPong(3000); // warm-up: JIT, pools, thread-pool growth

            // The counter is process-wide, and the test host shares the process: while it reports
            // finished tests it allocates tens of KB per second. A slow machine stretches every
            // window and catches more of that, so each window starts once the process is quiet —
            // nothing is subtracted: whatever the data path allocates still counts in full.
            const int measured = 5_000;
            double best = double.MaxValue;
            for (int window = 0; window < 4 && best >= 8; window++) {
                double quietMs = WaitUntilQuiet();
                long before = GC.GetTotalAllocatedBytes(precise: true);
                var clock = Stopwatch.StartNew();
                PingPong(measured);
                double elapsedMs = clock.Elapsed.TotalMilliseconds;
                double perMessage = (GC.GetTotalAllocatedBytes(precise: true) - before) / (double)measured;
                _output.WriteLine($"{label}, window {window}: {perMessage:F1} B per round trip ({elapsedMs:F0} ms, quiet after {quietMs:F0} ms)");
                best = Math.Min(best, perMessage);
            }
            if (best >= 8) {
                // Say what allocates: one more window with the runtime's allocation sampling on
                // (only now — the sampler allocates too, so it can't run while measuring).
                using var sampler = new AllocationSampler();
                PingPong(4 * measured);
                _output.WriteLine($"{label}, sampled allocations (one sample per ~100 KB): {sampler.Report()}");
            }
            return best;
        }

        /// <summary>
        /// Waits (up to 30 s) for a 250 ms stretch in which the process allocates under 4 KB — the
        /// connections' keepalive alone stays well below. Returns how long that took.
        /// </summary>
        private static double WaitUntilQuiet() {
            var clock = Stopwatch.StartNew();
            while (clock.ElapsedMilliseconds < 30_000) {
                long before = GC.GetTotalAllocatedBytes(precise: true);
                Thread.Sleep(250);
                if (GC.GetTotalAllocatedBytes(precise: true) - before < 4096) {
                    break;
                }
            }
            return clock.Elapsed.TotalMilliseconds;
        }

        /// <summary>Counts the runtime's GC allocation-tick samples by type.</summary>
        private sealed class AllocationSampler : System.Diagnostics.Tracing.EventListener {
            private readonly System.Collections.Concurrent.ConcurrentDictionary<string, int> _types =
                new System.Collections.Concurrent.ConcurrentDictionary<string, int>();

            protected override void OnEventSourceCreated(System.Diagnostics.Tracing.EventSource source) {
                if (source.Name == "Microsoft-Windows-DotNETRuntime") {
                    EnableEvents(source, System.Diagnostics.Tracing.EventLevel.Verbose, (System.Diagnostics.Tracing.EventKeywords)0x1); // GC
                }
            }

            protected override void OnEventWritten(System.Diagnostics.Tracing.EventWrittenEventArgs e) {
                if (e.EventName == null || !e.EventName.StartsWith("GCAllocationTick", StringComparison.Ordinal) || e.PayloadNames == null) {
                    return;
                }
                int index = e.PayloadNames.IndexOf("TypeName");
                string type = index >= 0 ? e.Payload?[index]?.ToString() ?? "?" : "?";
                _types.AddOrUpdate(type, 1, (_, count) => count + 1);
            }

            public string Report() {
                var top = System.Linq.Enumerable.Take(System.Linq.Enumerable.OrderByDescending(_types, pair => pair.Value), 12);
                return string.Join(", ", System.Linq.Enumerable.Select(top, pair => $"{pair.Key} x{pair.Value}"));
            }
        }

        private struct Position : NetSync.Messaging.INetMessage {
            public int Id;
            public float X, Y, Z;

            public void Write(NetSync.Serialization.NetWriter writer) => writer.WriteVarUInt((uint)Id).WriteSingle(X).WriteSingle(Y).WriteSingle(Z);

            public void Read(NetSync.Serialization.NetReader reader) {
                Id = (int)reader.ReadVarUInt();
                X = reader.ReadSingle();
                Y = reader.ReadSingle();
                Z = reader.ReadSingle();
            }
        }

        [Fact]
        public async Task Typed_Struct_Messages_Allocate_Nothing_Per_Message() {
            var registry = new NetSync.Messaging.NetMessageRegistry().Register<Position>(1);
            using var server = new NetServer(MakeConfig());
            using var serverMessenger = new NetSync.Messaging.NetServerMessenger(server, registry);
            serverMessenger.On<Position>((connection, position) => _ = serverMessenger.SendAsync(connection, 1, position));
            int port = await server.StartAsync(0);

            using var client = new NetClient(MakeConfig());
            using var clientMessenger = new NetSync.Messaging.NetClientMessenger(client, registry);
            using var echoes = new EchoCounter();
            clientMessenger.On<Position>(position => echoes.Add());
            await client.ConnectAsync("127.0.0.1", port);

            var message = new Position { Id = 7, X = 1, Y = 2, Z = 3 };
            double best = MeasureRoundTrip(() => clientMessenger.SendAsync(1, message), echoes, "typed UDP messages");
            double budget = 16;
#if CORE_NETSTANDARD
            budget = 160 * 2; // the UDP server's endpoint per datagram (see Echo_Steady_State)
#endif
            Assert.True(best < budget, $"{best:F1} B allocated per typed round trip (budget {budget}){BuildNote}");
        }

        [Theory]
        [InlineData(1, false)] // UDP through the relay
        [InlineData(2, false)] // reliable UDP through the relay
        [InlineData(1, true)]  // UDP on the hole-punched direct path
        [InlineData(0, false)] // TCP slot (always through the relay)
        public async Task Relayed_Echo_Allocates_Nothing_Per_Message(byte channel, bool direct) {
            using var relay = new NetSync.Relay.NetRelayServer();
            int relayPort = await relay.StartAsync(0);
            var relayEndPoint = new System.Net.IPEndPoint(System.Net.IPAddress.Loopback, relayPort);
            string roomId = NetSync.Relay.RelayRoom.NewRoomCode();

            using var host = new NetServer(MakeConfig());
            host.MessageReceived += (connection, ch, data) => _ = connection.SendAsync(ch, data);
            var hostRoom = new NetSync.Relay.RelayRoom(roomId) { RelayEndPoint = relayEndPoint, AllowDirect = direct };
            host.AddListener(TransportType.Tcp, hostRoom.Tcp, 0);
            host.AddListener(TransportType.Udp, hostRoom.Udp, 0);
            await host.StartAsync(0);

            var clientConfig = MakeConfig();
            var clientRoom = new NetSync.Relay.RelayRoom(roomId) { AllowDirect = direct };
            clientConfig.TransportFactories[TransportType.Tcp] = clientRoom.Tcp;
            clientConfig.TransportFactories[TransportType.Udp] = clientRoom.Udp;
            using var client = new NetClient(clientConfig);
            using var echoes = new EchoCounter();
            client.MessageReceived += (ch, data) => echoes.Add();
            await client.ConnectAsync(relayEndPoint);
            if (direct) {
                await Task.Delay(1000); // punching runs in the background
            }

            double best = MeasureRoundTrip(client, channel, echoes, $"relay channel {channel}, direct={direct}");
            double budget = 16;
#if CORE_NETSTANDARD
            if (channel != 0) {
                // No allocation-free ReceiveFrom/SendTo: every socket operation pays for an endpoint
                // (~130 B). Per message: send + receive, twice more when it passes the relay.
                int datagrams = channel == 2 ? 4 : 2; // reliable: data + ack each way
                budget = 160 * datagrams * (direct ? 2 : 4);
            }
#endif
            Assert.True(best < budget, $"{best:F1} B allocated per round trip through the relay (budget {budget}){BuildNote}");
        }

        [Theory]
        [InlineData(0, 6 * 1024 * 1024)]     // TCP: fragments wait for the send queue's byte budget
        [InlineData(2, 2 * 1024 * 1024)]     // reliable UDP: fragments wait for the window
        public async Task Send_Buffer_May_Be_Reused_As_Soon_As_SendAsync_Returns(byte channel, int size) {
            var messages = new System.Collections.Concurrent.ConcurrentQueue<byte[]>();
            var bothArrived = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            using var server = new NetServer(MakeConfig());
            server.DataReceived += (_, ch, data) => {
                if (ch == channel && data.Length == size) {
                    messages.Enqueue(data);
                    if (messages.Count == 2) {
                        bothArrived.TrySetResult(true);
                    }
                }
            };
            int port = await server.StartAsync(0);
            using var client = new NetClient(MakeConfig());
            await client.ConnectAsync("127.0.0.1", port);

            var buffer = new byte[size];
            buffer.AsSpan().Fill(0xAA);
            var fillerData = new byte[size];
            fillerData.AsSpan().Fill(0x11);
            var filler = client.SendAsync(channel, fillerData); // occupies the queue/window first
            var send = client.SendAsync(channel, buffer);
            buffer.AsSpan().Fill(0x55); // scribble over it before the send has finished
            await WithProgressReport(filler.AsTask(), client, server, "filler");
            await WithProgressReport(send.AsTask(), client, server, "send");

            Assert.Same(bothArrived.Task, await Task.WhenAny(bothArrived.Task, Task.Delay(30_000)));
            int intact = 0;
            foreach (var data in messages) {
                Assert.False(Array.Exists(data, b => b == 0x55), "the receiver saw bytes written after SendAsync returned");
                if (Array.TrueForAll(data, b => b == 0xAA)) {
                    intact++;
                }
            }
            Assert.Equal(1, intact);
            await server.StopAsync();
        }

        /// <summary>Waits for a send; if it doesn't finish, fails with what both sides were doing instead of hanging.</summary>
        private static async Task WithProgressReport(Task task, NetClient client, NetServer server, string what) {
            var history = new System.Text.StringBuilder();
            for (int second = 0; second < 60; second++) {
                if (await Task.WhenAny(task, Task.Delay(1000)) == task) {
                    await task;
                    return;
                }
                var connection = System.Linq.Enumerable.FirstOrDefault(server.Connections);
                history.Append($"[{second}s client {client.Statistics} | server {connection?.Statistics}] ");
            }
            throw new TimeoutException($"{what} did not finish in 60 s: {history}");
        }

        [Fact]
        public async Task Polled_Mode_Delivers_ZeroCopy_And_Owned_Payloads_Alike() {
            var config = MakeConfig();
            config.EventDelivery = EventDelivery.Polled;
            using var server = new NetServer(MakeConfig());
            server.MessageReceived += (connection, ch, data) => _ = connection.SendAsync(ch, data);
            int port = await server.StartAsync(0);

            using var client = new NetClient(config);
            byte[]? fromZeroCopy = null, fromOwned = null;
            client.MessageReceived += (ch, data) => fromZeroCopy = data.ToArray();
            client.DataReceived += (ch, data) => fromOwned = data;
            await client.ConnectAsync("127.0.0.1", port);
            client.PollEvents();

            var message = new byte[] { 1, 2, 3, 4, 5 };
            await client.SendAsync(0, message);
            var deadline = DateTime.UtcNow.AddSeconds(5);
            while (fromOwned == null && DateTime.UtcNow < deadline) {
                client.PollEvents();
                await Task.Delay(5);
            }
            Assert.Equal(message, fromZeroCopy);
            Assert.Equal(message, fromOwned);
            await server.StopAsync();
        }
    }
}
