using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using NetSync.Diagnostics;
using NetSync.Peers;
using NetSync.Pipeline.Reliability;
using NetSync.Sending;
using NetSync.Streams;
using Xunit;
using Xunit.Abstractions;

namespace NetSync.Tests {
    /// <summary>Stage 12: congestion control, priorities, unreliable fragmentation, streaming.</summary>
    public class TrafficTests {
        private const byte Bulk = 5;
        private const byte Probe = 6;
        private readonly ITestOutputHelper _output;

        public TrafficTests(ITestOutputHelper output) => _output = output;

        private static Dictionary<byte, ChannelConfig> Channels(ChannelPriority bulkPriority) {
            return new Dictionary<byte, ChannelConfig> {
                [Bulk] = new ChannelConfig(TransportType.Udp, ReliabilityMode.ReliableOrdered, priority: bulkPriority),
                [Probe] = new ChannelConfig(TransportType.Udp, ReliabilityMode.UnreliableSequenced)
            };
        }

        private sealed class Link : IDisposable {
            public ReliableEndpoint Sender = null!;
            public ReliableEndpoint Receiver = null!;
            public BottleneckPipe Forward = null!;
            public BottleneckPipe Backward = null!;

            public void Dispose() {
                Sender.Dispose();
                Receiver.Dispose();
                Forward.Dispose();
                Backward.Dispose();
            }
        }

        private static Link CreateLink(ChannelPriority bulkPriority, bool congestionControl, double queueMs,
                                       Action<byte, byte[]> deliverAtReceiver) {
            var link = new Link {
                Forward = new BottleneckPipe(bytesPerSecond: 2_000_000, maxQueueMs: queueMs, delayMs: 10),
                Backward = new BottleneckPipe(bytesPerSecond: 0, maxQueueMs: 0, delayMs: 10)
            };
            link.Sender = new ReliableEndpoint(Channels(bulkPriority), link.Forward.Send, (_, _) => { }, () => 20,
                _ => { }, NullNetLogger.Instance, congestionControl: congestionControl);
            link.Receiver = new ReliableEndpoint(Channels(bulkPriority), link.Backward.Send, deliverAtReceiver, () => 20,
                _ => { }, NullNetLogger.Instance, congestionControl: congestionControl);
            link.Forward.Target = link.Receiver;
            link.Backward.Target = link.Sender;
            link.Sender.Start();
            link.Receiver.Start();
            return link;
        }

        [Fact]
        public async Task Congestion_Control_Avoids_Retransmission_Storms_On_A_Bottleneck() {
            var (ccRatio, ccSeconds) = await MeasureBulkTransferAsync(congestionControl: true);
            var (rawRatio, rawSeconds) = await MeasureBulkTransferAsync(congestionControl: false);
            _output.WriteLine($"with CC:    {ccRatio:P1} retransmitted, {ccSeconds:F2} s");
            _output.WriteLine($"without CC: {rawRatio:P1} retransmitted, {rawSeconds:F2} s");

            Assert.True(ccRatio < 0.15, $"with congestion control {ccRatio:P1} of packets were retransmitted");
            Assert.True(ccRatio < rawRatio / 2, $"CC {ccRatio:P1} vs no CC {rawRatio:P1}");
        }

        private async Task<(double RetransmitRatio, double Seconds)> MeasureBulkTransferAsync(bool congestionControl) {
            var done = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
            using var link = CreateLink(ChannelPriority.Normal, congestionControl, queueMs: 50,
                (channel, payload) => { if (channel == Bulk) done.TrySetResult(payload); });

            var data = new byte[3 * 1024 * 1024]; // 3 MB over a 2 MB/s bottleneck
            new Random(5).NextBytes(data);
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            await link.Sender.SendReliableAsync(Bulk, data);
            Assert.Same(done.Task, await Task.WhenAny(done.Task, Task.Delay(60_000)));
            Assert.Equal(data, await done.Task);

            double retransmitted = link.Sender.Retransmissions + link.Sender.FastRetransmissions;
            return (retransmitted / link.Sender.PacketsSent, stopwatch.Elapsed.TotalSeconds);
        }

        [Fact]
        public async Task Low_Priority_Bulk_Keeps_Queueing_Delay_Low_For_Realtime_Traffic() {
            double normal = await MeasureProbeDelayAsync(ChannelPriority.Normal);
            double low = await MeasureProbeDelayAsync(ChannelPriority.Low);
            _output.WriteLine($"median queueing delay seen by real-time probes: bulk Normal {normal:F1} ms, bulk Low {low:F1} ms");

            // No comparison with Normal here: a busy machine can't saturate the path in real time,
            // so its baseline proves nothing. Low_Priority_Window_Leaves_The_Queue_Empty compares them.
            Assert.True(low < 60, $"Low-priority bulk still caused {low:F1} ms of queueing delay");
        }

        [Fact]
        public void Low_Priority_Window_Leaves_The_Queue_Empty() {
            double normal = SimulateQueueingDelayMs(background: false);
            double low = SimulateQueueingDelayMs(background: true);
            _output.WriteLine($"median queueing delay in virtual time: Normal {normal:F1} ms, Low {low:F1} ms");

            Assert.True(low < 60, $"Low-priority window kept {low:F1} ms queued");
            Assert.True(low < normal / 2, $"Low {low:F1} ms vs Normal {normal:F1} ms");
        }

        /// <summary>
        /// A bulk sender on the bottleneck of <see cref="CreateLink"/>, in virtual time: the real
        /// congestion window without threads or timers, so the machine can't affect the result.
        /// </summary>
        private static double SimulateQueueingDelayMs(bool background) {
            const double bytesPerMs = 2000, packetBytes = 1100, oneWayMs = 10, queueLimitMs = 300, durationMs = 10_000;
            var rtt = new RttEstimator();
            var window = new CongestionWindow(background, maxWindow: ReliableEndpoint.WindowSize);
            var events = new PriorityQueue<(bool Lost, double SentAtMs), double>();
            var samples = new List<double>();
            double now = 0, lastDeparture = 0, nextSample = 0;
            int inFlight = 0;

            void Fill() {
                while (inFlight < window.AllowedInFlight) {
                    inFlight++;
                    if (Math.Max(0, lastDeparture - now) > queueLimitMs) {
                        events.Enqueue((true, now), now + queueLimitMs + 2 * oneWayMs); // drop-tail: noticed about an RTT later
                        continue;
                    }
                    lastDeparture = Math.Max(now, lastDeparture) + packetBytes / bytesPerMs;
                    events.Enqueue((false, now), lastDeparture + 2 * oneWayMs);
                }
            }

            Fill();
            while (now < durationMs && events.TryDequeue(out var item, out double at)) {
                for (; nextSample < at; nextSample += 10) {
                    samples.Add(Math.Max(0, lastDeparture - nextSample)); // a probe sent then waits this long
                }
                now = at;
                inFlight--;
                if (item.Lost) {
                    window.OnLoss(rtt, (long)now);
                }
                else {
                    rtt.AddSample(now - item.SentAtMs, (long)now);
                    window.OnAcked(1, rtt, (long)now);
                }
                Fill();
            }

            var settled = samples.Skip(samples.Count / 3).OrderBy(sample => sample).ToList(); // skip ramp-up
            return settled[settled.Count / 2];
        }

        private async Task<double> MeasureProbeDelayAsync(ChannelPriority bulkPriority) {
            using var link = CreateLink(bulkPriority, congestionControl: true, queueMs: 300, (_, _) => { });
            link.Forward.ProbeChannel = Probe;

            var bulk = link.Sender.SendReliableAsync(Bulk, new byte[8 * 1024 * 1024]).AsTask(); // more than fits in the test window
            var probe = new byte[100];
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            while (stopwatch.ElapsedMilliseconds < 3000) {
                await link.Sender.SendSequencedAsync(Probe, probe);
                await Task.Delay(10);
            }
            link.Sender.Dispose(); // stop the bulk sender
            try {
                await bulk;
            }
            catch (InvalidOperationException) { }

            List<double> samples;
            lock (link.Forward.ProbeQueueingDelaysMs) {
                samples = link.Forward.ProbeQueueingDelaysMs.Skip(link.Forward.ProbeQueueingDelaysMs.Count / 3).ToList(); // skip ramp-up
            }
            samples.Sort();
            return samples[samples.Count / 2];
        }

        [Fact]
        public async Task UnreliableFragmented_Delivers_Whole_Messages_Drops_Incomplete_And_Stale() {
            var channels = new Dictionary<byte, ChannelConfig> {
                [7] = new ChannelConfig(TransportType.Udp, ReliabilityMode.UnreliableFragmented)
            };
            var captured = new List<byte[]>();
            var delivered = new List<byte[]>();
            using var sender = new ReliableEndpoint(channels, message => { captured.Add(message.ToArray()); return default; },
                (_, _) => { }, () => 5, _ => { }, NullNetLogger.Instance);
            using var receiver = new ReliableEndpoint(channels, _ => default, (_, payload) => delivered.Add(payload),
                () => 5, _ => { }, NullNetLogger.Instance);

            List<byte[]> Send(byte[] message) {
                captured.Clear();
                sender.SendUnreliableFragmentedAsync(7, message).AsTask().GetAwaiter().GetResult();
                return new List<byte[]>(captured);
            }
            void Deliver(IEnumerable<byte[]> packets) {
                foreach (var packet in packets) {
                    receiver.HandleRelData(packet, 0, packet.Length);
                }
            }

            var frameA = MakeFrame(30_000, 1);  // ~28 fragments
            var frameB = MakeFrame(30_000, 2);
            var frameC = MakeFrame(500, 3);     // single fragment
            var frameD = MakeFrame(10_000, 4);
            var frameE = MakeFrame(10_000, 5);
            var a = Send(frameA);
            var b = Send(frameB);
            var c = Send(frameC);
            var d = Send(frameD);
            var e = Send(frameE);
            Assert.True(a.Count > 20);

            Deliver(a);                 // complete → delivered
            Deliver(b.Skip(1));         // one fragment lost → never delivered
            Deliver(c);                 // newer, complete → delivered
            Deliver(e);                 // newest → delivered
            Deliver(d);                 // completes after a newer one → stale, dropped

            Assert.Equal(3, delivered.Count);
            Assert.Equal(frameA, delivered[0]);
            Assert.Equal(frameC, delivered[1]);
            Assert.Equal(frameE, delivered[2]);
            await Task.CompletedTask;
        }

        [Fact]
        public async Task UnreliableFragmented_Channel_Carries_Large_Frames_EndToEnd() {
            var config = new NetConfig { EventDelivery = EventDelivery.Immediate };
            config.Channels[0] = new ChannelConfig(TransportType.Tcp);
            config.Channels[3] = new ChannelConfig(TransportType.Udp, ReliabilityMode.UnreliableFragmented);
            using var server = new NetServer(config);
            server.DataReceived += (conn, channel, data) => _ = conn.SendAsync(channel, data);
            int port = await server.StartAsync(0);

            using var client = new NetClient(config);
            var echo = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
            client.DataReceived += (_, data) => echo.TrySetResult(data);
            await client.ConnectAsync("127.0.0.1", port);

            var frame = MakeFrame(120_000, 9); // a video-frame-sized message, ~110 datagrams
            await client.SendAsync(3, frame);
            Assert.Same(echo.Task, await Task.WhenAny(echo.Task, Task.Delay(10_000)));
            Assert.Equal(frame, await echo.Task);
            await server.StopAsync();
        }

        [Fact]
        public async Task High_Priority_Tcp_Channel_Overtakes_Queued_Normal_Traffic() {
            var config = new NetConfig { EventDelivery = EventDelivery.Immediate };
            config.Channels[0] = new ChannelConfig(TransportType.Tcp);
            config.Channels[1] = new ChannelConfig(TransportType.Tcp, priority: ChannelPriority.High);
            using var server = new NetServer(config);
            var arrivals = new ConcurrentQueue<byte>();
            var gotHigh = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            int normalReceived = 0;
            // The server stops reading at the first message until everything is queued: TCP
            // backpressure then guarantees a backlog in the client's send queue, which is
            // exactly where priority has to act — independent of machine speed.
            var gate = new ManualResetEventSlim(false);
            server.DataReceived += (_, channel, _) => {
                gate.Wait(10_000);
                if (channel == 1) {
                    arrivals.Enqueue(1);
                    gotHigh.TrySetResult(true);
                }
                else {
                    Interlocked.Increment(ref normalReceived);
                    arrivals.Enqueue(0);
                }
            };
            int port = await server.StartAsync(0);

            using var client = new NetClient(config);
            await client.ConnectAsync("127.0.0.1", port);

            const int normalCount = 9000;
            var payload = new byte[4096];
            for (int i = 0; i < normalCount; i++) {
                await client.SendAsync(0, payload); // queued instantly, drains over tens of ms
            }
            await client.SendAsync(1, new byte[] { 42 });
            gate.Set();

            Assert.True(await Task.WhenAny(gotHigh.Task, Task.Delay(10_000)) == gotHigh.Task);
            int position = arrivals.ToList().IndexOf(1);
            _output.WriteLine($"High-priority message arrived at position {position} of {normalCount + 1}");
            Assert.True(position < normalCount - 500, $"High-priority message arrived at position {position}");
            await server.StopAsync();
        }

        [Fact]
        public async Task SendQueue_Bulk_Is_Bounded_In_Bytes() {
            int sent = 0;
            using var queue = new SendQueue((_, _, _) => {
                Interlocked.Increment(ref sent);
                return Task.CompletedTask;
            }, NullNetLogger.Instance, maxQueueSize: 1000, maxQueuedBytes: 100);

            await queue.EnqueueAsync(new byte[60], 60, false, SendPriority.Low, CancellationToken.None);
            var second = queue.EnqueueAsync(new byte[60], 60, false, SendPriority.Low, CancellationToken.None).AsTask();
            await Task.Delay(100);
            Assert.False(second.IsCompleted); // 120 bytes > 100-byte budget: must wait
            Assert.Equal(60, queue.QueuedBytes);

            queue.Start();
            Assert.Same(second, await Task.WhenAny(second, Task.Delay(5000)));
            await second;
        }

        [Theory]
        [InlineData(0, 20 * 1024 * 1024)] // TCP
        [InlineData(2, 3 * 1024 * 1024)]  // reliable-ordered UDP, low priority
        public async Task Stream_Transfers_Large_Data_Intact(byte channel, int size) {
            var config = new NetConfig { EventDelivery = EventDelivery.Immediate };
            config.Channels[0] = new ChannelConfig(TransportType.Tcp, priority: ChannelPriority.Low);
            config.Channels[2] = new ChannelConfig(TransportType.Udp, ReliabilityMode.ReliableOrdered, priority: ChannelPriority.Low);

            var destination = new MemoryStream();
            var completed = new TaskCompletionSource<NetStreamInfo>(TaskCreationOptions.RunContinuationsAsynchronously);
            var receiver = new NetStreamReceiver(_ => destination) { DisposeDestinations = false };
            receiver.StreamCompleted += info => completed.TrySetResult(info);
            receiver.StreamFailed += (_, reason) => completed.TrySetException(new Exception(reason));

            using var server = new NetServer(config);
            server.DataReceived += (_, ch, data) => {
                if (ch == channel) receiver.HandleMessage(data);
            };
            int port = await server.StartAsync(0);

            using var client = new NetClient(config);
            await client.ConnectAsync("127.0.0.1", port);

            var source = new byte[size];
            new Random(13).NextBytes(source);
            await client.SendStreamAsync(channel, new MemoryStream(source), "payload.bin");

            Assert.Same(completed.Task, await Task.WhenAny(completed.Task, Task.Delay(60_000)));
            var info = await completed.Task;
            Assert.Equal("payload.bin", info.Name);
            Assert.Equal(size, info.Length);
            Assert.Equal(SHA256.HashData(source), SHA256.HashData(destination.ToArray()));
            await server.StopAsync();
        }

        [Fact]
        public async Task Failing_Source_Aborts_The_Stream_On_The_Receiver() {
            var config = new NetConfig { EventDelivery = EventDelivery.Immediate };
            config.Channels[0] = new ChannelConfig(TransportType.Tcp);
            var failed = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
            var receiver = new NetStreamReceiver(_ => new MemoryStream());
            receiver.StreamFailed += (_, reason) => failed.TrySetResult(reason);

            using var server = new NetServer(config);
            server.DataReceived += (_, _, data) => receiver.HandleMessage(data);
            int port = await server.StartAsync(0);
            using var client = new NetClient(config);
            await client.ConnectAsync("127.0.0.1", port);

            await Assert.ThrowsAsync<IOException>(() => client.SendStreamAsync(0, new FailingStream(failAfter: 600_000)));
            Assert.Same(failed.Task, await Task.WhenAny(failed.Task, Task.Delay(5000)));
            Assert.Equal(0, receiver.ActiveStreams);
            await server.StopAsync();
        }

        /// <summary>The wire messages of one stream, as SendStreamAsync produces them.</summary>
        private static async Task<List<byte[]>> CaptureStreamAsync(int size, string name) {
            var messages = new List<byte[]>();
            await NetStreams.SendAsync((data, _) => { messages.Add(data.ToArray()); return default; },
                                       new MemoryStream(new byte[size]), name, 1024, CancellationToken.None);
            return messages;
        }

        [Fact]
        public async Task Receiver_Refuses_Streams_Beyond_Its_Limit_And_Survives_Bad_Input() {
            int opened = 0;
            var receiver = new NetStreamReceiver(_ => { opened++; return new MemoryStream(); }) { MaxActiveStreams = 2 };
            int refused = 0;
            receiver.StreamRefused += _ => refused++;

            var streams = new List<List<byte[]>>();
            for (int i = 0; i < 3; i++) {
                streams.Add(await CaptureStreamAsync(3000, $"file{i}"));
            }
            foreach (var stream in streams) {
                receiver.HandleMessage(stream[0]); // three announcements
            }
            Assert.Equal(2, opened);
            Assert.Equal(1, refused);
            Assert.Equal(2, receiver.ActiveStreams);

            // A truncated announcement is ignored instead of throwing into the caller.
            receiver.HandleMessage(new byte[] { NetStreams.KindBegin, 9, 0, 0, 0, 1 });
            Assert.Equal(2, receiver.ActiveStreams);

            // A destination that fails to write ends that stream with StreamFailed.
            var failing = new NetStreamReceiver(_ => new MemoryStream(new byte[10], writable: true));
            string? failure = null;
            failing.StreamFailed += (_, reason) => failure = reason;
            foreach (var message in streams[0]) {
                failing.HandleMessage(message);
            }
            Assert.Contains("writing the destination failed", failure);
            Assert.Equal(0, failing.ActiveStreams);
        }

        private sealed class FailingStream : Stream {
            private readonly int _failAfter;
            private int _read;
            public FailingStream(int failAfter) => _failAfter = failAfter;
            public override int Read(byte[] buffer, int offset, int count) {
                if (_read >= _failAfter) {
                    throw new IOException("disk on fire");
                }
                int n = Math.Min(count, _failAfter - _read);
                _read += n;
                return n;
            }
            public override bool CanRead => true;
            public override bool CanSeek => false;
            public override bool CanWrite => false;
            public override long Length => throw new NotSupportedException();
            public override long Position { get => _read; set => throw new NotSupportedException(); }
            public override void Flush() { }
            public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
            public override void SetLength(long value) => throw new NotSupportedException();
            public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        }

        private static byte[] MakeFrame(int size, int seed) {
            var frame = new byte[size];
            new Random(seed).NextBytes(frame);
            return frame;
        }
    }
}
