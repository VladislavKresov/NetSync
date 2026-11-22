using System;
using System.Collections.Generic;
using System.Linq;
using NetSync.Pipeline.Reliability;
using NetSync.Transports;
using Xunit;
using Xunit.Abstractions;

namespace NetSync.Tests {
    /// <summary>
    /// Bulk throughput of a reliable ordered channel over links like a VPN tunnel (Hamachi,
    /// Radmin: 20–60 ms, some loss, some reordering), in virtual time — exact, any machine.
    /// The bars are what users need from a file transfer over such a link.
    /// </summary>
    public class ThroughputTests {
        private const byte Channel = 1;
        private const int PacketBytes = 1000;
        private readonly ITestOutputHelper _output;

        public ThroughputTests(ITestOutputHelper output) => _output = output;

        /// <summary>KB/s for <paramref name="megabytes"/> MB over a link with the given one-way delay, loss and jitter.</summary>
        internal static (double KBps, long Resent, long Sent) Measure(int oneWayMs, double loss, int jitterMs, double megabytes = 2,
                                                                       double bytesPerMs = 0, double queueLimitMs = 0, int seed = 1) {
            var network = new VirtualNetwork();
            var channels = new Dictionary<byte, ChannelConfig> { [Channel] = new ChannelConfig(TransportType.Udp, ReliabilityMode.ReliableOrdered) };
            var forward = new VirtualPath(network, seed) { DelayMs = oneWayMs, LossRate = loss, JitterMs = jitterMs, BytesPerMs = bytesPerMs, QueueLimitMs = queueLimitMs };
            var backward = new VirtualPath(network, seed + 1) { DelayMs = oneWayMs, LossRate = loss, JitterMs = jitterMs };
            int count = (int)(megabytes * 1024 * 1024 / PacketBytes);
            int received = 0;
            var sender = network.Endpoint(channels, () => forward, (_, _, _, _) => { }, deadAfterMs: 600_000);
            var receiver = network.Endpoint(channels, () => backward, (_, _, _, _) => received++, deadAfterMs: 600_000);
            forward.Target = receiver;
            backward.Target = sender;

            var payload = new byte[PacketBytes];
            int next = 0;
            long start = network.NowMs;
            bool done = network.RunUntil(() => received >= count, 600_000, () => {
                while (next < count && sender.TrySendReliable(Channel, payload)) {
                    next++;
                }
            }, sender, receiver);
            Assert.True(done, $"only {received} of {count} packets in 600 virtual seconds");
            double seconds = Math.Max(0.001, (network.NowMs - start) / 1000.0);
            return (count * PacketBytes / 1024.0 / seconds, sender.Retransmissions + sender.FastRetransmissions, sender.PacketsSent);
        }

        private void Report(string name, (double KBps, long Resent, long Sent) result) {
            _output.WriteLine($"{name}: {result.KBps:F0} KB/s, {result.Resent} resent of {result.Sent} sent ({100.0 * result.Resent / result.Sent:F1} %)");
        }

        [Theory]
        // one-way ms, loss, jitter ms, minimum KB/s
        [InlineData(20, 0.00, 0, 2000)]  // a clean 40 ms round trip
        [InlineData(20, 0.01, 0, 1000)]  // was ~130 KB/s
        [InlineData(20, 0.03, 0, 500)]   // was ~65 KB/s
        [InlineData(0, 0.03, 0, 2000)]   // loss without delay
        [InlineData(30, 0.01, 0, 400)]   // a 60 ms tunnel
        public void Reliable_Throughput_Over_A_Lossy_Delayed_Link(int oneWayMs, double loss, int jitterMs, double minimumKBps) {
            var result = Measure(oneWayMs, loss, jitterMs);
            Report($"{2 * oneWayMs} ms RTT, {loss:P0} loss, {jitterMs} ms jitter", result);
            Assert.True(result.KBps >= minimumKBps, $"{result.KBps:F0} KB/s, wanted at least {minimumKBps}");
        }

        [Fact]
        public void Reordering_Alone_Does_Not_Cost_Throughput() {
            var steady = Measure(10, 0, 0);
            var reordered = Measure(10, 0, 5); // jitter reorders packets; nothing is lost
            Report("no jitter", steady);
            Report("5 ms jitter", reordered);
            Assert.True(reordered.KBps >= 0.7 * steady.KBps, $"{reordered.KBps:F0} KB/s with reordering vs {steady.KBps:F0} without");
            Assert.True(reordered.Resent <= reordered.Sent / 20, $"{reordered.Resent} needless resends of {reordered.Sent}");
        }

        [Fact]
        public void A_Real_Bottleneck_Still_Gets_Congestion_Control() {
            // 2 MB/s with a 50 ms drop-tail queue: here losses mean congestion, and the sender must back off.
            var result = Measure(10, 0, 0, megabytes: 4, bytesPerMs: 2000, queueLimitMs: 50);
            Report("2 MB/s bottleneck", result);
            Assert.True(result.KBps >= 0.8 * 2000 * 1000 / 1024.0, $"{result.KBps:F0} KB/s through a 2 MB/s bottleneck");
            Assert.True(result.Resent <= result.Sent / 20, $"{result.Resent} resent of {result.Sent}");
        }
    }
}
