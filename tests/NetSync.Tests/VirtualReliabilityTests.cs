using System;
using System.Collections.Generic;
using System.Linq;
using NetSync.Pipeline.Reliability;
using NetSync.Transports;
using Xunit;
using Xunit.Abstractions;

namespace NetSync.Tests {
    /// <summary>
    /// The reliability layer end to end in virtual time (<see cref="VirtualNetwork"/>): the same
    /// outcome on every machine, every run — thresholds here are exact, not "usually".
    /// </summary>
    public class VirtualReliabilityTests {
        private const byte Ordered = 1;
        private const byte Unordered = 2;
        private readonly ITestOutputHelper _output;

        public VirtualReliabilityTests(ITestOutputHelper output) => _output = output;

        private static Dictionary<byte, ChannelConfig> Channels() => new Dictionary<byte, ChannelConfig> {
            [Ordered] = new ChannelConfig(TransportType.Udp, ReliabilityMode.ReliableOrdered),
            [Unordered] = new ChannelConfig(TransportType.Udp, ReliabilityMode.Reliable)
        };

        /// <summary>A sender and a receiver; the receiver records (channel, number) of what it gets.</summary>
        private sealed class Pair {
            public readonly VirtualNetwork Network = new VirtualNetwork();
            public VirtualPath Forward, Backward;
            public readonly ReliableEndpoint Sender, Receiver;
            public readonly List<(byte Channel, int Number)> Received = new List<(byte, int)>();
            public DisconnectReason? SenderDead;
            public long SenderDeadAtMs;

            public Pair(int seed, bool congestionControl = true, int deadAfterMs = 5000) {
                Forward = new VirtualPath(Network, seed);
                Backward = new VirtualPath(Network, seed + 1);
                Sender = Network.Endpoint(Channels(), () => Forward, (_, _, _, _) => { },
                                          reason => { SenderDead = reason; SenderDeadAtMs = Network.NowMs; },
                                          congestionControl, deadAfterMs);
                Receiver = Network.Endpoint(Channels(), () => Backward,
                                            (channel, buffer, offset, count) => Received.Add((channel, BitConverter.ToInt32(buffer, offset))),
                                            congestionControl: congestionControl);
                Forward.Target = Receiver;
                Backward.Target = Sender;
            }

            /// <summary>Offers messages 0..count-1 whenever the window has room; runs until all arrived.</summary>
            public bool Transfer(byte channel, int count, long forMs, int size = 4, Func<int, bool>? beforeSending = null, int first = 0) {
                int next = first;
                var payload = new byte[size];
                return Network.RunUntil(() => Received.Count(m => m.Channel == channel) >= count, forMs, () => {
                    while (next < count && (beforeSending?.Invoke(next) ?? true)) {
                        BitConverter.GetBytes(next).CopyTo(payload, 0);
                        if (!Sender.TrySendReliable(channel, payload)) {
                            break;
                        }
                        next++;
                    }
                }, Sender, Receiver);
            }
        }

        [Fact]
        public void A_Lossy_Jittery_Link_Delivers_Everything_Once_And_In_Order() {
            var pair = new Pair(seed: 7);
            foreach (var path in new[] { pair.Forward, pair.Backward }) {
                path.LossRate = 0.2;
                path.DelayMs = 20;
                path.JitterMs = 15; // reorders packets, as the internet does
            }
            // Loss-based congestion control backs off hard at 20 % loss: slow, but it must get there.
            Assert.True(pair.Transfer(Ordered, 2000, forMs: 300_000), $"{pair.Received.Count} of 2000 in 300 virtual seconds");
            Assert.Equal(Enumerable.Range(0, 2000), pair.Received.Select(m => m.Number));
            Assert.Null(pair.SenderDead);
            _output.WriteLine($"{pair.Sender.Retransmissions} timeouts + {pair.Sender.FastRetransmissions} fast resends, " +
                              $"{pair.Network.NowMs - 1_000_000} virtual ms");
        }

        [Fact]
        public void An_Unordered_Reliable_Channel_Delivers_Each_Message_Exactly_Once() {
            var pair = new Pair(seed: 11);
            pair.Forward.LossRate = pair.Backward.LossRate = 0.2;
            pair.Forward.JitterMs = 30;
            Assert.True(pair.Transfer(Unordered, 1000, forMs: 300_000), $"{pair.Received.Count} of 1000");
            Assert.Equal(Enumerable.Range(0, 1000), pair.Received.Select(m => m.Number).OrderBy(n => n));
        }

        [Fact]
        public void A_Dead_Path_Is_Declared_Dead_Only_After_The_Silence_Allowance() {
            var pair = new Pair(seed: 3, deadAfterMs: 5000);
            Assert.True(pair.Transfer(Ordered, 100, forMs: 10_000));

            pair.Forward.Down = pair.Backward.Down = true;
            long lostAtMs = pair.Network.NowMs;
            pair.Transfer(Ordered, 101, forMs: 120_000, first: 100); // one more message, into the void
            Assert.Equal(DisconnectReason.Timeout, pair.SenderDead);
            long silentMs = pair.SenderDeadAtMs - lostAtMs;
            _output.WriteLine($"declared dead after {silentMs} virtual ms of silence");
            Assert.True(silentMs >= 5000, $"declared dead after only {silentMs} ms");
            Assert.True(silentMs < 60_000, $"took {silentMs} ms");
        }

        [Fact]
        public void A_Stall_Of_Our_Own_Process_Does_Not_Declare_The_Path_Dead() {
            var pair = new Pair(seed: 5, deadAfterMs: 5000);
            Assert.True(pair.Transfer(Ordered, 100, forMs: 10_000));

            // Messages in flight, then our process freezes for 30 s (GC, a loading screen, a
            // suspended laptop): no timer ran, so nothing was retried and nobody failed to answer.
            int inFlight = 100;
            while (pair.Sender.TrySendReliable(Ordered, BitConverter.GetBytes(inFlight))) {
                inFlight++;
            }
            Assert.True(inFlight >= 110, $"only {inFlight - 100} in flight");
            pair.Network.Stall(30_000);

            Assert.True(pair.Transfer(Ordered, 200, forMs: 30_000, first: inFlight));
            Assert.Null(pair.SenderDead);
            Assert.Equal(Enumerable.Range(0, 200), pair.Received.Select(m => m.Number));
        }

        [Fact]
        public void Unacknowledged_Packets_Resent_On_A_New_Path_Arrive_Once_And_In_Order() {
            var pair = new Pair(seed: 9, deadAfterMs: 60_000);
            Assert.True(pair.Transfer(Ordered, 500, forMs: 30_000));

            // The old path dies with a full window in flight...
            pair.Forward.Down = pair.Backward.Down = true;
            var oldForward = pair.Forward;
            int next = 500;
            pair.Network.RunUntil(() => false, 500, () => {
                while (next < 1000 && pair.Sender.TrySendReliable(Ordered, BitConverter.GetBytes(next))) {
                    next++;
                }
            }, pair.Sender, pair.Receiver);
            Assert.True(next > 500 && pair.Received.Count == 500, "packets should be stuck on the dead path");

            // ...the endpoints move to a new one (as when UDP is lost and TCP takes over) and
            // resend what is unacknowledged; the rest of the messages follow.
            pair.Forward = new VirtualPath(pair.Network, 100) { Target = pair.Receiver, DelayMs = 15 };
            pair.Backward = new VirtualPath(pair.Network, 101) { Target = pair.Sender, DelayMs = 15 };
            pair.Sender.ResendAllOnNewPath(pair.Network.NowMs);
            pair.Receiver.ResendAllOnNewPath(pair.Network.NowMs);
            Assert.True(pair.Transfer(Ordered, 1000, forMs: 30_000, first: next));

            Assert.Equal(Enumerable.Range(0, 1000), pair.Received.Select(m => m.Number));
            Assert.True(oldForward.Dropped > 0);
            Assert.True(pair.Network.RunUntil(() => pair.Sender.IsIdle, 5_000, null, pair.Sender, pair.Receiver)); // the last acks arrive
        }

        [Fact]
        public void Congestion_Control_Keeps_A_Bottleneck_From_Dropping_Packets() {
            double withControl = BottleneckLossRatio(congestionControl: true);
            double without = BottleneckLossRatio(congestionControl: false);
            _output.WriteLine($"retransmitted: with congestion control {withControl:P1}, without {without:P1}");
            Assert.True(withControl < 0.10, $"{withControl:P1} retransmitted with congestion control");
            Assert.True(withControl < without / 4, $"{withControl:P1} vs {without:P1}");
        }

        /// <summary>2 MB/s with a 50 ms drop-tail queue, 10 ms each way; 2000 × 1000-byte messages.</summary>
        private static double BottleneckLossRatio(bool congestionControl) {
            var pair = new Pair(seed: 1, congestionControl);
            pair.Forward.BytesPerMs = 2000;
            pair.Forward.QueueLimitMs = 50;
            Assert.True(pair.Transfer(Ordered, 2000, forMs: 120_000, size: 1000));
            Assert.Equal(Enumerable.Range(0, 2000), pair.Received.Select(m => m.Number));
            return (double)(pair.Sender.Retransmissions + pair.Sender.FastRetransmissions) / pair.Sender.PacketsSent;
        }
    }
}
