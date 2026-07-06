using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NetSync;
using NetSync.Diagnostics;
using NetSync.Peers;
using NetSync.Pipeline.Reliability;
using NetSync.Transports;
using Xunit;

namespace NetSync.Tests {
    public class ReliableEndpointTests {
        private const byte Channel = 5;

        /// <summary>
        /// In-memory datagram link with seeded loss/duplication. Delivery is serialized
        /// by a lock, mimicking a transport's single receive thread.
        /// </summary>
        private sealed class LossyPipe {
            private readonly Random _random;
            private readonly double _lossRate;
            private readonly double _dupRate;
            private readonly object _deliverLock = new object();
            public ReliableEndpoint? Target;
            public int Dropped;

            public LossyPipe(int seed, double lossRate, double dupRate) {
                _random = new Random(seed);
                _lossRate = lossRate;
                _dupRate = dupRate;
            }

            public ValueTask Send(ReadOnlyMemory<byte> message) {
                bool drop, dup;
                lock (_random) {
                    drop = _random.NextDouble() < _lossRate;
                    dup = _random.NextDouble() < _dupRate;
                }
                if (drop) {
                    Interlocked.Increment(ref Dropped);
                    return default;
                }
                Deliver(message.ToArray());
                if (dup) {
                    Deliver(message.ToArray());
                }
                return default;
            }

            private void Deliver(byte[] packet) {
                var target = Target;
                if (target == null) {
                    return;
                }
                lock (_deliverLock) {
                    if (packet[0] == 0x20) {
                        target.HandleRelData(packet, 0, packet.Length);
                    }
                    else if (packet[0] == 0x21) {
                        target.HandleAck(packet, 0, packet.Length);
                    }
                }
            }
        }

        private static Dictionary<byte, ChannelConfig> Channels(ReliabilityMode mode) {
            return new Dictionary<byte, ChannelConfig> {
                [Channel] = new ChannelConfig(TransportType.Udp, mode)
            };
        }

        private static (ReliableEndpoint a, ReliableEndpoint b, LossyPipe aToB, LossyPipe bToA) CreatePair(
            ReliabilityMode mode, double lossRate, double dupRate, Action<byte, byte[]> deliverAtB,
            Action<DisconnectReason>? onDeadA = null, int maxRetransmits = 12) {
            var aToB = new LossyPipe(seed: 1234, lossRate, dupRate);
            var bToA = new LossyPipe(seed: 5678, lossRate, dupRate);

            var a = new ReliableEndpoint(Channels(mode), aToB.Send, (_, _) => { }, () => 5,
                onDeadA ?? (_ => { }), NullNetLogger.Instance, maxRetransmits, retransmitScanMs: 10, rtoFloorMs: 30);
            var b = new ReliableEndpoint(Channels(mode), bToA.Send, deliverAtB, () => 5,
                _ => { }, NullNetLogger.Instance, maxRetransmits, retransmitScanMs: 10, rtoFloorMs: 30);

            aToB.Target = b;
            bToA.Target = a;
            a.Start();
            b.Start();
            return (a, b, aToB, bToA);
        }

        [Fact]
        public async Task ReliableOrdered_Delivers_All_In_Order_Under_Loss_And_Dups() {
            const int messageCount = 400;
            var received = new List<int>();
            var (a, b, aToB, _) = CreatePair(ReliabilityMode.ReliableOrdered, lossRate: 0.25, dupRate: 0.10,
                (_, payload) => { lock (received) received.Add(BitConverter.ToInt32(payload)); });

            using (a)
            using (b) {
                for (int i = 0; i < messageCount; i++) {
                    await a.SendReliableAsync(Channel, BitConverter.GetBytes(i));
                }
                await WaitUntilAsync(() => { lock (received) return received.Count >= messageCount; }, 30_000);
            }

            Assert.True(aToB.Dropped > 0, "the pipe should actually have dropped packets");
            lock (received) {
                Assert.Equal(messageCount, received.Count);
                Assert.Equal(Enumerable.Range(0, messageCount), received);
            }
        }

        [Fact]
        public async Task Reliable_Unordered_Delivers_Everything_Exactly_Once() {
            const int messageCount = 400;
            var received = new ConcurrentBag<int>();
            var (a, b, aToB, _) = CreatePair(ReliabilityMode.Reliable, lossRate: 0.25, dupRate: 0.10,
                (_, payload) => received.Add(BitConverter.ToInt32(payload)));

            using (a)
            using (b) {
                for (int i = 0; i < messageCount; i++) {
                    await a.SendReliableAsync(Channel, BitConverter.GetBytes(i));
                }
                await WaitUntilAsync(() => received.Count >= messageCount, 30_000);
            }

            Assert.True(aToB.Dropped > 0);
            Assert.Equal(messageCount, received.Count); // exactly once: no dup deliveries
            Assert.Equal(Enumerable.Range(0, messageCount).OrderBy(x => x), received.OrderBy(x => x));
        }

        [Fact]
        public async Task Large_Message_Is_Fragmented_And_Reassembled_Under_Loss() {
            var data = new byte[300_000]; // ~273 fragments
            new Random(7).NextBytes(data);
            byte[]? result = null;
            var done = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

            var (a, b, aToB, _) = CreatePair(ReliabilityMode.ReliableOrdered, lossRate: 0.10, dupRate: 0.05,
                (_, payload) => { result = payload; done.TrySetResult(true); });

            using (a)
            using (b) {
                await a.SendReliableAsync(Channel, data);
                Assert.Same(done.Task, await Task.WhenAny(done.Task, Task.Delay(30_000)));
            }

            Assert.True(aToB.Dropped > 0);
            Assert.Equal(data, result);
        }

        [Fact]
        public async Task Sequenced_Delivers_Newest_And_Drops_Stale() {
            // Manual pipe: capture packets, deliver out of order (0, then 2, then 1).
            var captured = new List<byte[]>();
            ReliableEndpoint? receiver = null;
            var received = new List<int>();

            var sender = new ReliableEndpoint(
                Channels(ReliabilityMode.UnreliableSequenced),
                message => { lock (captured) captured.Add(message.ToArray()); return default; },
                (_, _) => { }, () => 5, _ => { }, NullNetLogger.Instance);
            receiver = new ReliableEndpoint(
                Channels(ReliabilityMode.UnreliableSequenced),
                _ => default,
                (_, payload) => received.Add(BitConverter.ToInt32(payload)),
                () => 5, _ => { }, NullNetLogger.Instance);

            using (sender)
            using (receiver) {
                for (int i = 0; i < 3; i++) {
                    await sender.SendSequencedAsync(Channel, BitConverter.GetBytes(i));
                }
                Assert.Equal(3, captured.Count);

                receiver.HandleRelData(captured[0], 0, captured[0].Length); // seq 0 → delivered
                receiver.HandleRelData(captured[2], 0, captured[2].Length); // seq 2 → delivered
                receiver.HandleRelData(captured[1], 0, captured[1].Length); // seq 1 → stale, dropped
            }

            Assert.Equal(new[] { 0, 2 }, received);
        }

        [Fact]
        public async Task Sequenced_Rejects_Oversized_Payload() {
            using var endpoint = new ReliableEndpoint(
                Channels(ReliabilityMode.UnreliableSequenced),
                _ => default, (_, _) => { }, () => 5, _ => { }, NullNetLogger.Instance);

            await Assert.ThrowsAsync<ArgumentException>(async () =>
                await endpoint.SendSequencedAsync(Channel, new byte[ReliableEndpoint.FragmentThreshold + 1]));
        }

        [Fact]
        public async Task Dead_Link_Signals_Timeout() {
            var deadReason = new TaskCompletionSource<DisconnectReason>(TaskCreationOptions.RunContinuationsAsynchronously);
            // Blackhole: everything sent by A disappears, so nothing is ever acked.
            var (a, b, _, _) = CreatePair(ReliabilityMode.Reliable, lossRate: 1.0, dupRate: 0,
                (_, _) => { }, onDeadA: reason => deadReason.TrySetResult(reason), maxRetransmits: 3);

            using (a)
            using (b) {
                await a.SendReliableAsync(Channel, new byte[] { 1 });
                Assert.Same(deadReason.Task, await Task.WhenAny(deadReason.Task, Task.Delay(15_000)));
                Assert.Equal(DisconnectReason.Timeout, await deadReason.Task);
            }
        }

        private static async Task WaitUntilAsync(Func<bool> condition, int timeoutMs) {
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
