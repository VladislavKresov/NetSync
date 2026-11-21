using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using NetSync.Diagnostics;
using NetSync.Internal;
using NetSync.Pipeline.Reliability;
using NetSync.Transports;

namespace NetSync.Tests {
    /// <summary>A clock that moves only when the test moves it.</summary>
    internal sealed class VirtualClock : ITimeSource {
        public long NowMs { get; set; } = 1_000_000;
    }

    /// <summary>
    /// Two reliable endpoints joined by simulated paths, in virtual time: no threads, no sockets,
    /// no timers. Every run with the same seeds takes exactly the same course, however fast or
    /// busy the machine is — what the reliability layer does is all that is measured.
    /// </summary>
    internal sealed class VirtualNetwork {
        private const int TickMs = 5; // the period of the real ReliabilityClock

        private readonly PriorityQueue<Action, (long At, long Order)> _events = new PriorityQueue<Action, (long, long)>();
        private long _order;

        public VirtualClock Clock { get; } = new VirtualClock();
        public long NowMs => Clock.NowMs;

        public void At(long time, Action action) => _events.Enqueue(action, (Math.Max(time, Clock.NowMs), _order++));

        /// <summary>
        /// Runs events and timer ticks in time order until <paramref name="done"/> holds or the
        /// virtual deadline passes. <paramref name="eachTick"/> runs before every tick (e.g. to offer
        /// more messages). True when done.
        /// </summary>
        public bool RunUntil(Func<bool> done, long forMs, Action? eachTick, params ReliableEndpoint[] endpoints) {
            long deadline = Clock.NowMs + forMs;
            while (!done()) {
                if (Clock.NowMs >= deadline) {
                    return false;
                }
                long nextTick = (Clock.NowMs / TickMs + 1) * TickMs;
                while (_events.TryPeek(out _, out var key) && key.At <= nextTick) {
                    var action = _events.Dequeue();
                    Clock.NowMs = Math.Max(Clock.NowMs, key.At);
                    action();
                }
                Clock.NowMs = nextTick;
                eachTick?.Invoke();
                foreach (var endpoint in endpoints) {
                    endpoint.Tick(nextTick);
                }
            }
            return true;
        }

        /// <summary>Our process stops for a while: no packet is processed and no timer fires, but time passes.</summary>
        public void Stall(long ms) => Clock.NowMs += ms;

        /// <summary>A reliable endpoint on the virtual clock (not on the real timer thread).</summary>
        public ReliableEndpoint Endpoint(Dictionary<byte, ChannelConfig> channels, Func<VirtualPath> path, ReliableEndpoint.SegmentHandler deliver,
                                         Action<DisconnectReason>? onDead = null, bool congestionControl = true, int deadAfterMs = 5000) {
            return new ReliableEndpoint(channels, message => path().Send(message), deliver, () => -1, onDead ?? (_ => { }),
                                        NullNetLogger.Instance, congestionControl: congestionControl, deadAfterMs: deadAfterMs, time: Clock);
        }
    }

    /// <summary>
    /// One direction of a simulated path: propagation delay, jitter, random loss, and optionally a
    /// bandwidth limit with a drop-tail queue (a bottleneck). Deterministic for a given seed.
    /// </summary>
    internal sealed class VirtualPath {
        private readonly VirtualNetwork _network;
        private readonly Random _random;
        private double _lastDepartureMs;

        public ReliableEndpoint? Target;
        public int DelayMs = 10;
        public int JitterMs;
        public double LossRate;
        public double BytesPerMs;   // 0 = unlimited
        public double QueueLimitMs; // drop-tail limit, as queued transmission time
        public bool Down;           // everything is lost
        public int Sent, Dropped;

        public VirtualPath(VirtualNetwork network, int seed) {
            _network = network;
            _random = new Random(seed);
        }

        public ValueTask Send(ReadOnlyMemory<byte> message) {
            Sent++;
            double now = _network.NowMs;
            if (Down || _random.NextDouble() < LossRate) {
                Dropped++;
                return default;
            }
            double departure = now;
            if (BytesPerMs > 0) {
                double backlog = Math.Max(0, _lastDepartureMs - now);
                if (backlog > QueueLimitMs) {
                    Dropped++;
                    return default;
                }
                departure = Math.Max(now, _lastDepartureMs) + message.Length / BytesPerMs;
                _lastDepartureMs = departure;
            }
            var packet = message.ToArray(); // the sender reuses its buffer
            long arrival = (long)Math.Ceiling(departure) + DelayMs + (JitterMs > 0 ? _random.Next(JitterMs + 1) : 0);
            _network.At(arrival, () => Deliver(packet));
            return default;
        }

        private void Deliver(byte[] packet) {
            var target = Target;
            if (target == null) {
                return;
            }
            if (packet[0] == 0x20) {
                target.HandleRelData(packet, 0, packet.Length);
            }
            else if (packet[0] == 0x21) {
                target.HandleAck(packet, 0, packet.Length);
            }
        }
    }
}
