using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using NetSync.Pipeline.Reliability;

namespace NetSync.Tests {
    /// <summary>
    /// One direction of a simulated network path: a bottleneck link with limited
    /// bandwidth, a drop-tail queue and a fixed propagation delay — the conditions under
    /// which congestion control matters (loopback has neither limit nor queue).
    ///
    /// Virtual-time model: each packet's departure is computed from timestamps
    /// (serialization at <see cref="_bytesPerSecond"/> behind the packets already
    /// queued), so the 15 ms Windows timer resolution doesn't distort it. A dedicated
    /// delivery thread hands packets to the target endpoint when due, one at a time,
    /// just like a socket receive loop.
    /// </summary>
    internal sealed class BottleneckPipe : IDisposable {
        private readonly struct InFlight {
            public readonly byte[] Packet;
            public readonly double DeliverAtMs;
            public InFlight(byte[] packet, double deliverAtMs) {
                Packet = packet;
                DeliverAtMs = deliverAtMs;
            }
        }

        private readonly double _bytesPerSecond;
        private readonly double _maxQueueMs;
        private readonly double _delayMs;
        private readonly Queue<InFlight> _queue = new Queue<InFlight>();
        private readonly object _lock = new object();
        private readonly Stopwatch _clock = Stopwatch.StartNew();
        private readonly Thread _thread;
        private double _lastDepartureMs;
        private volatile bool _stopped;

        public ReliableEndpoint? Target;
        public int Dropped;
        public int Delivered;

        /// <summary>Queueing delay seen by every packet that carried the given channel id (for latency probes).</summary>
        public readonly List<double> ProbeQueueingDelaysMs = new List<double>();
        public byte ProbeChannel = 255;

        /// <param name="bytesPerSecond">Bottleneck bandwidth; <= 0 means unlimited.</param>
        /// <param name="maxQueueMs">Drop-tail limit, expressed as queued transmission time.</param>
        /// <param name="delayMs">One-way propagation delay.</param>
        public BottleneckPipe(double bytesPerSecond, double maxQueueMs, double delayMs) {
            _bytesPerSecond = bytesPerSecond;
            _maxQueueMs = maxQueueMs;
            _delayMs = delayMs;
            _thread = new Thread(DeliveryLoop) { IsBackground = true, Name = "BottleneckPipe" };
            _thread.Start();
        }

        private double NowMs => _clock.Elapsed.TotalMilliseconds;

        public System.Threading.Tasks.ValueTask Send(ReadOnlyMemory<byte> message) {
            var packet = message.ToArray();
            double now = NowMs;
            lock (_lock) {
                double backlogMs = Math.Max(0, _lastDepartureMs - now);
                if (_bytesPerSecond > 0 && backlogMs > _maxQueueMs) {
                    Dropped++;
                    return default;
                }
                if (packet.Length > 2 && packet[0] == 0x20 && packet[2] == ProbeChannel) {
                    ProbeQueueingDelaysMs.Add(backlogMs);
                }
                double serializationMs = _bytesPerSecond > 0 ? packet.Length * 1000.0 / _bytesPerSecond : 0;
                double departure = Math.Max(now, _lastDepartureMs) + serializationMs;
                _lastDepartureMs = departure;
                _queue.Enqueue(new InFlight(packet, departure + _delayMs));
            }
            return default;
        }

        private void DeliveryLoop() {
            while (!_stopped) {
                InFlight? due = null;
                lock (_lock) {
                    if (_queue.Count > 0 && _queue.Peek().DeliverAtMs <= NowMs) {
                        due = _queue.Dequeue();
                    }
                }
                if (due is not { } item) {
                    Thread.Sleep(0); // precise-ish pacing without a 15 ms timer tick
                    continue;
                }
                var target = Target;
                if (target == null) {
                    continue;
                }
                Delivered++;
                if (item.Packet[0] == 0x20) {
                    target.HandleRelData(item.Packet, 0, item.Packet.Length);
                }
                else if (item.Packet[0] == 0x21) {
                    target.HandleAck(item.Packet, 0, item.Packet.Length);
                }
            }
        }

        public void Dispose() {
            _stopped = true;
            _thread.Join(1000);
        }
    }
}
