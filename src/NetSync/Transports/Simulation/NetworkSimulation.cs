using System;

namespace NetSync.Transports.Simulation {
    /// <summary>
    /// A bad network, on demand — for development and testing only. Set
    /// <see cref="NetConfig.Simulation"/> on one peer (typically the client) to see how the
    /// application behaves with lag, jitter and loss, without touching the real network.
    ///
    /// Applies to every packet this peer sends and receives: latency and jitter are added
    /// in each direction (the round trip grows by 2 × <see cref="LatencyMs"/>); loss and
    /// duplication hit datagram (UDP-slot) links only — stream links stay reliable and
    /// ordered, as TCP is.
    /// </summary>
    public sealed class NetworkSimulation {
        private double _lossRate;
        private double _duplicateRate;

        /// <summary>Added one-way delay, per direction.</summary>
        public int LatencyMs { get; set; }

        /// <summary>Random extra delay, 0..JitterMs, per packet. On datagram links it reorders packets, as the internet does.</summary>
        public int JitterMs { get; set; }

        /// <summary>Probability (0..1) that a datagram is lost, per direction.</summary>
        public double LossRate {
            get => _lossRate;
            set => _lossRate = Clamp(value, nameof(LossRate));
        }

        /// <summary>Probability (0..1) that a datagram arrives twice.</summary>
        public double DuplicateRate {
            get => _duplicateRate;
            set => _duplicateRate = Clamp(value, nameof(DuplicateRate));
        }

        /// <summary>Seed for reproducible runs; null = random.</summary>
        public int? Seed { get; set; }

        private static double Clamp(double value, string name) {
            if (double.IsNaN(value) || value < 0 || value > 1) {
                throw new ArgumentOutOfRangeException(name, "A probability between 0 and 1");
            }
            return value;
        }
    }
}
