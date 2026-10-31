using System;

namespace NetSync.Peers {
    /// <summary>
    /// Estimates the offset between this client's clock and the server's, NTP-style:
    /// each round trip gives offset = serverTime + rtt/2 − localReceiveTime, which is
    /// exact when both legs took equally long. The sample with the smallest RTT has the
    /// least room for asymmetry, so the estimate is the min-RTT sample of a small window —
    /// a congested round trip can't drag it off.
    /// </summary>
    internal sealed class ClockSync {
        private const int Window = 8;

        private readonly double[] _rtt = new double[Window];
        private readonly double[] _offset = new double[Window];
        private readonly object _lock = new object();
        private int _count;
        private int _next;
        private double _bestOffset = double.NaN;
        private double _bestRtt = double.NaN;

        public void AddSample(double clientSendMs, double serverTimeMs, double clientReceiveMs) {
            double rtt = clientReceiveMs - clientSendMs;
            if (rtt < 0 || double.IsNaN(serverTimeMs) || double.IsInfinity(serverTimeMs)) {
                return; // not a reply to one of our requests (clock values are monotonic)
            }
            double offset = serverTimeMs + rtt / 2 - clientReceiveMs;
            lock (_lock) {
                _rtt[_next] = rtt;
                _offset[_next] = offset;
                _next = (_next + 1) % Window;
                _count = Math.Min(_count + 1, Window);

                int best = 0;
                for (int i = 1; i < _count; i++) {
                    if (_rtt[i] < _rtt[best]) {
                        best = i;
                    }
                }
                _bestRtt = _rtt[best];
                _bestOffset = _offset[best];
            }
        }

        /// <summary>Server clock now, by our estimate; NaN until the first sample.</summary>
        public double ServerTimeMs(double localNowMs) {
            lock (_lock) {
                return localNowMs + _bestOffset;
            }
        }

        /// <summary>Worst-case error of the estimate (half the best RTT); NaN until the first sample.</summary>
        public double AccuracyMs {
            get {
                lock (_lock) {
                    return _bestRtt / 2;
                }
            }
        }
    }
}
