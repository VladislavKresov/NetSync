using System;

namespace NetSync.Pipeline.Reliability {
    /// <summary>
    /// Link round-trip estimator from ACK samples (RFC 6298: SRTT/RTTVAR, Karn's rule
    /// is applied by the caller — only first transmissions are sampled). Also tracks a
    /// windowed minimum RTT, the "empty queue" baseline for delay-based control.
    /// Not thread-safe; owned by the endpoint under its RTT lock.
    /// </summary>
    internal sealed class RttEstimator {
        private const long BaseWindowMs = 30_000;

        private double _srtt;
        private double _rttVar;
        private bool _hasSample;
        private double _minCurrent = double.MaxValue;
        private double _minPrevious = double.MaxValue;
        private long _bucketStartMs;
        private int _backoff = 1;
        private long _nextBackoffAtMs;

        /// <summary>Largest factor a run of timeouts multiplies the retransmission timeout by.</summary>
        public const int MaxBackoff = 64;

        public bool HasSample => _hasSample;

        /// <summary>The path changed (UDP to TCP): timeouts measured on the old one say nothing.</summary>
        public void ResetBackoff() {
            _backoff = 1;
            _nextBackoffAtMs = 0;
        }

        /// <summary>
        /// A retransmission timer fired (RFC 6298, 5.5): the timeout stays doubled for every
        /// packet — not just the one that timed out — until an acknowledgement brings a new
        /// valid sample. Without this, a link whose acknowledgements are merely slow (a busy
        /// receiver, a congested queue) keeps timing out at the old short timeout, and every
        /// spurious retransmission adds to the load that delays the acknowledgements: a storm.
        /// Retransmitted packets give no samples (Karn), so only this backoff lets the timeout grow.
        /// Doubles at most once per timeout period, like TCP's single timer: packets sent a little
        /// apart expire on consecutive scans, and that is one timeout, not several.
        /// </summary>
        /// <param name="nowMs">Current time.</param>
        /// <param name="timeoutMs">The (backed-off) retransmission timeout that just expired.</param>
        public void OnTimeout(long nowMs, int timeoutMs) {
            if (nowMs < _nextBackoffAtMs) {
                return;
            }
            _backoff = Math.Min(_backoff * 2, MaxBackoff);
            _nextBackoffAtMs = nowMs + 2L * timeoutMs;
        }
        public double SmoothedRtt => _srtt;

        /// <summary>Minimum RTT over the last 30–60 s (route changes age out).</summary>
        public double BaseRtt => Math.Min(_minCurrent, _minPrevious);

        /// <summary>How much the network queue currently adds to the round trip.</summary>
        public double QueueingDelay => _hasSample ? Math.Max(0, _srtt - BaseRtt) : 0;

        public void AddSample(double rttMs, long nowMs) {
            _backoff = 1; // a valid sample: the measured timeout is trustworthy again
            _nextBackoffAtMs = 0;
            rttMs = Math.Max(rttMs, 0.1);
            if (!_hasSample) {
                _srtt = rttMs;
                _rttVar = rttMs / 2;
                _hasSample = true;
                _bucketStartMs = nowMs;
            }
            else {
                _rttVar = 0.75 * _rttVar + 0.25 * Math.Abs(_srtt - rttMs);
                _srtt = 0.875 * _srtt + 0.125 * rttMs;
            }

            if (nowMs - _bucketStartMs > BaseWindowMs) {
                _minPrevious = _minCurrent;
                _minCurrent = double.MaxValue;
                _bucketStartMs = nowMs;
            }
            _minCurrent = Math.Min(_minCurrent, rttMs);
        }

        public int RetransmitTimeoutMs(int floorMs, int ceilingMs, int fallbackMs) {
            double measured = _hasSample ? _srtt + Math.Max(4 * _rttVar, 10) : fallbackMs;
            return (int)Math.Clamp(Math.Max(measured, floorMs) * _backoff, floorMs, ceilingMs);
        }
    }

    /// <summary>
    /// Per-channel congestion window, in packets in flight.
    ///
    /// Loss-based (Normal/High): Reno-style — slow start to ssthresh, then +1 packet per
    /// RTT; a loss event halves the window at most once per RTT.
    ///
    /// Delay-based (Low): LEDBAT-style scavenger — grows only while the queueing delay
    /// it observes stays under <see cref="TargetQueueingDelayMs"/> and shrinks as soon as
    /// it exceeds it, so bulk transfers soak up idle capacity but yield to real-time
    /// channels the moment a queue starts forming. Also reacts to loss like Reno.
    /// Not thread-safe; mutated under the owning channel's lock.
    /// </summary>
    internal sealed class CongestionWindow {
        public const double InitialWindow = 16;
        public const double MinWindow = 2;
        public const double TargetQueueingDelayMs = 25;

        private readonly bool _background;
        private readonly double _maxWindow;
        private double _ssthresh;
        private long _lastReductionMs = long.MinValue / 2;

        public double Window { get; private set; } = InitialWindow;

        public CongestionWindow(bool background, double maxWindow) {
            _background = background;
            _maxWindow = maxWindow;
            _ssthresh = maxWindow;
        }

        public int AllowedInFlight => Math.Max((int)MinWindow, (int)Window);

        public void OnAcked(int newlyAcked, RttEstimator rtt, long nowMs) {
            if (_background && rtt.HasSample) {
                double queueing = rtt.QueueingDelay;
                if (queueing > TargetQueueingDelayMs) {
                    // We are building a queue: back off (once per RTT) and don't grow.
                    if (nowMs - _lastReductionMs > rtt.SmoothedRtt) {
                        Window = Math.Max(MinWindow, Window * 0.8);
                        _ssthresh = Math.Max(MinWindow, Window);
                        _lastReductionMs = nowMs;
                    }
                    return;
                }
            }

            for (int i = 0; i < newlyAcked; i++) {
                Window += Window < _ssthresh ? 1 : 1 / Window;
            }
            Window = Math.Min(Window, _maxWindow);
        }

        public void OnLoss(RttEstimator rtt, long nowMs) {
            double rttMs = rtt.HasSample ? rtt.SmoothedRtt : 100;
            if (nowMs - _lastReductionMs <= rttMs) {
                return; // same loss event — one reduction per round trip
            }
            _ssthresh = Math.Max(MinWindow, Window / 2);
            Window = _ssthresh;
            _lastReductionMs = nowMs;
        }
    }
}
