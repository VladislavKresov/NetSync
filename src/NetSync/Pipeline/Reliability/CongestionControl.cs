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
    /// Loss-based (Normal/High): TCP Veno — slow start to ssthresh, then +1 packet per RTT.
    /// On a loss event (at most one reduction per RTT) it estimates how many of its packets
    /// sit in a queue: (window / base RTT − window / smoothed RTT) × base RTT. Under
    /// <see cref="RandomLossBacklog"/> packets there is no queue to blame, so the loss was
    /// random (Wi-Fi, a VPN tunnel) and the window shrinks only to 4/5; with a queue it is
    /// congestion and the window halves, as in Reno. Without this a tunnel with 1 % loss
    /// carried a few hundred KB/s however fast it was.
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

        /// <summary>Veno's β: fewer of our packets than this in a queue means a loss was not congestion.</summary>
        public const double RandomLossBacklog = 3;

        private readonly bool _background;
        private readonly double _maxWindow;
        private double _ssthresh;
        private long _lastReductionMs = long.MinValue / 2;
        private double _windowBeforeRandomLoss; // > 0: recovering from a random loss toward this window
        private double _undoWindow, _undoSsthresh;
        private long _undoUntilMs = long.MinValue;

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
                if (Window < _ssthresh) {
                    Window += 1; // slow start
                }
                else if (_windowBeforeRandomLoss > Window) {
                    // After a random loss nothing says the path got slower: climb back to the old
                    // window in halving steps (BIC's binary search), half the gap per round trip.
                    Window += Math.Max(1, (_windowBeforeRandomLoss - Window) / 2) / Window;
                }
                else {
                    _windowBeforeRandomLoss = 0;
                    Window += 1 / Window; // congestion avoidance: +1 per round trip
                }
            }
            Window = Math.Min(Window, _maxWindow);
        }

        /// <summary>
        /// The last reduction was for a loss that wasn't one: the "lost" packet's original arrived
        /// after all (it was only reordered). Restore the window it cost.
        /// </summary>
        public void UndoLastReduction(long nowMs) {
            if (nowMs > _undoUntilMs || _undoWindow <= Window) {
                return;
            }
            Window = _undoWindow;
            _ssthresh = Math.Max(_ssthresh, _undoSsthresh);
            _undoUntilMs = long.MinValue;
        }

        /// <summary>Packets of ours in the network queue, from how much the round trip exceeds its base (Veno's N).</summary>
        private double Backlog(RttEstimator rtt) {
            if (!rtt.HasSample || rtt.SmoothedRtt <= 0 || double.IsInfinity(rtt.BaseRtt) || rtt.BaseRtt >= double.MaxValue) {
                return double.MaxValue; // no measurements yet: assume congestion
            }
            return Window * Math.Max(0, rtt.SmoothedRtt - rtt.BaseRtt) / rtt.SmoothedRtt;
        }

        public void OnLoss(RttEstimator rtt, long nowMs) {
            double rttMs = rtt.HasSample ? rtt.SmoothedRtt : 100;
            if (nowMs - _lastReductionMs <= rttMs) {
                return; // same loss event — one reduction per round trip
            }
            // The scavenger (Low) always yields; the others tell random loss from congestion.
            bool random = !_background && Backlog(rtt) < RandomLossBacklog;
            _undoWindow = Window;
            _undoSsthresh = _ssthresh;
            _undoUntilMs = nowMs + (long)(4 * rttMs); // a spurious resend shows within a few round trips
            _windowBeforeRandomLoss = random ? Math.Max(Window, _windowBeforeRandomLoss) : 0;
            _ssthresh = Math.Max(MinWindow, Window * (random ? 0.8 : 0.5));
            Window = _ssthresh;
            _lastReductionMs = nowMs;
        }
    }
}
