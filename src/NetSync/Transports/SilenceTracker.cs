using System;

namespace NetSync.Transports {
    /// <summary>
    /// Decides when a link has been silent long enough to call it dead.
    ///
    /// Silence is counted in keepalive rounds this process actually ran, not only in elapsed
    /// time: while our own process is paused (a busy machine, a starved thread pool, a laptop
    /// waking up) we send no pings, so the peer can't answer them — the silence is ours, not
    /// the peer's. A link is dead when enough rounds in a row heard nothing
    /// (PingTimeoutMs / PingIntervalMs of them) and the timeout has also passed.
    /// Owned by one keepalive loop; not thread-safe.
    /// </summary>
    internal struct SilenceTracker {
        private long _previousRoundMs;
        private int _silentRounds;

        /// <summary>One keepalive round, after its ping went out. True = the peer is gone.</summary>
        /// <param name="lastHeardMs">When the peer was last heard from (pong or any packet, per caller).</param>
        /// <param name="nowMs">Current time.</param>
        /// <param name="settings">Ping interval and timeout.</param>
        public bool IsDead(long lastHeardMs, long nowMs, TransportSettings settings) {
            if (settings.PingTimeoutMs <= 0) {
                return false;
            }
            _silentRounds = lastHeardMs > _previousRoundMs ? 0 : _silentRounds + 1;
            _previousRoundMs = nowMs;
            int interval = Math.Max(1, settings.PingIntervalMs);
            int roundsNeeded = Math.Max(2, (settings.PingTimeoutMs + interval - 1) / interval);
            return _silentRounds >= roundsNeeded && nowMs - lastHeardMs > settings.PingTimeoutMs;
        }
    }
}
