using System.Diagnostics;

namespace NetSync.Internal {
    /// <summary>
    /// Monotonic clock. DateTime.UtcNow is not monotonic (NTP jumps, DST issues on some
    /// platforms) and must not be used for RTT or timeout measurement.
    /// </summary>
    internal static class NetTime {
        private static readonly double TicksToMs = 1000.0 / Stopwatch.Frequency;

        /// <summary>Milliseconds since an arbitrary fixed origin. Monotonic, thread-safe.</summary>
        public static long NowMs => (long)(Stopwatch.GetTimestamp() * TicksToMs);
    }
}
