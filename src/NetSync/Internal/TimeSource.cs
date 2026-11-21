namespace NetSync.Internal {
    /// <summary>
    /// Where time-driven code reads the time. Production uses <see cref="SystemTimeSource"/>; tests
    /// pass a virtual clock and drive the timers themselves, so timeouts, retransmissions and
    /// congestion control can be checked without threads, sockets or a dependency on machine speed.
    /// </summary>
    internal interface ITimeSource {
        /// <summary>Milliseconds since an arbitrary fixed origin; never goes backwards.</summary>
        long NowMs { get; }
    }

    /// <summary>The monotonic system clock (<see cref="NetTime"/>).</summary>
    internal sealed class SystemTimeSource : ITimeSource {
        public static readonly SystemTimeSource Instance = new SystemTimeSource();

        private SystemTimeSource() { }

        public long NowMs => NetTime.NowMs;
    }
}
