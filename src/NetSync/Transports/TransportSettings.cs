using NetSync.Diagnostics;

namespace NetSync.Transports {
    /// <summary>
    /// Tuning shared by all built-in transports. <see cref="Peers.NetClient"/> and
    /// <see cref="Peers.NetServer"/> fill it from <see cref="NetConfig"/>.
    /// </summary>
    public sealed class TransportSettings {
        /// <summary>Keepalive ping period.</summary>
        public int PingIntervalMs { get; set; } = 1000;

        /// <summary>Link is considered dead after this long without a pong; 0 disables.</summary>
        public int PingTimeoutMs { get; set; } = 5000;

        /// <summary>
        /// A single socket write stuck for longer than this means the remote stopped
        /// reading (slow consumer); the link is closed with <see cref="DisconnectReason.Timeout"/>.
        /// 0 disables.
        /// </summary>
        public int WriteTimeoutMs { get; set; } = 15000;

        /// <summary>Largest message (after reassembly) accepted from the remote. Bigger = protocol violation.</summary>
        public int MaxMessageSize { get; set; } = NetConfig.DefaultMaxMessageSize;

        public INetLogger Logger { get; set; } = NullNetLogger.Instance;

        internal static TransportSettings From(int pingIntervalMs, int pingTimeoutMs, INetLogger? logger) {
            return new TransportSettings {
                PingIntervalMs = pingIntervalMs,
                PingTimeoutMs = pingTimeoutMs,
                Logger = logger ?? NullNetLogger.Instance
            };
        }
    }
}
