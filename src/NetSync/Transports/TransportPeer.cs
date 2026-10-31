using System.Net;
using System.Threading;

namespace NetSync.Transports {
    /// <summary>
    /// A remote peer as seen by a server transport. Transport-specific state stays
    /// inside the transport (<see cref="TransportState"/> is internal on purpose).
    /// </summary>
    public sealed class TransportPeer {
        private static long _nextId;

        /// <summary>Process-unique peer id.</summary>
        public long Id { get; } = Interlocked.Increment(ref _nextId);

        public IPEndPoint EndPoint { get; }

        /// <summary>Last measured round-trip time in milliseconds, -1 when unknown.</summary>
        public int PingMs => Volatile.Read(ref _pingMs);

        private int _pingMs = -1;
        private long _lastSeenMs;
        private int _trusted;

        /// <summary>Transport-specific state (e.g. the TCP socket and its send queue).</summary>
        internal object? TransportState { get; set; }

        /// <summary>Transport-specific send state (e.g. the UDP batch queue).</summary>
        internal object? SendState { get; set; }

        /// <summary>Set by the peer layer once the handshake binds this link to a NetConnection.</summary>
        internal object? PeerLayerState { get; set; }

        /// <summary>The server transport this peer arrived on (a server may run several per slot).</summary>
        internal IServerTransport? Owner { get; set; }

        /// <summary>
        /// Set by the peer layer once the handshake succeeded. Until then a transport in
        /// trust-required mode treats the peer as a potential attacker: tiny frame limit,
        /// no keepalive traffic, counted against the untrusted-peer cap.
        /// </summary>
        internal bool IsTrusted => Volatile.Read(ref _trusted) == 1;
        internal void MarkTrusted() => Volatile.Write(ref _trusted, 1);

        internal TransportPeer(IPEndPoint endPoint, long nowMs) {
            EndPoint = endPoint;
            _lastSeenMs = nowMs;
        }

        /// <summary>For custom <see cref="IServerTransport"/> implementations: one per remote link.</summary>
        public TransportPeer(IPEndPoint endPoint) : this(endPoint, Internal.NetTime.NowMs) { }

        /// <summary>For custom transports: publish a measured round-trip time.</summary>
        public void ReportPing(int pingMs) => SetPing(pingMs);

        internal void SetPing(int pingMs) => Volatile.Write(ref _pingMs, pingMs);
        internal void Touch(long nowMs) => Volatile.Write(ref _lastSeenMs, nowMs);
        internal long LastSeenMs => Volatile.Read(ref _lastSeenMs);

        public override string ToString() => $"Peer#{Id} {EndPoint} ({PingMs}ms)";
    }
}
