using System.Net;
using System.Threading;
using NetSync.Pipeline.Fragmentation;

namespace NetSync.Transports {
    /// <summary>
    /// A remote peer as seen by a server transport. Replaces the old TransportClient:
    /// no untyped 'object Client' — transport-specific state stays inside the transport.
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

        internal SemaphoreSlim WriteLock { get; } = new SemaphoreSlim(1, 1);
        internal FragmentBuffer FragmentBuffer { get; } = new FragmentBuffer();

        /// <summary>Transport-specific state (e.g. the TcpClient). Internal on purpose.</summary>
        internal object? TransportState { get; set; }

        /// <summary>Set by the peer layer once the handshake binds this link to a NetConnection.</summary>
        internal object? PeerLayerState { get; set; }

        internal TransportPeer(IPEndPoint endPoint, long nowMs) {
            EndPoint = endPoint;
            _lastSeenMs = nowMs;
        }

        internal void SetPing(int pingMs) => Volatile.Write(ref _pingMs, pingMs);
        internal void Touch(long nowMs) => Volatile.Write(ref _lastSeenMs, nowMs);
        internal long LastSeenMs => Volatile.Read(ref _lastSeenMs);

        public override string ToString() => $"Peer#{Id} {EndPoint} ({PingMs}ms)";
    }
}
