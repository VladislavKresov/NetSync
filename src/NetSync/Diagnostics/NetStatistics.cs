using System.Threading;

namespace NetSync.Diagnostics {
    /// <summary>
    /// Point-in-time snapshot of a connection's traffic, as seen by the application:
    /// message counts and payload bytes (before compression/encryption), plus the
    /// reliability engine's view of the link.
    /// </summary>
    public sealed class NetStatistics {
        public long MessagesSent { get; internal set; }
        public long MessagesReceived { get; internal set; }
        /// <summary>Application payload bytes handed to SendAsync.</summary>
        public long BytesSent { get; internal set; }
        /// <summary>Application payload bytes delivered to DataReceived.</summary>
        public long BytesReceived { get; internal set; }
        /// <summary>Reliable-UDP packets resent after a timeout.</summary>
        public long Retransmissions { get; internal set; }
        /// <summary>Reliable-UDP packets resent early because later ones were acked.</summary>
        public long FastRetransmissions { get; internal set; }
        /// <summary>Keepalive round trip, lowest across links; -1 when unknown.</summary>
        public int PingMs { get; internal set; } = -1;
        /// <summary>Smoothed RTT measured from reliable-UDP acks; -1 when unknown.</summary>
        public double SmoothedRttMs { get; internal set; } = -1;
        /// <summary>Data events dropped because PollEvents() wasn't called (Polled mode).</summary>
        public long DroppedEvents { get; internal set; }
        public bool IsDegraded { get; internal set; }

        public override string ToString() {
            return $"sent {MessagesSent} msg / {BytesSent} B, received {MessagesReceived} msg / {BytesReceived} B, " +
                   $"ping {PingMs} ms, srtt {SmoothedRttMs:F1} ms, retransmits {Retransmissions}+{FastRetransmissions} fast" +
                   (DroppedEvents > 0 ? $", dropped events {DroppedEvents}" : "") +
                   (IsDegraded ? ", DEGRADED (UDP over TCP)" : "");
        }
    }

    /// <summary>Lock-free counters behind <see cref="NetStatistics"/>.</summary>
    internal sealed class TrafficCounters {
        private long _messagesSent;
        private long _messagesReceived;
        private long _bytesSent;
        private long _bytesReceived;

        public void OnSent(int bytes) {
            Interlocked.Increment(ref _messagesSent);
            Interlocked.Add(ref _bytesSent, bytes);
        }

        public void OnReceived(int bytes) {
            Interlocked.Increment(ref _messagesReceived);
            Interlocked.Add(ref _bytesReceived, bytes);
        }

        public void CopyTo(NetStatistics statistics) {
            statistics.MessagesSent = Interlocked.Read(ref _messagesSent);
            statistics.MessagesReceived = Interlocked.Read(ref _messagesReceived);
            statistics.BytesSent = Interlocked.Read(ref _bytesSent);
            statistics.BytesReceived = Interlocked.Read(ref _bytesReceived);
        }
    }
}
