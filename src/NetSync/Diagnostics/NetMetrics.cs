using System.Threading;

namespace NetSync.Diagnostics {
    /// <summary>
    /// Per-connection counters. All writes go through <see cref="Interlocked"/> so the
    /// metrics can be read from any thread while network threads update them.
    /// </summary>
    public sealed class NetMetrics {
        private long _bytesSent;
        private long _bytesReceived;
        private long _packetsSent;
        private long _packetsReceived;
        private long _fragmentsSent;
        private long _fragmentsReceived;
        private long _packetsReassembled;
        private long _pingTimeouts;
        private int _sendQueueSize;
        private int _maxSendQueueSize;

        public long BytesSent => Interlocked.Read(ref _bytesSent);
        public long BytesReceived => Interlocked.Read(ref _bytesReceived);
        public long PacketsSent => Interlocked.Read(ref _packetsSent);
        public long PacketsReceived => Interlocked.Read(ref _packetsReceived);
        public long FragmentsSent => Interlocked.Read(ref _fragmentsSent);
        public long FragmentsReceived => Interlocked.Read(ref _fragmentsReceived);
        public long PacketsReassembled => Interlocked.Read(ref _packetsReassembled);
        public long PingTimeouts => Interlocked.Read(ref _pingTimeouts);
        public int SendQueueSize => Volatile.Read(ref _sendQueueSize);
        public int MaxSendQueueSize => Volatile.Read(ref _maxSendQueueSize);

        internal void AddBytesSent(long count) => Interlocked.Add(ref _bytesSent, count);
        internal void AddBytesReceived(long count) => Interlocked.Add(ref _bytesReceived, count);
        internal void IncrementPacketsSent() => Interlocked.Increment(ref _packetsSent);
        internal void IncrementPacketsReceived() => Interlocked.Increment(ref _packetsReceived);
        internal void IncrementFragmentsSent() => Interlocked.Increment(ref _fragmentsSent);
        internal void IncrementFragmentsReceived() => Interlocked.Increment(ref _fragmentsReceived);
        internal void IncrementPacketsReassembled() => Interlocked.Increment(ref _packetsReassembled);
        internal void IncrementPingTimeouts() => Interlocked.Increment(ref _pingTimeouts);

        internal void UpdateSendQueueSize(int size) {
            Volatile.Write(ref _sendQueueSize, size);
            int max;
            while (size > (max = Volatile.Read(ref _maxSendQueueSize))) {
                if (Interlocked.CompareExchange(ref _maxSendQueueSize, size, max) == max) {
                    break;
                }
            }
        }

        public override string ToString() {
            return $"NetMetrics(sent={PacketsSent}p/{BytesSent}B, recv={PacketsReceived}p/{BytesReceived}B, " +
                   $"fragments={FragmentsSent}/{FragmentsReceived}, reassembled={PacketsReassembled}, " +
                   $"queue={SendQueueSize}/{MaxSendQueueSize}, pingTimeouts={PingTimeouts})";
        }
    }
}
