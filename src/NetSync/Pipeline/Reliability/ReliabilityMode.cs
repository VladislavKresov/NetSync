namespace NetSync {
    /// <summary>
    /// Delivery guarantees for a channel. Only meaningful on UDP: TCP channels are
    /// inherently reliable-ordered and ignore this setting.
    /// </summary>
    public enum ReliabilityMode : byte {
        /// <summary>
        /// Fire-and-forget datagrams. Lowest latency, no guarantees. Keep payloads under
        /// ~1100 bytes: bigger ones get IP-fragmented, and losing any piece loses all
        /// of it — use <see cref="UnreliableFragmented"/> for large unreliable data.
        /// </summary>
        Unreliable = 0,

        /// <summary>
        /// No retransmission, but stale packets are dropped: a packet older than the
        /// newest already delivered is discarded. Ideal for state snapshots (positions)
        /// where only the latest value matters. Payload ≤ 1100 bytes.
        /// </summary>
        UnreliableSequenced = 1,

        /// <summary>
        /// Every packet arrives exactly once (ACK + retransmission), but order is not
        /// guaranteed. Large payloads are fragmented automatically.
        /// </summary>
        Reliable = 2,

        /// <summary>
        /// Every packet arrives exactly once and in send order. Large payloads are
        /// fragmented automatically. TCP semantics over UDP.
        /// </summary>
        ReliableOrdered = 3,

        /// <summary>
        /// For large real-time data (video frames, audio bursts): messages of any size
        /// are split into datagram-sized fragments with no retransmission. A message is
        /// delivered only if all its fragments arrive; a message completing after a newer
        /// one was already delivered is dropped as stale.
        /// </summary>
        UnreliableFragmented = 4
    }

    /// <summary>How a channel shares the link with other channels.</summary>
    public enum ChannelPriority : byte {
        /// <summary>Latency-critical traffic (inputs, voice). Jumps ahead of everything but keepalive.</summary>
        High = 1,
        /// <summary>Default.</summary>
        Normal = 2,
        /// <summary>
        /// Background bulk (file transfers, asset downloads). Queued behind other traffic
        /// on TCP; on reliable UDP it uses delay-based "scavenger" congestion control that
        /// backs off as soon as it starts adding queueing delay, so it fills spare
        /// bandwidth without hurting real-time channels on the same link.
        /// </summary>
        Low = 3
    }
}
