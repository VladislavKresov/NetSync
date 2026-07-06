namespace NetSync {
    /// <summary>
    /// Delivery guarantees for a channel. Only meaningful on UDP: TCP channels are
    /// inherently reliable-ordered and ignore this setting.
    /// </summary>
    public enum ReliabilityMode : byte {
        /// <summary>Fire-and-forget datagrams. Lowest latency, no guarantees.</summary>
        Unreliable = 0,

        /// <summary>
        /// No retransmission, but stale packets are dropped: a packet older than the
        /// newest already delivered is discarded. Ideal for state snapshots (positions)
        /// where only the latest value matters.
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
        ReliableOrdered = 3
    }
}
