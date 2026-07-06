namespace NetSync.Sending {
    public enum SendPriority : byte {
        /// <summary>Keepalive/control traffic. Always jumps the queue.</summary>
        Critical = 0,
        High = 1,
        Normal = 2,
        /// <summary>Bulk transfers (file fragments) — never starve real-time traffic.</summary>
        Low = 3
    }
}
