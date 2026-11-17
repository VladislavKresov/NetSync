namespace NetSync.Transports {
    /// <summary>
    /// Why a connection ended. Replaces the old OnDisconnected/OnServerDisconnected event
    /// pair: subscribers get a single event with an explicit reason.
    /// </summary>
    public enum DisconnectReason {
        /// <summary>Local side called Disconnect() or disposed the transport.</summary>
        Local,
        /// <summary>Remote side closed the connection.</summary>
        Remote,
        /// <summary>No pong received within the configured timeout.</summary>
        Timeout,
        /// <summary>An unrecoverable transport error occurred.</summary>
        Error,
        /// <summary>The server deliberately disconnected this client (NetConnection.DisconnectAsync). No auto-reconnect.</summary>
        Kicked,
        /// <summary>
        /// Polled delivery only: the event queue was full (<c>NetConfig.MaxPendingEvents</c>) when a
        /// message of a reliable channel arrived. Dropping it would break the channel's
        /// guarantee, so the connection is closed instead. Call PollEvents() more often.
        /// </summary>
        Overloaded
    }
}
