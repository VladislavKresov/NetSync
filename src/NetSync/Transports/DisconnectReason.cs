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
        Error
    }
}
