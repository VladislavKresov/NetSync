using System;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

namespace NetSync.Transports {
    /// <summary>
    /// Client-side transport: a single link to a remote server.
    ///
    /// Threading contract: all events are raised on background network threads.
    /// Never block inside a handler; hand data off to your own queue if you need
    /// main-thread delivery (the Unity adapter does exactly that).
    ///
    /// Lifetime contract: a transport instance is reusable — after Disconnect()
    /// completes it may be connected again with ConnectAsync().
    /// </summary>
    public interface ITransport : IDisposable {
        bool IsConnected { get; }

        /// <summary>Last measured round-trip time in milliseconds, -1 when unknown.</summary>
        int PingMs { get; }

        event Action? Connected;
        event Action<DisconnectReason>? Disconnected;
        event Action<byte[]>? DataReceived;

        Task ConnectAsync(IPEndPoint endpoint, CancellationToken ct = default);

        /// <summary>
        /// Queues data for sending. Returns once the payload is accepted by the send
        /// queue (not once it hits the wire). Throws if the transport is not connected
        /// or the queue is full (backpressure).
        /// </summary>
        ValueTask SendAsync(ReadOnlyMemory<byte> data, CancellationToken ct = default);

        /// <summary>
        /// Scatter-gather variant: sends prefix immediately followed by payload as one
        /// packet, without requiring the caller to concatenate them. Lets upper layers
        /// prepend their headers with zero intermediate allocations.
        /// </summary>
        ValueTask SendAsync(ReadOnlyMemory<byte> prefix, ReadOnlyMemory<byte> payload, CancellationToken ct = default);

        void Disconnect();
    }
}
