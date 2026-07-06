using System;
using System.Threading;
using System.Threading.Tasks;

namespace NetSync.Transports {
    /// <summary>
    /// Server-side transport: listens on a port and manages many peers.
    /// Events are raised on background network threads (see <see cref="ITransport"/> notes).
    /// </summary>
    public interface IServerTransport : IDisposable {
        /// <summary>Actual bound port (useful when started with port 0), 0 when stopped.</summary>
        int Port { get; }

        bool IsRunning { get; }

        event Action<TransportPeer>? PeerConnected;
        event Action<TransportPeer, DisconnectReason>? PeerDisconnected;
        event Action<TransportPeer, byte[]>? DataReceived;

        /// <summary>Starts listening. Pass port 0 to let the OS pick one. Returns the bound port.</summary>
        Task<int> StartAsync(int port, CancellationToken ct = default);

        Task StopAsync();

        ValueTask SendAsync(TransportPeer peer, ReadOnlyMemory<byte> data, CancellationToken ct = default);

        /// <summary>Scatter-gather variant, see <see cref="ITransport"/> notes.</summary>
        ValueTask SendAsync(TransportPeer peer, ReadOnlyMemory<byte> prefix, ReadOnlyMemory<byte> payload, CancellationToken ct = default);

        void DisconnectPeer(TransportPeer peer);
    }
}
