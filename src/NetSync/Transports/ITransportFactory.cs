using NetSync.Transports.Tcp;
using NetSync.Transports.Udp;

namespace NetSync.Transports {
    /// <summary>
    /// Creates the implementation behind a transport slot. Register a factory in
    /// <see cref="NetConfig.TransportFactories"/> to run NetSync over your own transport
    /// (WebSocket, a platform socket API, an in-memory test network, …) — channels,
    /// handshake, reliability, compression and encryption all work on top unchanged.
    ///
    /// Slot contracts:
    /// - <see cref="TransportType.Tcp"/>: messages arrive reliably, in order, as sent.
    /// - <see cref="TransportType.Udp"/>: messages are datagrams — may be lost,
    ///   duplicated or reordered; NetSync's reliability layer runs on top.
    /// Every implementation must preserve message boundaries (one SendAsync = one
    /// DataReceived on the other side) and raise events on a single thread per link.
    /// </summary>
    public interface ITransportFactory {
        ITransport CreateClient(TransportSettings settings);
        IServerTransport CreateServer(TransportSettings settings);
    }

    /// <summary>The built-in TCP implementation (default for the Tcp slot).</summary>
    public sealed class TcpTransportFactory : ITransportFactory {
        public static readonly TcpTransportFactory Instance = new TcpTransportFactory();
        public ITransport CreateClient(TransportSettings settings) => new TcpTransport(settings);
        public IServerTransport CreateServer(TransportSettings settings) => new TcpServerTransport(settings);
    }

    /// <summary>The built-in UDP implementation (default for the Udp slot).</summary>
    public sealed class UdpTransportFactory : ITransportFactory {
        public static readonly UdpTransportFactory Instance = new UdpTransportFactory();
        public ITransport CreateClient(TransportSettings settings) => new UdpTransport(settings);
        public IServerTransport CreateServer(TransportSettings settings) => new UdpServerTransport(settings);
    }
}
