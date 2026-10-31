using System;
using System.Net;
using System.Net.Sockets;
using NetSync.Transports.Udp;

namespace NetSync.Relay {
    /// <summary>
    /// The UDP socket of a relay transport, as an interface: hole punching depends on how
    /// the network maps and filters datagrams, and tests substitute NAT behaviours here.
    /// </summary>
    internal interface IDatagramSocket : IDisposable {
        void SendTo(byte[] buffer, int offset, int count, IPEndPoint remote);

        /// <summary>Blocks until a datagram arrives; throws once the socket is disposed.</summary>
        int ReceiveFrom(byte[] buffer, out IPEndPoint remote);
    }

    /// <summary>
    /// A plain unconnected UDP socket on an ephemeral port. On net8.0 sends and receives
    /// cost no allocation (cached addresses: a relay socket talks to a handful of peers).
    /// </summary>
    internal sealed class DatagramSocket : IDatagramSocket {
        private readonly Socket _socket;
        private readonly IPEndPoint _any;
#if NET8_0_OR_GREATER
        private readonly EndPointCache _sources;
        private readonly SocketAddressCache _targets;
#endif

        public DatagramSocket(AddressFamily family) {
            _socket = UdpSockets.CreateClient(family);
            _any = new IPEndPoint(family == AddressFamily.InterNetworkV6 ? IPAddress.IPv6Any : IPAddress.Any, 0);
            _socket.Bind(_any);
#if NET8_0_OR_GREATER
            _sources = new EndPointCache(family, capacity: 4096);
            _targets = new SocketAddressCache(family, capacity: 4096);
#endif
        }

        public IPEndPoint LocalEndPoint => (IPEndPoint)_socket.LocalEndPoint!;

        public void SendTo(byte[] buffer, int offset, int count, IPEndPoint remote) {
            try {
#if NET8_0_OR_GREATER
                _socket.SendTo(buffer.AsSpan(offset, count), SocketFlags.None, _targets.For(remote));
#else
                _socket.SendTo(buffer, offset, count, SocketFlags.None, remote);
#endif
            }
            catch (SocketException ex) when (UdpSockets.IsTransient(ex.SocketErrorCode)) {
                // lost, like any datagram
            }
        }

        /// <summary>Single reader (the transport's receive thread).</summary>
        public int ReceiveFrom(byte[] buffer, out IPEndPoint remote) {
            while (true) {
                try {
#if NET8_0_OR_GREATER
                    int received = _socket.ReceiveFrom(buffer.AsSpan(), SocketFlags.None, _sources.Address);
                    remote = _sources.Resolve();
                    return received;
#else
                    EndPoint from = _any;
                    int received = _socket.ReceiveFrom(buffer, 0, buffer.Length, SocketFlags.None, ref from);
                    remote = (IPEndPoint)from;
                    return received;
#endif
                }
                catch (SocketException ex) when (UdpSockets.IsTransient(ex.SocketErrorCode)) {
                    // ICMP feedback for an earlier datagram: keep listening
                }
            }
        }


        public void Dispose() => _socket.Dispose();
    }
}
