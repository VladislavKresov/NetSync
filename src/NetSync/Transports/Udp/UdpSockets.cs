using System;
using System.Net;
using System.Net.Sockets;
using System.Runtime.InteropServices;

namespace NetSync.Transports.Udp {
    /// <summary>Socket setup shared by the UDP transports.</summary>
    internal static class UdpSockets {
        /// <summary>Receive buffer large enough for any datagram.</summary>
        public const int ReceiveBufferSize = 64 * 1024;

        public static Socket CreateClient(AddressFamily family) {
            var socket = new Socket(family, SocketType.Dgram, ProtocolType.Udp);
            Configure(socket);
            return socket;
        }

        /// <summary>
        /// Server socket on all interfaces — dual-stack (IPv6 + IPv4) where possible, IPv4-only
        /// otherwise — or on one local address when <paramref name="address"/> is given.
        /// </summary>
        public static Socket BindServer(int port, IPAddress? address = null) {
            if (!DualStack.IsAnyAddress(address)) {
                var bound = new Socket(address!.AddressFamily, SocketType.Dgram, ProtocolType.Udp);
                try {
                    RequireExclusivePort(bound);
                    bound.Bind(new IPEndPoint(address, port));
                    Configure(bound);
                    return bound;
                }
                catch {
                    bound.Dispose();
                    throw;
                }
            }
            if (port != 0) {
                RefuseIfHeldOverIpv4(port);
            }
            if (Socket.OSSupportsIPv6) {
                Socket? socket = null;
                try {
                    socket = new Socket(AddressFamily.InterNetworkV6, SocketType.Dgram, ProtocolType.Udp) { DualMode = true };
                    RequireExclusivePort(socket);
                    socket.Bind(new IPEndPoint(IPAddress.IPv6Any, port));
                    Configure(socket);
                    return socket;
                }
                catch (Exception ex) when (ex is SocketException || ex is NotSupportedException || ex is PlatformNotSupportedException) {
                    socket?.Dispose();
                    if (ex is SocketException socketError && socketError.SocketErrorCode == SocketError.AddressAlreadyInUse) {
                        throw; // a real conflict, not a missing-IPv6 problem
                    }
                }
            }
            var v4 = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
            try {
                RequireExclusivePort(v4);
                v4.Bind(new IPEndPoint(IPAddress.Any, port));
                Configure(v4);
                return v4;
            }
            catch {
                v4.Dispose();
                throw;
            }
        }

        /// <summary>
        /// Windows and macOS let a dual-stack socket bind a port an IPv4 socket already holds, and
        /// the IPv4 socket then gets the IPv4 traffic: the server would hear nothing. Binding an
        /// IPv4 socket to the port first turns that into an ordinary "port in use" everywhere.
        /// (Windows also gets exclusive binding below; this covers the other platforms.)
        /// </summary>
        private static void RefuseIfHeldOverIpv4(int port) {
            using var probe = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
            probe.Bind(new IPEndPoint(IPAddress.Any, port)); // throws AddressAlreadyInUse when someone holds it
        }

        /// <summary>
        /// Windows lets an IPv4 socket and a dual-stack socket share a port number and delivers
        /// IPv4 traffic to the IPv4 one: a server would bind without an error and hear nothing.
        /// Exclusive binding turns that into an ordinary "port in use". Elsewhere it already is one.
        /// </summary>
        private static void RequireExclusivePort(Socket socket) {
            if (!RuntimeInformation.IsOSPlatform(OSPlatform.Windows)) {
                return;
            }
            try {
                socket.ExclusiveAddressUse = true;
            }
            catch (Exception ex) when (ex is SocketException || ex is PlatformNotSupportedException || ex is NotSupportedException) {
                // Not available on this runtime: binding works as before.
            }
        }

        private static void Configure(Socket socket) {
            // Default OS buffers (~64 KB) drop datagrams under bursts; real-time and media
            // traffic needs headroom, and a server fans in traffic from many clients.
            socket.ReceiveBufferSize = 1 << 20;
            socket.SendBufferSize = 1 << 20;
            DisableIcmpConnectionReset(socket);
        }

        /// <summary>
        /// Closes a socket whose receive loop may be blocked in it, waking that loop first (the
        /// loop must already be cancelled). Closing alone isn't enough everywhere: on macOS a
        /// UDP socket that isn't connected doesn't wake a thread blocked in it, and the close
        /// waits for that call — a stop would hang until the next datagram arrived. So a
        /// connected socket is shut down, and any other one gets an empty datagram from itself;
        /// the loops skip empty datagrams and then see the cancellation.
        /// (A receive timeout isn't an option: a socket whose receive timed out is in an
        /// undefined state on Windows, and can lose a datagram arriving just then elsewhere.)
        /// </summary>
        public static void Close(Socket socket) {
            try {
                if (socket.Connected) {
                    socket.Shutdown(SocketShutdown.Both);
                }
                else if (socket.LocalEndPoint is IPEndPoint local) {
                    socket.SendTo(Array.Empty<byte>(), SocketFlags.None, new IPEndPoint(WakeAddress(local.Address), local.Port));
                }
            }
            catch (Exception) {
                // already closed, or no loopback: closing is all that's left
            }
            socket.Dispose();
        }

        private static IPAddress WakeAddress(IPAddress bound) {
            if (bound.Equals(IPAddress.Any)) {
                return IPAddress.Loopback;
            }
            if (bound.Equals(IPAddress.IPv6Any)) {
                return IPAddress.IPv6Loopback;
            }
            return bound;
        }

        /// <summary>
        /// Windows reports an ICMP "port unreachable" for an earlier datagram as an error on
        /// the next receive (WSAECONNRESET), which would otherwise kill a receive loop over a
        /// transient condition (e.g. a restarting server). Turn that off; link liveness is the
        /// keepalive's job. Elsewhere this is a no-op (receive loops tolerate the errors too).
        /// </summary>
        private static void DisableIcmpConnectionReset(Socket socket) {
            if (!RuntimeInformation.IsOSPlatform(OSPlatform.Windows)) {
                return;
            }
            try {
                const int SIO_UDP_CONNRESET = -1744830452;
                socket.IOControl(SIO_UDP_CONNRESET, new byte[] { 0 }, null);
            }
            catch (Exception ex) when (ex is SocketException || ex is PlatformNotSupportedException || ex is NotSupportedException) {
                // Not available on this runtime: the receive loops skip the error instead.
            }
        }

        /// <summary>Errors a datagram socket reports for earlier sends (ICMP); never fatal for the socket.</summary>
        public static bool IsTransient(SocketError error) {
            return error == SocketError.WouldBlock ||      // a momentarily full send buffer
                   error == SocketError.ConnectionReset ||
                   error == SocketError.ConnectionRefused ||
                   error == SocketError.HostUnreachable ||
                   error == SocketError.NetworkUnreachable ||
                   error == SocketError.MessageSize ||
                   error == SocketError.NoBufferSpaceAvailable;
        }
    }
}
