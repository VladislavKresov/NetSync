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

        /// <summary>Dual-stack (IPv6 + IPv4) server socket where possible, IPv4-only otherwise.</summary>
        public static Socket BindServer(int port) {
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
            if (!RuntimeInformation.IsOSPlatform(OSPlatform.Windows)) {
                // Our receive loops block in ReceiveFrom on their own thread, and stop by closing the
                // socket. On macOS closing a UDP socket doesn't wake a thread blocked in it, and the
                // close waits for that call: a stop would hang until the next datagram. Waking up
                // regularly bounds that wait; the loops treat the timeout as "nothing arrived".
                socket.ReceiveTimeout = ReceiveWakeMs;
            }
        }

        /// <summary>How often a blocked receive wakes up on its own (non-Windows).</summary>
        public const int ReceiveWakeMs = 250;

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
            return error == SocketError.TimedOut ||        // ReceiveWakeMs passed without a datagram
                   error == SocketError.WouldBlock ||
                   error == SocketError.ConnectionReset ||
                   error == SocketError.ConnectionRefused ||
                   error == SocketError.HostUnreachable ||
                   error == SocketError.NetworkUnreachable ||
                   error == SocketError.MessageSize ||
                   error == SocketError.NoBufferSpaceAvailable;
        }
    }
}
