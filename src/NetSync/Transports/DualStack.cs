using System;
using System.Net;
using System.Net.Sockets;

namespace NetSync.Transports {
    /// <summary>
    /// Server sockets that accept IPv4 and IPv6 clients on one port (dual-stack), with
    /// an automatic IPv4-only fallback where the OS or runtime lacks IPv6. Apple's App
    /// Store review runs apps on IPv6-only (NAT64) networks, so servers reachable over
    /// IPv6 matter for mobile games.
    /// </summary>
    internal static class DualStack {
        public static TcpListener StartTcpListener(int port) {
            if (Socket.OSSupportsIPv6) {
                TcpListener? listener = null;
                try {
                    listener = new TcpListener(IPAddress.IPv6Any, port);
                    listener.Server.DualMode = true;
                    listener.Start();
                    return listener;
                }
                catch (Exception ex) when (ex is SocketException || ex is NotSupportedException || ex is PlatformNotSupportedException) {
                    try {
                        listener?.Stop();
                    }
                    catch { }
                    if (ex is SocketException socketError && socketError.SocketErrorCode == SocketError.AddressAlreadyInUse) {
                        throw; // a real conflict, not a missing-IPv6 problem
                    }
                }
            }
            var v4 = new TcpListener(IPAddress.Any, port);
            v4.Start();
            return v4;
        }

        /// <summary>IPv4 clients on a dual-stack socket show up as ::ffff:a.b.c.d — present them as plain IPv4.</summary>
        public static IPEndPoint Normalize(IPEndPoint endPoint) {
            return endPoint.Address.IsIPv4MappedToIPv6
                ? new IPEndPoint(endPoint.Address.MapToIPv4(), endPoint.Port)
                : endPoint;
        }
    }
}
