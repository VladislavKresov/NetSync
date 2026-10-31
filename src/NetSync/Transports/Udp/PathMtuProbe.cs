using System;
using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using NetSync.Internal;

namespace NetSync.Transports.Udp {
    /// <summary>
    /// Measures how large a UDP datagram can travel to the server unfragmented: sends
    /// "don't fragment" probes of decreasing size from a side socket and takes the largest
    /// one the server's UDP port confirms. Routers drop a DF datagram that doesn't fit
    /// (and the OS refuses to send one above a known path MTU), so a confirmed size is a
    /// size that crossed the whole path in one piece.
    ///
    /// IPv4 only: .NET exposes the DF flag for IPv4 sockets, and IPv6 hosts fragment at
    /// the source anyway — IPv6 links keep the always-safe default.
    /// </summary>
    internal static class PathMtuProbe {
        /// <summary>UDP payload sizes tried, largest first: Ethernet, PPPoE/tunnels, VPNs.</summary>
        private static readonly int[] Candidates = { 1472, 1452, 1400 };

        private const int AttemptsPerSize = 2;
        private const int ReplyHeaderSize = 7; // [MtuReply][nonce:4][size:2]

        /// <summary>The largest confirmed datagram size, or 0 when nothing could be confirmed.</summary>
        public static async Task<int> ProbeAsync(IPEndPoint server, int timeoutMs, CancellationToken ct) {
            if (server.AddressFamily != AddressFamily.InterNetwork) {
                return 0;
            }
            using var socket = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
            try {
                socket.DontFragment = true;
            }
            catch (Exception ex) when (ex is SocketException || ex is NotSupportedException || ex is PlatformNotSupportedException) {
                return 0; // can't forbid fragmentation: a probe would prove nothing
            }
            socket.Connect(server);

            var probe = new byte[Candidates[0]];
            probe[0] = (byte)PacketType.MtuProbe;
            var reply = new byte[64];
            Task<int>? receive = null;
            try {
                foreach (int size in Candidates) {
                    for (int attempt = 0; attempt < AttemptsPerSize; attempt++) {
                        RandomNumberGenerator.Fill(probe.AsSpan(1, 4));
                        try {
                            socket.Send(probe, 0, size, SocketFlags.None);
                        }
                        catch (SocketException) {
                            break; // bigger than the local or known path MTU: try the next size
                        }
                        long deadline = Environment.TickCount + timeoutMs;
                        while (true) {
                            int remaining = (int)(deadline - Environment.TickCount);
                            if (remaining <= 0) {
                                break;
                            }
                            receive ??= socket.ReceiveAsync(new ArraySegment<byte>(reply), SocketFlags.None);
                            if (await Task.WhenAny(receive, Task.Delay(remaining, ct)).ConfigureAwait(false) != receive) {
                                ct.ThrowIfCancellationRequested();
                                break;
                            }
                            var completed = receive;
                            receive = null;
                            int received;
                            try {
                                received = await completed.ConfigureAwait(false);
                            }
                            catch (SocketException) {
                                continue; // ICMP noise
                            }
                            if (received >= ReplyHeaderSize && reply[0] == (byte)PacketType.MtuReply &&
                                reply.AsSpan(1, 4).SequenceEqual(probe.AsSpan(1, 4)) &&
                                BinaryPrimitives.ReadUInt16BigEndian(reply.AsSpan(5)) == size) {
                                return size;
                            }
                        }
                    }
                }
                return 0;
            }
            finally {
                if (receive != null) {
                    receive.ObserveFaults();
                }
            }
        }
    }
}
