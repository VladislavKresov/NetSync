using System;
using System.Buffers.Binary;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using NetSync.Internal;
using NetSync.Transports;

namespace NetSync.Discovery {
    /// <summary>A reply to <see cref="NetQuery.QueryAsync"/>.</summary>
    public sealed class NetQueryResult {
        /// <summary>What the server's <see cref="Peers.NetServer.QueryHandler"/> returned.</summary>
        public byte[] Response { get; }

        /// <summary>Round trip of the answered attempt — a ready-made "ping" column for a server list.</summary>
        public double RoundTripMs { get; }

        internal NetQueryResult(byte[] response, double roundTripMs) {
            Response = response;
            RoundTripMs = roundTripMs;
        }
    }

    /// <summary>
    /// Connectionless request/response with a NetSync server's UDP port: ask a server for
    /// its name, map, player count… without connecting. The server answers through
    /// <see cref="Peers.NetServer.QueryHandler"/>.
    /// </summary>
    public static class NetQuery {
        private const int HeaderSize = 7;      // [type][id:4][requestLength:2]
        private const int ReplyHeaderSize = 5; // [type][id:4]

        /// <summary>Largest reply a query can ask for (keeps the padded query within one unfragmented datagram).</summary>
        public const int MaxResponseSize = 1200;

        /// <param name="server">The server's endpoint (its UDP port).</param>
        /// <param name="request">Query payload for the handler (may be empty).</param>
        /// <param name="maxResponseSize">
        /// Largest reply accepted. The query is padded to this size because a server never
        /// answers with more bytes than it received — that is what makes queries useless for
        /// reflection attacks.
        /// </param>
        /// <param name="timeoutMs">Wait per attempt.</param>
        /// <param name="attempts">Datagrams get lost: how many times to ask.</param>
        /// <param name="ct">Cancels the query.</param>
        /// <exception cref="TimeoutException">No reply after all attempts (server down, no handler, filtered).</exception>
        public static async Task<NetQueryResult> QueryAsync(IPEndPoint server, ReadOnlyMemory<byte> request, int maxResponseSize = 512,
                                                            int timeoutMs = 1000, int attempts = 3, CancellationToken ct = default) {
            if (server == null) {
                throw new ArgumentNullException(nameof(server));
            }
            if (maxResponseSize < 0 || maxResponseSize > MaxResponseSize) {
                throw new ArgumentOutOfRangeException(nameof(maxResponseSize), $"0..{MaxResponseSize}");
            }
            if (HeaderSize + request.Length > MaxResponseSize + HeaderSize) {
                throw new ArgumentException($"A query request is limited to {MaxResponseSize} bytes", nameof(request));
            }

            int datagramLength = Math.Max(HeaderSize + request.Length, ReplyHeaderSize + maxResponseSize);
            var datagram = new byte[datagramLength];
            datagram[0] = (byte)PacketType.Query;
            var id = new byte[4];
            RandomNumberGenerator.Fill(id);
            id.CopyTo(datagram, 1);
            BinaryPrimitives.WriteUInt16BigEndian(datagram.AsSpan(5), (ushort)request.Length);
            request.Span.CopyTo(datagram.AsSpan(HeaderSize));

            using var socket = new Socket(server.AddressFamily, SocketType.Dgram, ProtocolType.Udp);
            socket.Connect(server);
            var receiveBuffer = new byte[ReplyHeaderSize + MaxResponseSize];
            Task<int>? receive = null; // at most one receive in flight; it survives across attempts
            for (int attempt = 0; attempt < Math.Max(1, attempts); attempt++) {
                var stopwatch = Stopwatch.StartNew();
                try {
                    await socket.SendAsync(new ArraySegment<byte>(datagram), SocketFlags.None).ConfigureAwait(false);
                }
                catch (SocketException) {
                    continue; // e.g. ICMP unreachable from an earlier attempt
                }
                while (true) {
                    int remaining = timeoutMs - (int)stopwatch.ElapsedMilliseconds;
                    if (remaining <= 0) {
                        break;
                    }
                    receive ??= socket.ReceiveAsync(new ArraySegment<byte>(receiveBuffer), SocketFlags.None);
                    var winner = await Task.WhenAny(receive, Task.Delay(remaining, ct)).ConfigureAwait(false);
                    ct.ThrowIfCancellationRequested();
                    if (winner != receive) {
                        break; // next attempt; the pending receive stays armed
                    }
                    var completed = receive;
                    receive = null;
                    int received;
                    try {
                        received = await completed.ConfigureAwait(false);
                    }
                    catch (SocketException) {
                        continue; // ICMP noise (Windows reports it on receive): keep listening
                    }
                    if (received >= ReplyHeaderSize && receiveBuffer[0] == (byte)PacketType.QueryReply &&
                        receiveBuffer.AsSpan(1, 4).SequenceEqual(id)) {
                        return new NetQueryResult(receiveBuffer.AsSpan(ReplyHeaderSize, received - ReplyHeaderSize).ToArray(),
                                                  stopwatch.Elapsed.TotalMilliseconds);
                    }
                }
            }
            if (receive != null) {
                receive.ObserveFaults(); // completes when the socket closes
            }
            throw new TimeoutException($"No reply to the query from {server} after {attempts} attempt(s)");
        }
    }
}
