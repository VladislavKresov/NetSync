using System;
using System.Collections.Concurrent;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using NetSync.Pipeline.Reliability;
using NetSync.Pipeline.Security;
using NetSync.Transports;

namespace NetSync.Peers {
    /// <summary>
    /// A connected client as seen by <see cref="NetServer"/>: one logical connection
    /// spanning one transport link per configured transport type.
    /// </summary>
    public sealed class NetConnection {
        internal const int JoinNone = 0;
        internal const int JoinPending = 1;   // approver running
        internal const int JoinAccepted = 2;
        internal const int JoinRejected = 3;

        private readonly NetServer _server;
        internal readonly ConcurrentDictionary<TransportType, TransportPeer> Links = new ConcurrentDictionary<TransportType, TransportPeer>();
        internal readonly long Token;
        internal readonly long CreatedAtMs;
        internal ReliableEndpoint? Reliable;
        internal ConnectionCrypto? Crypto;
        /// <summary>Packet authenticator per link slot (encrypted connections only).</summary>
        internal readonly LinkSecurity?[] Security = new LinkSecurity?[2];
        /// <summary>The client's ECDH key from its first Hello: later Hellos must carry the same.</summary>
        internal byte[]? ClientPublicKey;
        /// <summary>Identical on every link, so built once (the identity signature is costly).</summary>
        internal byte[] WelcomeMessage = Array.Empty<byte>();
        internal int JoinState;
        internal int AuthRejectSent;
        internal int Announced;
        internal int Degraded; // client gave up on UDP: UDP channels ride the TCP link
        internal int HoldsSlot; // counts against NetConfig.MaxConnections

        /// <summary>
        /// True when this client could not establish UDP and its UDP channels are carried
        /// over TCP (see <see cref="NetConfig.RequireAllTransports"/>).
        /// </summary>
        public bool IsDegraded => Volatile.Read(ref Degraded) == 1;
        internal int Closed;
        internal readonly Diagnostics.TrafficCounters Traffic = new Diagnostics.TrafficCounters();

        /// <summary>
        /// Free slot for your own per-connection state (player object, session data,
        /// a NetStreamReceiver…). NetSync never touches it.
        /// </summary>
        public object? Tag { get; set; }

        /// <summary>Negotiated packet protection (None without a key exchange).</summary>
        public CipherSuite CipherSuite { get; internal set; }

        /// <summary>True between ConnectionOpened and ConnectionClosed.</summary>
        public bool IsConnected => Volatile.Read(ref Announced) == 1 && Volatile.Read(ref Closed) == 0;

        /// <summary>Traffic snapshot for this connection.</summary>
        public Diagnostics.NetStatistics Statistics {
            get {
                var statistics = new Diagnostics.NetStatistics { PingMs = PingMs, IsDegraded = IsDegraded };
                Traffic.CopyTo(statistics);
                var reliable = Reliable;
                if (reliable != null) {
                    statistics.Retransmissions = reliable.Retransmissions;
                    statistics.FastRetransmissions = reliable.FastRetransmissions;
                    statistics.SmoothedRttMs = reliable.SmoothedRttMs;
                }
                return statistics;
            }
        }

        public long Id { get; }

        internal NetConnection(NetServer server, long id, long token, long createdAtMs) {
            _server = server;
            Id = id;
            Token = token;
            CreatedAtMs = createdAtMs;
        }

        /// <summary>Endpoint of the link on the given transport, null when not bound.</summary>
        public IPEndPoint? GetEndPoint(TransportType transport) {
            return Links.TryGetValue(transport, out var peer) ? peer.EndPoint : null;
        }

        /// <summary>RTT on the given transport link, -1 when unknown.</summary>
        public int GetPing(TransportType transport) {
            return Links.TryGetValue(transport, out var peer) ? peer.PingMs : -1;
        }

        /// <summary>Lowest RTT across bound links, -1 when unknown.</summary>
        public int PingMs {
            get {
                int best = -1;
                foreach (var peer in Links.Values) {
                    int ping = peer.PingMs;
                    if (ping >= 0 && (best < 0 || ping < best)) {
                        best = ping;
                    }
                }
                return best;
            }
        }

        /// <summary>
        /// Sends on a channel. <paramref name="data"/> is read only before this returns:
        /// the buffer may be reused (or returned to a pool) right after the call.
        /// </summary>
        public ValueTask SendAsync(byte channel, ReadOnlyMemory<byte> data, CancellationToken ct = default) {
            return _server.SendAsync(this, channel, data, ct);
        }

        /// <summary>Kicks this client: sends Bye on every link, then closes the connection.</summary>
        public Task DisconnectAsync() => _server.KickAsync(this, kicked: true);

        public override string ToString() {
            var tcp = GetEndPoint(TransportType.Tcp);
            var udp = GetEndPoint(TransportType.Udp);
            return $"Connection#{Id} (tcp={tcp?.ToString() ?? "-"}, udp={udp?.ToString() ?? "-"}, ping={PingMs}ms)";
        }
    }
}
