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
        private readonly NetServer _server;
        internal readonly ConcurrentDictionary<TransportType, TransportPeer> Links = new ConcurrentDictionary<TransportType, TransportPeer>();
        internal readonly long Token;
        internal readonly long CreatedAtMs;
        internal readonly object CryptoLock = new object();
        internal ReliableEndpoint? Reliable;
        internal ConnectionCrypto? Crypto;
        internal System.Security.Cryptography.ECDiffieHellman? Ecdh; // ephemeral, gone after key derivation
        internal byte[]? EcdhPublicKey; // cached for Welcome re-sends after Ecdh is disposed
        internal int Announced;
        internal int Closed;

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

        public ValueTask SendAsync(byte channel, ReadOnlyMemory<byte> data, CancellationToken ct = default) {
            return _server.SendAsync(this, channel, data, ct);
        }

        /// <summary>Kicks this client: sends Bye on every link, then closes the connection.</summary>
        public Task DisconnectAsync() => _server.KickAsync(this);

        public override string ToString() {
            var tcp = GetEndPoint(TransportType.Tcp);
            var udp = GetEndPoint(TransportType.Udp);
            return $"Connection#{Id} (tcp={tcp?.ToString() ?? "-"}, udp={udp?.ToString() ?? "-"}, ping={PingMs}ms)";
        }
    }
}
