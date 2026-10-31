using System;
using System.Collections.Generic;
using NetSync.Diagnostics;

namespace NetSync {
    /// <summary>
    /// A transport slot. The name says what runs there by default; the contract is what
    /// matters when you plug in your own implementation via
    /// <see cref="NetConfig.TransportFactories"/>.
    /// </summary>
    public enum TransportType : byte {
        /// <summary>Reliable, ordered messages (TCP by default; e.g. WebSocket when replaced).</summary>
        Tcp = 0,
        /// <summary>Unreliable datagrams (UDP by default); the reliability layer runs on top.</summary>
        Udp = 1
    }

    public enum EventDelivery {
        /// <summary>
        /// Events accumulate in a queue and fire inside PollEvents() on the caller's
        /// thread. The right choice for game loops (call PollEvents from Update).
        /// </summary>
        Polled,
        /// <summary>
        /// Events fire directly on network threads for minimal latency. Handlers must
        /// be thread-safe and fast.
        /// </summary>
        Immediate
    }

    public enum KeyExchangeMode {
        /// <summary>No encryption keys; channels with Encryption=true are rejected.</summary>
        None = 0,
        /// <summary>
        /// Both sides share a secret key out-of-band; per-connection keys are derived
        /// from it and the session token. Simple and works everywhere.
        /// </summary>
        PresharedKey = 1,
        /// <summary>
        /// Ephemeral ECDH (P-256) during the handshake: no pre-shared secret needed.
        /// Give the server a <see cref="EncryptionConfig.ServerIdentity"/> and pin its key on
        /// clients (<see cref="EncryptionConfig.TrustedServerKeys"/>) to stop an active
        /// man-in-the-middle; without pinning it only defeats passive eavesdropping.
        /// </summary>
        Ecdh = 2
    }

    /// <summary>Exponential backoff with jitter for <see cref="NetConfig.Reconnect"/>.</summary>
    public sealed class ReconnectPolicy {
        /// <summary>Give up after this many failed attempts; 0 or less = keep trying forever.</summary>
        public int MaxAttempts { get; set; } = 10;
        public int InitialDelayMs { get; set; } = 500;
        public int MaxDelayMs { get; set; } = 10_000;
    }

    /// <summary>How per-connection encryption keys are established.</summary>
    public sealed class EncryptionConfig {
        public KeyExchangeMode Mode { get; set; } = KeyExchangeMode.None;

        /// <summary>Shared secret for <see cref="KeyExchangeMode.PresharedKey"/>; 32 bytes recommended, 16 minimum.</summary>
        public byte[]? PresharedKey { get; set; }

        /// <summary>
        /// Server only, <see cref="KeyExchangeMode.Ecdh"/>: the server's long-term key. It signs
        /// every handshake so clients can verify they talk to this server and not to a
        /// man-in-the-middle.
        /// </summary>
        public NetIdentity? ServerIdentity { get; set; }

        /// <summary>
        /// Client only, <see cref="KeyExchangeMode.Ecdh"/>: public keys (<see cref="NetIdentity.PublicKey"/>)
        /// of servers this client trusts. When non-empty, a connection succeeds only if the
        /// server proves it holds one of them. Several keys allow rotation.
        /// </summary>
        public List<byte[]> TrustedServerKeys { get; } = new List<byte[]>();
    }

    /// <summary>
    /// A logical channel: which transport carries it and with what guarantees.
    /// </summary>
    public sealed class ChannelConfig {
        public TransportType Transport { get; set; }

        /// <summary>
        /// Delivery guarantees. Only applies to UDP channels — TCP is inherently
        /// reliable-ordered and ignores this setting. Reliable modes also enable
        /// automatic fragmentation, lifting the single-datagram size limit.
        /// </summary>
        public ReliabilityMode Reliability { get; set; } = ReliabilityMode.Unreliable;

        /// <summary>
        /// Deflate-compress payloads on this channel. Small payloads and payloads that
        /// do not shrink are sent uncompressed automatically (flagged per packet).
        /// </summary>
        public bool Compression { get; set; }

        /// <summary>
        /// Encrypt payloads on this channel with the connection's negotiated cipher suite
        /// (AES-256-GCM when both sides run net8.0, AES-256-CBC + HMAC-SHA256 otherwise — see
        /// <see cref="CipherSuite"/>). Requires <see cref="NetConfig.Encryption"/> mode ≠ None.
        /// </summary>
        public bool Encryption { get; set; }

        /// <summary>
        /// How this channel shares the link (see <see cref="ChannelPriority"/>). A sender-side
        /// policy: client and server may choose differently, it is not part of the handshake.
        /// </summary>
        public ChannelPriority Priority { get; set; } = ChannelPriority.Normal;

        internal bool NeedsTransform => Compression || Encryption;

        public ChannelConfig() { }

        public ChannelConfig(TransportType transport, ReliabilityMode reliability = ReliabilityMode.Unreliable,
                             bool compression = false, bool encryption = false,
                             ChannelPriority priority = ChannelPriority.Normal) {
            Transport = transport;
            Reliability = reliability;
            Compression = compression;
            Encryption = encryption;
            Priority = priority;
        }
    }

    /// <summary>
    /// Shared configuration for <see cref="Peers.NetClient"/> and <see cref="Peers.NetServer"/>.
    /// Client and server must agree on the channel table (same ids, same transports).
    /// When <see cref="Channels"/> is left empty, two defaults are used:
    /// channel 0 = TCP, channel 1 = UDP.
    /// </summary>
    public sealed class NetConfig {
        public Dictionary<byte, ChannelConfig> Channels { get; } = new Dictionary<byte, ChannelConfig>();

        public EventDelivery EventDelivery { get; set; } = EventDelivery.Polled;

        /// <summary>Keepalive ping period per transport link.</summary>
        public int PingIntervalMs { get; set; } = 1000;

        /// <summary>Link is considered dead after this long without a pong; 0 disables.</summary>
        public int PingTimeoutMs { get; set; } = 5000;

        /// <summary>Overall budget for ConnectAsync including the NetSync handshake.</summary>
        public int ConnectTimeoutMs { get; set; } = 10000;

        public const int DefaultMaxMessageSize = 16 * 1024 * 1024;

        /// <summary>
        /// Largest application message, in bytes, after reassembly and decompression.
        /// Sending more throws; receiving more (a hostile or misconfigured peer) closes
        /// the connection without allocating. Must be equal on both sides.
        /// </summary>
        public int MaxMessageSize { get; set; } = DefaultMaxMessageSize;

        /// <summary>
        /// A socket write stuck for longer than this means the remote stopped reading;
        /// the connection is closed with Timeout instead of hanging. 0 disables.
        /// </summary>
        public int WriteTimeoutMs { get; set; } = 15000;

        /// <summary>
        /// Polled mode only: cap on undelivered events. When PollEvents() isn't called
        /// for a while, data events beyond the cap are dropped (with a warning) instead
        /// of growing memory without bound. Connect/disconnect events are never dropped.
        /// </summary>
        public int MaxPendingEvents { get; set; } = 100_000;

        /// <summary>
        /// Server only: open connections beyond this are refused with "server full"
        /// (<see cref="Peers.ConnectionRejectedException"/> on the client). 0 = unlimited.
        /// </summary>
        public int MaxConnections { get; set; }

        /// <summary>Key exchange for channels with Encryption enabled.</summary>
        public EncryptionConfig Encryption { get; set; } = new EncryptionConfig();

        /// <summary>
        /// Replace the implementation behind a transport slot (see <see cref="Transports.ITransportFactory"/>).
        /// Empty = built-in TCP and UDP. Not part of the handshake: client and server may use
        /// different implementations as long as they interoperate on the wire.
        /// </summary>
        public Dictionary<TransportType, Transports.ITransportFactory> TransportFactories { get; } =
            new Dictionary<TransportType, Transports.ITransportFactory>();

        /// <summary>
        /// When false (default) and the channel table uses both slots, a client whose UDP
        /// handshake doesn't complete within <see cref="TransportFallbackTimeoutMs"/> after
        /// TCP succeeded keeps the connection and carries its UDP channels over TCP
        /// (<see cref="Peers.NetClient.IsDegraded"/>). Typical cause: firewalls or networks
        /// that block UDP. When true, such a connect fails instead.
        /// </summary>
        public bool RequireAllTransports { get; set; }

        /// <summary>How long to wait for UDP after TCP connected before falling back.</summary>
        public int TransportFallbackTimeoutMs { get; set; } = 3000;

        /// <summary>
        /// Client only: after connecting, measure the path MTU to the server (IPv4, built-in UDP)
        /// and let reliable/fragmented UDP traffic use the largest datagrams that cross it
        /// unfragmented — up to ~1450 payload bytes instead of the safe 1100. Runs in the
        /// background; the connection works meanwhile.
        /// </summary>
        public bool MtuDiscovery { get; set; } = true;

        /// <summary>
        /// Client only: reconnect automatically when an established connection is lost
        /// (timeout, error, server restart). Null = off. Never triggers after Disconnect()
        /// or a kick. Each reconnect is a brand-new connection on the server side.
        /// </summary>
        public ReconnectPolicy? Reconnect { get; set; }

        /// <summary>
        /// Development only: simulate latency, jitter, loss and duplication on this peer's
        /// links (see <see cref="Transports.Simulation.NetworkSimulation"/>). Null = the real network, untouched.
        /// </summary>
        public Transports.Simulation.NetworkSimulation? Simulation { get; set; }

        public INetLogger Logger { get; set; } = NullNetLogger.Instance;

        internal Dictionary<byte, ChannelConfig> BuildChannelTable() {
            var table = new Dictionary<byte, ChannelConfig>();
            if (Channels.Count == 0) {
                table[0] = new ChannelConfig(TransportType.Tcp);
                table[1] = new ChannelConfig(TransportType.Udp);
            }
            else {
                foreach (var kvp in Channels) {
                    table[kvp.Key] = new ChannelConfig(kvp.Value.Transport, kvp.Value.Reliability,
                                                       kvp.Value.Compression, kvp.Value.Encryption, kvp.Value.Priority);
                }
            }
            return table;
        }

        /// <summary>
        /// Transport/reliability-level cap: the user limit plus room for peer headers
        /// and the channel transform (flags, IV, MAC, padding).
        /// </summary>
        internal int WireMessageLimit => MaxMessageSize + 128;

        internal Transports.ITransportFactory GetTransportFactory(TransportType slot) {
            if (!TransportFactories.TryGetValue(slot, out var factory)) {
                factory = slot switch {
                    TransportType.Tcp => Transports.TcpTransportFactory.Instance,
                    TransportType.Udp => Transports.UdpTransportFactory.Instance,
                    _ => throw new NotSupportedException($"Transport slot {slot} has no implementation")
                };
            }
            return Simulation == null
                ? factory
                : new Transports.Simulation.SimulatedTransportFactory(factory, Simulation, datagram: slot == TransportType.Udp);
        }

        internal Transports.TransportSettings BuildTransportSettings() {
            return new Transports.TransportSettings {
                PingIntervalMs = PingIntervalMs,
                PingTimeoutMs = PingTimeoutMs,
                WriteTimeoutMs = WriteTimeoutMs,
                MaxMessageSize = WireMessageLimit,
                Logger = Logger
            };
        }

        /// <summary>
        /// FNV-1a over everything both sides must agree on: the channel table (ids,
        /// transports, reliability, compression, encryption flags) and MaxMessageSize.
        /// Exchanged in Hello so a mismatch fails the connect with a clear error.
        /// </summary>
        internal uint ComputeConfigHash(Dictionary<byte, ChannelConfig> channelTable) {
            uint hash = 2166136261;
            void Mix(int value) {
                for (int i = 0; i < 4; i++) {
                    hash = (hash ^ (byte)(value >> (i * 8))) * 16777619;
                }
            }

            var ids = new List<byte>(channelTable.Keys);
            ids.Sort();
            foreach (var id in ids) {
                var channel = channelTable[id];
                Mix(id);
                Mix((int)channel.Transport);
                Mix((int)channel.Reliability);
                Mix(channel.Compression ? 1 : 0);
                Mix(channel.Encryption ? 1 : 0);
            }
            Mix(MaxMessageSize);
            return hash;
        }

        internal void ValidateLimits() {
            if (MaxMessageSize < 1024) {
                throw new InvalidOperationException("NetConfig.MaxMessageSize must be at least 1024 bytes.");
            }
            if (MaxPendingEvents < 1) {
                throw new InvalidOperationException("NetConfig.MaxPendingEvents must be positive.");
            }
            if (MaxConnections < 0) {
                throw new InvalidOperationException("NetConfig.MaxConnections must be 0 (unlimited) or positive.");
            }
        }

        internal void ValidateSecurity(Dictionary<byte, ChannelConfig> channelTable) {
            bool anyEncrypted = false;
            foreach (var channel in channelTable.Values) {
                if (channel.Encryption) {
                    anyEncrypted = true;
                    break;
                }
            }
            if (anyEncrypted && Encryption.Mode == KeyExchangeMode.None) {
                throw new InvalidOperationException(
                    "A channel has Encryption enabled but NetConfig.Encryption.Mode is None. " +
                    "Set Mode to PresharedKey (with a key) or Ecdh.");
            }
            if (Encryption.Mode == KeyExchangeMode.Ecdh && !Pipeline.Security.EcdhKeyExchange.IsSupported) {
                throw new NotSupportedException(
                    "KeyExchangeMode.Ecdh needs P-256 ECDiffieHellman, which this runtime doesn't provide " +
                    "(some Mono/IL2CPP builds lack it). Use KeyExchangeMode.PresharedKey instead.");
            }
            if (Encryption.Mode == KeyExchangeMode.PresharedKey &&
                (Encryption.PresharedKey == null || Encryption.PresharedKey.Length < 16)) {
                throw new InvalidOperationException(
                    "KeyExchangeMode.PresharedKey requires a PresharedKey of at least 16 bytes (32 recommended).");
            }
            bool usesIdentity = Encryption.ServerIdentity != null || Encryption.TrustedServerKeys.Count > 0;
            if (usesIdentity && Encryption.Mode != KeyExchangeMode.Ecdh) {
                throw new InvalidOperationException(
                    "ServerIdentity / TrustedServerKeys authenticate the ECDH key exchange: set Encryption.Mode to Ecdh " +
                    "(PresharedKey already authenticates both sides through the shared key).");
            }
            foreach (var key in Encryption.TrustedServerKeys) {
                if (key == null || key.Length != NetIdentity.PublicKeySize) {
                    throw new InvalidOperationException(
                        $"Every TrustedServerKeys entry must be a {NetIdentity.PublicKeySize}-byte NetIdentity.PublicKey.");
                }
            }
            if (usesIdentity && !NetIdentity.IsSupported) {
                throw new NotSupportedException(
                    "Server identity needs ECDSA P-256, which this runtime doesn't provide. Use KeyExchangeMode.PresharedKey instead.");
            }
        }

        /// <summary>Some UDP channel runs a reliability mode: links need a reliability engine.</summary>
        internal static bool HasReliableUdpChannels(Dictionary<byte, ChannelConfig> channelTable) {
            foreach (var channel in channelTable.Values) {
                if (channel.Transport == TransportType.Udp && channel.Reliability != ReliabilityMode.Unreliable) {
                    return true;
                }
            }
            return false;
        }

        /// <summary>The configuration of an outgoing message's channel; throws for an unknown channel or an oversized message.</summary>
        internal ChannelConfig ResolveOutgoing(Dictionary<byte, ChannelConfig> channelTable, byte channel, int length) {
            if (!channelTable.TryGetValue(channel, out var channelConfig)) {
                throw new ArgumentException($"Channel {channel} is not configured", nameof(channel));
            }
            if (length > MaxMessageSize) {
                throw new ArgumentException($"Message of {length} bytes exceeds NetConfig.MaxMessageSize ({MaxMessageSize}).", "data");
            }
            return channelConfig;
        }

        internal List<TransportType> BuildTransportList(Dictionary<byte, ChannelConfig> channelTable) {
            var types = new List<TransportType>();
            foreach (var channel in channelTable.Values) {
                if (!types.Contains(channel.Transport)) {
                    types.Add(channel.Transport);
                }
            }
            // TCP first: with port 0 the TCP listener picks the number and UDP binds to it.
            types.Sort();
            return types;
        }
    }
}
