using System;
using System.Collections.Generic;
using NetSync.Diagnostics;

namespace NetSync {
    public enum TransportType : byte {
        Tcp = 0,
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
        /// Note: the exchange is not authenticated — it protects against passive
        /// eavesdropping, not an active man-in-the-middle.
        /// </summary>
        Ecdh = 2
    }

    /// <summary>How per-connection encryption keys are established.</summary>
    public sealed class EncryptionConfig {
        public KeyExchangeMode Mode { get; set; } = KeyExchangeMode.None;

        /// <summary>Shared secret for <see cref="KeyExchangeMode.PresharedKey"/>; 32 bytes recommended, 16 minimum.</summary>
        public byte[]? PresharedKey { get; set; }
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
        /// Encrypt payloads on this channel (AES-256-CBC + HMAC-SHA256, encrypt-then-MAC —
        /// the same cipher on every target so netstandard2.1/Unity and net8.0 peers
        /// interoperate). Requires <see cref="NetConfig.Encryption"/> mode ≠ None.
        /// </summary>
        public bool Encryption { get; set; }

        internal bool NeedsTransform => Compression || Encryption;

        public ChannelConfig() { }

        public ChannelConfig(TransportType transport, ReliabilityMode reliability = ReliabilityMode.Unreliable,
                             bool compression = false, bool encryption = false) {
            Transport = transport;
            Reliability = reliability;
            Compression = compression;
            Encryption = encryption;
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

        /// <summary>Key exchange for channels with Encryption enabled.</summary>
        public EncryptionConfig Encryption { get; set; } = new EncryptionConfig();

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
                                                       kvp.Value.Compression, kvp.Value.Encryption);
                }
            }
            return table;
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
            if (Encryption.Mode == KeyExchangeMode.PresharedKey &&
                (Encryption.PresharedKey == null || Encryption.PresharedKey.Length < 16)) {
                throw new InvalidOperationException(
                    "KeyExchangeMode.PresharedKey requires a PresharedKey of at least 16 bytes (32 recommended).");
            }
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
