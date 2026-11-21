using System;
using System.Buffers.Binary;
using System.Text;

namespace NetSync.Peers {
    /// <summary>
    /// Peer-level messages, carried inside transport Data payloads.
    ///
    /// Handshake (plaintext):
    /// Hello      [0x01][ver=4][token:8][configHash:4][keyExchange:1][suites:1][appId:8][appVersion:2][features:4]
    ///            [pubLen:1][pub…][cookieLen:1][cookie…][extLen:2][extensions…]
    ///             — client introduces itself on every link; the token groups the links into one
    ///               connection, configHash covers the channel table and MaxMessageSize, suites
    ///               is the bitmask of cipher suites the client can run, pub its ephemeral
    ///               X25519 key (keyed modes).
    /// Welcome    [0x02][connectionId:8][suite:1][features:4][pubLen:1][pub…][idLen:1][identityKey…][extLen:2][extensions…]
    ///             — server bound the link; its ephemeral X25519 key, the features both sides
    ///               support, and its long-term public key when it has an identity.
    ///
    /// Growing without a new version: features is a bitmask (the server answers with the
    /// bits both sides know), extensions a block of [type:1][length:2][value] entries.
    /// A receiver ignores feature bits and extension types it does not know.
    /// Reject     [0x03][reason:1]                   — handshake refused (version/encryption/config/auth).
    /// Retry      [0x06][cookie:16]                  — UDP-only servers: prove your address, resend Hello with this cookie.
    ///
    /// Session (inside the authenticated envelope on encrypted connections):
    /// Join       [0x07][connectData…]               — client: open the connection (data encrypted when keyed).
    /// JoinAccept [0x08]                             — server: connection open.
    /// JoinReject [0x09][reason utf8…]               — server application refused.
    /// Bye        [0x04][reason:1]                   — graceful disconnect notice.
    /// Degrade    [0x05]                             — on TCP: "UDP failed, carry the UDP channels over TCP". From the
    ///                                                client while connecting; from either side later (FeatureLiveDegrade).
    /// TimeReq    [0x0A][clientTime:8]               — client: what time is it on the server?
    /// TimeResp   [0x0B][clientTime:8][serverTime:8] — server: echo + its clock (double ms, bit pattern BE).
    /// PathMtu    [0x0C][datagramSize:2]             — client: the measured path MTU; use it for fragments too.
    /// Data       [0x10][channel:1][payload…]        — application data.
    /// RelData / RelAck                              — reliability layer (see ReliableEndpoint).
    /// Secure     [0x40][counter:8][tag:16][inner…]  — authenticated envelope (see LinkSecurity).
    /// </summary>
    internal static class PeerProtocol {
        public const byte ProtocolVersion = 4;

        public const byte MsgHello = 0x01;
        public const byte MsgWelcome = 0x02;
        public const byte MsgReject = 0x03;
        public const byte MsgBye = 0x04;
        public const byte MsgDegrade = 0x05;
        public const byte MsgRetry = 0x06;
        public const byte MsgJoin = 0x07;
        public const byte MsgJoinAccept = 0x08;
        public const byte MsgJoinReject = 0x09;
        public const byte MsgTimeRequest = 0x0A;
        public const byte MsgTimeResponse = 0x0B;
        public const byte MsgPathMtu = 0x0C;
        public const byte MsgSecure = 0x40;
        public const byte MsgData = 0x10;
        // 0x11 is taken: stream and message transports recognise a fragment by that first body
        // byte (PacketType.Fragment), so no peer message may ever start with it.
        /// <summary>Reliability-layer data: [0x20][flags:1][channel:1][seq:2 BE][payload…]</summary>
        public const byte MsgRelData = 0x20;
        /// <summary>Reliability-layer ack: [0x21][channel:1][anchorSeq:2 BE][mask:4 BE]</summary>
        public const byte MsgRelAck = 0x21;

        public const int DataHeaderSize = 2;
        public const int CookieSize = 16;

        /// <summary>Largest connect data a client may send with Join.</summary>
        public const int MaxConnectDataSize = 4096;

        public const byte RejectVersionMismatch = 1;
        public const byte RejectEncryptionMismatch = 2;
        public const byte RejectConfigMismatch = 3;
        /// <summary>The server can't verify the client's envelopes: the keys differ (wrong PSK).</summary>
        public const byte RejectAuthenticationFailed = 4;
        /// <summary>The client belongs to another application or application protocol version.</summary>
        public const byte RejectApplicationMismatch = 5;

        /// <summary>
        /// Bit 2: losing UDP mid-session moves the UDP channels to TCP instead of closing the
        /// connection. Either side may send Degrade; reliable packets still unacknowledged are
        /// resent over TCP before new messages follow (see UdpHandover). Bits 0 and 1 are reserved.
        /// </summary>
        public const uint FeatureLiveDegrade = 1u << 2;

        /// <summary>Feature bits this build understands.</summary>
        public const uint LocalFeatures = FeatureLiveDegrade;

        public const byte ByeGraceful = 0;   // client leaving / server shutting down
        public const byte ByeKicked = 1;     // server removed this client on purpose

        // ------------------------------------------------------------------ Hello

        internal readonly struct Hello {
            public readonly byte Version;
            public readonly long Token;
            public readonly uint ConfigHash;
            public readonly byte KeyExchange;
            public readonly byte Suites;
            public readonly ulong AppId;
            public readonly ushort AppVersion;
            public readonly uint Features;
            public readonly int PublicKeyOffset;
            public readonly int PublicKeyLength;
            public readonly int CookieOffset;
            public readonly int CookieLength;

            public Hello(byte version, long token, uint configHash, byte keyExchange, byte suites, ulong appId, ushort appVersion,
                         uint features, int publicKeyOffset, int publicKeyLength, int cookieOffset, int cookieLength) {
                Version = version;
                Token = token;
                ConfigHash = configHash;
                KeyExchange = keyExchange;
                Suites = suites;
                AppId = appId;
                AppVersion = appVersion;
                Features = features;
                PublicKeyOffset = publicKeyOffset;
                PublicKeyLength = publicKeyLength;
                CookieOffset = cookieOffset;
                CookieLength = cookieLength;
            }
        }

        private const int HelloFixedSize = 31; // type..pubLen

        public static byte[] EncodeHello(long token, uint configHash, KeyExchangeMode keyExchange, byte suites,
                                         byte[]? publicKey, byte[]? cookie, ulong appId = 0, ushort appVersion = 0,
                                         uint features = LocalFeatures) {
            int keyLength = publicKey?.Length ?? 0;
            int cookieLength = cookie?.Length ?? 0;
            var buf = new byte[HelloFixedSize + keyLength + 1 + cookieLength + 2];
            buf[0] = MsgHello;
            buf[1] = ProtocolVersion;
            BinaryPrimitives.WriteInt64BigEndian(buf.AsSpan(2), token);
            BinaryPrimitives.WriteUInt32BigEndian(buf.AsSpan(10), configHash);
            buf[14] = (byte)keyExchange;
            buf[15] = suites;
            BinaryPrimitives.WriteUInt64BigEndian(buf.AsSpan(16), appId);
            BinaryPrimitives.WriteUInt16BigEndian(buf.AsSpan(24), appVersion);
            BinaryPrimitives.WriteUInt32BigEndian(buf.AsSpan(26), features);
            buf[30] = (byte)keyLength;
            publicKey?.CopyTo(buf.AsSpan(HelloFixedSize));
            buf[HelloFixedSize + keyLength] = (byte)cookieLength;
            cookie?.CopyTo(buf.AsSpan(HelloFixedSize + keyLength + 1));
            // The last two bytes are the length of the extension block: empty.
            return buf;
        }

        /// <summary>Reads just the version (to reject old peers clearly before parsing the rest).</summary>
        public static bool TryReadHelloVersion(ReadOnlySpan<byte> message, out byte version) {
            version = message.Length >= 2 ? message[1] : (byte)0;
            return message.Length >= 2;
        }

        public static bool TryParseHello(byte[] buffer, int offset, int count, out Hello hello) {
            hello = default;
            if (count < HelloFixedSize + 1 + 2) {
                return false;
            }
            var span = new ReadOnlySpan<byte>(buffer, offset, count);
            int keyLength = span[30];
            int cookieLengthAt = HelloFixedSize + keyLength;
            if (count < cookieLengthAt + 1) {
                return false;
            }
            int cookieLength = span[cookieLengthAt];
            int extensionsAt = cookieLengthAt + 1 + cookieLength;
            if (!ExtensionsWellFormed(span, extensionsAt)) {
                return false;
            }
            hello = new Hello(span[1], BinaryPrimitives.ReadInt64BigEndian(span.Slice(2)), BinaryPrimitives.ReadUInt32BigEndian(span.Slice(10)),
                              span[14], span[15], BinaryPrimitives.ReadUInt64BigEndian(span.Slice(16)),
                              BinaryPrimitives.ReadUInt16BigEndian(span.Slice(24)), BinaryPrimitives.ReadUInt32BigEndian(span.Slice(26)),
                              offset + HelloFixedSize, keyLength, offset + cookieLengthAt + 1, cookieLength);
            return true;
        }

        /// <summary>
        /// The extension block [length:2][entries…] must fit the message; its entries
        /// ([type:1][length:2][value]) are skipped — none are defined yet, and unknown ones
        /// are ignored by design.
        /// </summary>
        private static bool ExtensionsWellFormed(ReadOnlySpan<byte> message, int at) {
            if (at + 2 > message.Length) {
                return false;
            }
            int length = BinaryPrimitives.ReadUInt16BigEndian(message.Slice(at));
            return at + 2 + length <= message.Length;
        }

        // ---------------------------------------------------------------- Welcome

        internal readonly struct Welcome {
            public readonly long ConnectionId;
            public readonly CipherSuite Suite;
            public readonly uint Features;
            public readonly byte[] PublicKey;
            public readonly byte[] IdentityKey;

            public Welcome(long connectionId, CipherSuite suite, uint features, byte[] publicKey, byte[] identityKey) {
                ConnectionId = connectionId;
                Suite = suite;
                Features = features;
                PublicKey = publicKey;
                IdentityKey = identityKey;
            }
        }

        public static byte[] EncodeWelcome(long connectionId, CipherSuite suite, uint features, byte[]? publicKey, byte[]? identityKey) {
            int keyLength = publicKey?.Length ?? 0;
            int idLength = identityKey?.Length ?? 0;
            var buf = new byte[1 + 8 + 1 + 4 + 1 + keyLength + 1 + idLength + 2];
            int at = 0;
            buf[at++] = MsgWelcome;
            BinaryPrimitives.WriteInt64BigEndian(buf.AsSpan(at), connectionId);
            at += 8;
            buf[at++] = (byte)suite;
            BinaryPrimitives.WriteUInt32BigEndian(buf.AsSpan(at), features);
            at += 4;
            at = WriteBlock(buf, at, publicKey);
            WriteBlock(buf, at, identityKey);
            // The last two bytes are the length of the extension block: empty.
            return buf;
        }

        public static bool TryParseWelcome(byte[] buffer, int offset, int count, out Welcome welcome) {
            welcome = default;
            var span = new ReadOnlySpan<byte>(buffer, offset, count);
            if (span.Length < 1 + 8 + 1 + 4 + 2 + 2) {
                return false;
            }
            long connectionId = BinaryPrimitives.ReadInt64BigEndian(span.Slice(1));
            var suite = (CipherSuite)span[9];
            uint features = BinaryPrimitives.ReadUInt32BigEndian(span.Slice(10));
            int at = 14;
            if (!TryReadBlock(span, ref at, out var publicKey) ||
                !TryReadBlock(span, ref at, out var identityKey) ||
                !ExtensionsWellFormed(span, at)) {
                return false;
            }
            welcome = new Welcome(connectionId, suite, features, publicKey, identityKey);
            return true;
        }

        private static int WriteBlock(byte[] buf, int at, byte[]? block) {
            int length = block?.Length ?? 0;
            buf[at++] = (byte)length;
            block?.CopyTo(buf, at);
            return at + length;
        }

        private static bool TryReadBlock(ReadOnlySpan<byte> span, ref int at, out byte[] block) {
            block = Array.Empty<byte>();
            if (at >= span.Length) {
                return false;
            }
            int length = span[at++];
            if (at + length > span.Length) {
                return false;
            }
            block = span.Slice(at, length).ToArray();
            at += length;
            return true;
        }

        // ------------------------------------------------------------- the rest

        public static byte[] EncodeReject(byte reason) => new[] { MsgReject, reason };

        public static byte[] EncodeRetry(ReadOnlySpan<byte> cookie) {
            var buf = new byte[1 + CookieSize];
            buf[0] = MsgRetry;
            cookie.Slice(0, CookieSize).CopyTo(buf.AsSpan(1));
            return buf;
        }

        public static byte[] EncodeJoinReject(string reason) {
            var text = Encoding.UTF8.GetBytes(reason ?? string.Empty);
            int length = Math.Min(text.Length, ConnectionDecision.MaxReasonBytes);
            var buf = new byte[1 + length];
            buf[0] = MsgJoinReject;
            Array.Copy(text, 0, buf, 1, length);
            return buf;
        }

        public static string DecodeJoinReject(byte[] buffer, int offset, int count) {
            return count > 1 ? Encoding.UTF8.GetString(buffer, offset + 1, count - 1) : "rejected";
        }

        public static readonly byte[] JoinAccept = { MsgJoinAccept };

        public const int TimeRequestSize = 9;
        public const int TimeResponseSize = 17;

        public static void WriteTimeRequest(Span<byte> destination, double clientTimeMs) {
            destination[0] = MsgTimeRequest;
            BinaryPrimitives.WriteInt64BigEndian(destination.Slice(1), BitConverter.DoubleToInt64Bits(clientTimeMs));
        }

        public static void WriteTimeResponse(Span<byte> destination, ReadOnlySpan<byte> request, double serverTimeMs) {
            destination[0] = MsgTimeResponse;
            request.Slice(1, 8).CopyTo(destination.Slice(1));
            BinaryPrimitives.WriteInt64BigEndian(destination.Slice(9), BitConverter.DoubleToInt64Bits(serverTimeMs));
        }

        public static double ReadTime(ReadOnlySpan<byte> source) {
            return BitConverter.Int64BitsToDouble(BinaryPrimitives.ReadInt64BigEndian(source));
        }

        public static byte[] EncodePathMtu(int datagramSize) {
            var buf = new byte[3];
            buf[0] = MsgPathMtu;
            BinaryPrimitives.WriteUInt16BigEndian(buf.AsSpan(1), (ushort)datagramSize);
            return buf;
        }

        public static byte[] EncodeBye(byte reason) => new[] { MsgBye, reason };

        public static byte[] EncodeDegrade() => new[] { MsgDegrade };

        public static byte[] EncodeData(byte channel, ReadOnlySpan<byte> payload) {
            var buf = new byte[DataHeaderSize + payload.Length];
            buf[0] = MsgData;
            buf[1] = channel;
            payload.CopyTo(buf.AsSpan(DataHeaderSize));
            return buf;
        }

        // One cached [MsgData][channel] prefix per channel: the hot send path passes it
        // to the transport's scatter-gather overload and allocates nothing.
        private static readonly byte[]?[] DataPrefixCache = new byte[256][];

        public static byte[] DataPrefix(byte channel) {
            return DataPrefixCache[channel] ??= new[] { MsgData, channel };
        }
    }
}
