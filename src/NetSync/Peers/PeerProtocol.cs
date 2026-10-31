using System;
using System.Buffers.Binary;
using System.Text;

namespace NetSync.Peers {
    /// <summary>
    /// Peer-level messages, carried inside transport Data payloads.
    ///
    /// Handshake (plaintext):
    /// Hello      [0x01][ver=3][token:8][configHash:4][keyExchange:1][suites:1][pubLen:1][pub…][cookieLen:1][cookie…]
    ///             — client introduces itself on every link; the token groups the links into one
    ///               connection, configHash covers the channel table and MaxMessageSize, suites
    ///               is the bitmask of cipher suites the client can run.
    /// Welcome    [0x02][connectionId:8][suite:1][pubLen:1][pub…][idLen:1][identityKey…][sigLen:1][signature…]
    ///             — server bound the link; ECDH public key, and when the server has an identity
    ///               its public key plus a signature over the handshake transcript.
    /// Reject     [0x03][reason:1]                   — handshake refused (version/encryption/config/auth).
    /// Retry      [0x06][cookie:16]                  — UDP-only servers: prove your address, resend Hello with this cookie.
    ///
    /// Session (inside the authenticated envelope on encrypted connections):
    /// Join       [0x07][connectData…]               — client: open the connection (data encrypted when keyed).
    /// JoinAccept [0x08]                             — server: connection open.
    /// JoinReject [0x09][reason utf8…]               — server application refused.
    /// Bye        [0x04][reason:1]                   — graceful disconnect notice.
    /// Degrade    [0x05]                             — client (on TCP): "UDP failed, carry my UDP channels over TCP".
    /// TimeReq    [0x0A][clientTime:8]               — client: what time is it on the server?
    /// TimeResp   [0x0B][clientTime:8][serverTime:8] — server: echo + its clock (double ms, bit pattern BE).
    /// PathMtu    [0x0C][datagramSize:2]             — client: the measured path MTU; use it for fragments too.
    /// Data       [0x10][channel:1][payload…]        — application data.
    /// RelData / RelAck                              — reliability layer (see ReliableEndpoint).
    /// Secure     [0x40][counter:8][tag:16][inner…]  — authenticated envelope (see LinkSecurity).
    /// </summary>
    internal static class PeerProtocol {
        public const byte ProtocolVersion = 3;

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

        public const byte ByeGraceful = 0;   // client leaving / server shutting down
        public const byte ByeKicked = 1;     // server removed this client on purpose

        // ------------------------------------------------------------------ Hello

        internal readonly struct Hello {
            public readonly byte Version;
            public readonly long Token;
            public readonly uint ConfigHash;
            public readonly byte KeyExchange;
            public readonly byte Suites;
            public readonly int PublicKeyOffset;
            public readonly int PublicKeyLength;
            public readonly int CookieOffset;
            public readonly int CookieLength;

            public Hello(byte version, long token, uint configHash, byte keyExchange, byte suites,
                         int publicKeyOffset, int publicKeyLength, int cookieOffset, int cookieLength) {
                Version = version;
                Token = token;
                ConfigHash = configHash;
                KeyExchange = keyExchange;
                Suites = suites;
                PublicKeyOffset = publicKeyOffset;
                PublicKeyLength = publicKeyLength;
                CookieOffset = cookieOffset;
                CookieLength = cookieLength;
            }
        }

        private const int HelloFixedSize = 17; // type..pubLen

        public static byte[] EncodeHello(long token, uint configHash, KeyExchangeMode keyExchange, byte suites,
                                         byte[]? publicKey, byte[]? cookie) {
            int keyLength = publicKey?.Length ?? 0;
            int cookieLength = cookie?.Length ?? 0;
            var buf = new byte[HelloFixedSize + keyLength + 1 + cookieLength];
            buf[0] = MsgHello;
            buf[1] = ProtocolVersion;
            BinaryPrimitives.WriteInt64BigEndian(buf.AsSpan(2), token);
            BinaryPrimitives.WriteUInt32BigEndian(buf.AsSpan(10), configHash);
            buf[14] = (byte)keyExchange;
            buf[15] = suites;
            buf[16] = (byte)keyLength;
            publicKey?.CopyTo(buf.AsSpan(HelloFixedSize));
            buf[HelloFixedSize + keyLength] = (byte)cookieLength;
            cookie?.CopyTo(buf.AsSpan(HelloFixedSize + keyLength + 1));
            return buf;
        }

        /// <summary>Reads just the version (to reject old peers clearly before parsing the rest).</summary>
        public static bool TryReadHelloVersion(ReadOnlySpan<byte> message, out byte version) {
            version = message.Length >= 2 ? message[1] : (byte)0;
            return message.Length >= 2;
        }

        public static bool TryParseHello(byte[] buffer, int offset, int count, out Hello hello) {
            hello = default;
            if (count < HelloFixedSize + 1) {
                return false;
            }
            var span = new ReadOnlySpan<byte>(buffer, offset, count);
            int keyLength = span[16];
            int cookieLengthAt = HelloFixedSize + keyLength;
            if (count < cookieLengthAt + 1) {
                return false;
            }
            int cookieLength = span[cookieLengthAt];
            if (count < cookieLengthAt + 1 + cookieLength) {
                return false;
            }
            hello = new Hello(span[1], BinaryPrimitives.ReadInt64BigEndian(span.Slice(2)), BinaryPrimitives.ReadUInt32BigEndian(span.Slice(10)),
                              span[14], span[15], offset + HelloFixedSize, keyLength, offset + cookieLengthAt + 1, cookieLength);
            return true;
        }

        // ---------------------------------------------------------------- Welcome

        internal readonly struct Welcome {
            public readonly long ConnectionId;
            public readonly CipherSuite Suite;
            public readonly byte[] PublicKey;
            public readonly byte[] IdentityKey;
            public readonly byte[] Signature;

            public Welcome(long connectionId, CipherSuite suite, byte[] publicKey, byte[] identityKey, byte[] signature) {
                ConnectionId = connectionId;
                Suite = suite;
                PublicKey = publicKey;
                IdentityKey = identityKey;
                Signature = signature;
            }
        }

        public static byte[] EncodeWelcome(long connectionId, CipherSuite suite, byte[]? publicKey, byte[]? identityKey, byte[]? signature) {
            int keyLength = publicKey?.Length ?? 0;
            int idLength = identityKey?.Length ?? 0;
            int sigLength = signature?.Length ?? 0;
            var buf = new byte[1 + 8 + 1 + 1 + keyLength + 1 + idLength + 1 + sigLength];
            int at = 0;
            buf[at++] = MsgWelcome;
            BinaryPrimitives.WriteInt64BigEndian(buf.AsSpan(at), connectionId);
            at += 8;
            buf[at++] = (byte)suite;
            at = WriteBlock(buf, at, publicKey);
            at = WriteBlock(buf, at, identityKey);
            WriteBlock(buf, at, signature);
            return buf;
        }

        public static bool TryParseWelcome(byte[] buffer, int offset, int count, out Welcome welcome) {
            welcome = default;
            var span = new ReadOnlySpan<byte>(buffer, offset, count);
            if (span.Length < 1 + 8 + 1 + 3) {
                return false;
            }
            long connectionId = BinaryPrimitives.ReadInt64BigEndian(span.Slice(1));
            var suite = (CipherSuite)span[9];
            int at = 10;
            if (!TryReadBlock(span, ref at, out var publicKey) ||
                !TryReadBlock(span, ref at, out var identityKey) ||
                !TryReadBlock(span, ref at, out var signature)) {
                return false;
            }
            welcome = new Welcome(connectionId, suite, publicKey, identityKey, signature);
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

        /// <summary>
        /// What the server's identity signs: everything that shapes the session keys, so a
        /// man-in-the-middle can neither swap the ECDH keys nor downgrade the cipher suite.
        /// </summary>
        public static byte[] IdentityTranscript(long token, uint configHash, byte keyExchange, byte offeredSuites,
                                                CipherSuite chosenSuite, ReadOnlySpan<byte> clientPublicKey,
                                                ReadOnlySpan<byte> serverPublicKey) {
            var label = Encoding.ASCII.GetBytes("NetSync/3 server identity");
            var transcript = new byte[label.Length + 8 + 4 + 3 + 1 + clientPublicKey.Length + 1 + serverPublicKey.Length];
            int at = 0;
            label.CopyTo(transcript, at);
            at += label.Length;
            BinaryPrimitives.WriteInt64BigEndian(transcript.AsSpan(at), token);
            at += 8;
            BinaryPrimitives.WriteUInt32BigEndian(transcript.AsSpan(at), configHash);
            at += 4;
            transcript[at++] = keyExchange;
            transcript[at++] = offeredSuites;
            transcript[at++] = (byte)chosenSuite;
            transcript[at++] = (byte)clientPublicKey.Length;
            clientPublicKey.CopyTo(transcript.AsSpan(at));
            at += clientPublicKey.Length;
            transcript[at++] = (byte)serverPublicKey.Length;
            serverPublicKey.CopyTo(transcript.AsSpan(at));
            return transcript;
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
