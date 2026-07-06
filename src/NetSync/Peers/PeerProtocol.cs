using System;
using System.Buffers.Binary;

namespace NetSync.Peers {
    /// <summary>
    /// Peer-level messages, carried inside transport Data payloads.
    ///
    /// Hello   [0x01][version:1][sessionToken:8][cipher:1][pubKeyLen:1][pubKey…]
    ///          — client introduces itself on every link; the token groups multiple
    ///            transport links into one logical connection. The cipher/pubkey tail
    ///            is optional (absent = no encryption).
    /// Welcome [0x02][connectionId:8][pubKeyLen:1][pubKey…]
    ///          — server accepted the link; the pubkey tail is present in ECDH mode.
    /// Reject  [0x03][reason:1]                   — server refused.
    /// Bye     [0x04][reason:1]                   — graceful disconnect notice.
    /// Data    [0x10][channel:1][payload…]        — application data.
    /// </summary>
    internal static class PeerProtocol {
        public const byte ProtocolVersion = 1;

        public const byte MsgHello = 0x01;
        public const byte MsgWelcome = 0x02;
        public const byte MsgReject = 0x03;
        public const byte MsgBye = 0x04;
        public const byte MsgData = 0x10;
        /// <summary>Reliability-layer data: [0x20][flags:1][channel:1][seq:2 BE][payload…]</summary>
        public const byte MsgRelData = 0x20;
        /// <summary>Reliability-layer ack: [0x21][channel:1][anchorSeq:2 BE][mask:4 BE]</summary>
        public const byte MsgRelAck = 0x21;

        public const int DataHeaderSize = 2;

        public const byte RejectVersionMismatch = 1;
        public const byte RejectEncryptionMismatch = 2;
        public const byte ByeGraceful = 0;

        public static byte[] EncodeHello(long sessionToken, KeyExchangeMode cipher = KeyExchangeMode.None, byte[]? publicKey = null) {
            int keyLength = publicKey?.Length ?? 0;
            var buf = new byte[12 + keyLength];
            buf[0] = MsgHello;
            buf[1] = ProtocolVersion;
            BinaryPrimitives.WriteInt64BigEndian(buf.AsSpan(2), sessionToken);
            buf[10] = (byte)cipher;
            buf[11] = (byte)keyLength;
            publicKey?.CopyTo(buf.AsSpan(12));
            return buf;
        }

        public static byte[] EncodeWelcome(long connectionId, byte[]? publicKey = null) {
            int keyLength = publicKey?.Length ?? 0;
            var buf = new byte[10 + keyLength];
            buf[0] = MsgWelcome;
            BinaryPrimitives.WriteInt64BigEndian(buf.AsSpan(1), connectionId);
            buf[9] = (byte)keyLength;
            publicKey?.CopyTo(buf.AsSpan(10));
            return buf;
        }

        public static byte[] EncodeReject(byte reason) => new[] { MsgReject, reason };

        public static byte[] EncodeBye(byte reason) => new[] { MsgBye, reason };

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
