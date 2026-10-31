using System;
using System.Buffers.Binary;
using System.Text;

namespace NetSync.Discovery {
    /// <summary>
    /// LAN discovery datagram:
    /// v2 [magic "NSD2":4][serverPort:2 BE][appIdLen:1][appId utf8][payloadLen:2 BE][payload]
    /// v1 [magic "NSD1":4][serverPort:2 BE][appIdLen:1][appId utf8] (still understood)
    /// The appId keeps unrelated NetSync applications on the same network apart; the
    /// payload is the application's own description of the server (name, map, players…).
    /// </summary>
    internal static class DiscoveryMessage {
        private static readonly byte[] MagicV1 = { (byte)'N', (byte)'S', (byte)'D', (byte)'1' };
        private static readonly byte[] MagicV2 = { (byte)'N', (byte)'S', (byte)'D', (byte)'2' };
        public const int MaxAppIdBytes = 255;
        public const int MaxPayloadBytes = 1024;

        public static byte[] Encode(string appId, int serverPort, ReadOnlySpan<byte> payload = default) {
            var appIdBytes = Encoding.UTF8.GetBytes(appId);
            if (appIdBytes.Length > MaxAppIdBytes) {
                throw new ArgumentException($"appId must encode to at most {MaxAppIdBytes} UTF-8 bytes", nameof(appId));
            }
            if (serverPort <= 0 || serverPort > ushort.MaxValue) {
                throw new ArgumentOutOfRangeException(nameof(serverPort));
            }
            if (payload.Length > MaxPayloadBytes) {
                throw new ArgumentException($"A discovery payload is limited to {MaxPayloadBytes} bytes", nameof(payload));
            }

            var buf = new byte[4 + 2 + 1 + appIdBytes.Length + 2 + payload.Length];
            MagicV2.CopyTo(buf, 0);
            BinaryPrimitives.WriteUInt16BigEndian(buf.AsSpan(4, 2), (ushort)serverPort);
            buf[6] = (byte)appIdBytes.Length;
            appIdBytes.CopyTo(buf.AsSpan(7));
            int at = 7 + appIdBytes.Length;
            BinaryPrimitives.WriteUInt16BigEndian(buf.AsSpan(at, 2), (ushort)payload.Length);
            payload.CopyTo(buf.AsSpan(at + 2));
            return buf;
        }

        public static bool TryDecode(ReadOnlySpan<byte> datagram, out string appId, out int serverPort) {
            return TryDecode(datagram, out appId, out serverPort, out _);
        }

        public static bool TryDecode(ReadOnlySpan<byte> datagram, out string appId, out int serverPort, out byte[] payload) {
            appId = string.Empty;
            serverPort = 0;
            payload = Array.Empty<byte>();

            if (datagram.Length < 7 || datagram[0] != (byte)'N' || datagram[1] != (byte)'S' || datagram[2] != (byte)'D') {
                return false;
            }
            bool v2 = datagram[3] == MagicV2[3];
            if (!v2 && datagram[3] != MagicV1[3]) {
                return false;
            }
            int appIdLength = datagram[6];
            if (datagram.Length < 7 + appIdLength) {
                return false;
            }

            serverPort = BinaryPrimitives.ReadUInt16BigEndian(datagram.Slice(4, 2));
            if (serverPort == 0) {
                return false;
            }
            if (v2) {
                int at = 7 + appIdLength;
                if (datagram.Length < at + 2) {
                    return false;
                }
                int payloadLength = BinaryPrimitives.ReadUInt16BigEndian(datagram.Slice(at, 2));
                if (payloadLength > MaxPayloadBytes || datagram.Length < at + 2 + payloadLength) {
                    return false;
                }
                payload = datagram.Slice(at + 2, payloadLength).ToArray();
            }
#if NET8_0_OR_GREATER
            appId = Encoding.UTF8.GetString(datagram.Slice(7, appIdLength));
#else
            appId = Encoding.UTF8.GetString(datagram.Slice(7, appIdLength).ToArray());
#endif
            return true;
        }
    }
}
