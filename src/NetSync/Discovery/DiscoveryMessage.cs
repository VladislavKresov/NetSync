using System;
using System.Buffers.Binary;
using System.Text;

namespace NetSync.Discovery {
    /// <summary>
    /// LAN discovery datagram: [magic "NSD1":4][serverPort:2 BE][appIdLen:1][appId utf8].
    /// The appId keeps unrelated NetSync applications on the same network apart.
    /// </summary>
    internal static class DiscoveryMessage {
        private static readonly byte[] Magic = { (byte)'N', (byte)'S', (byte)'D', (byte)'1' };
        public const int MaxAppIdBytes = 255;

        public static byte[] Encode(string appId, int serverPort) {
            var appIdBytes = Encoding.UTF8.GetBytes(appId);
            if (appIdBytes.Length > MaxAppIdBytes) {
                throw new ArgumentException($"appId must encode to at most {MaxAppIdBytes} UTF-8 bytes", nameof(appId));
            }
            if (serverPort <= 0 || serverPort > ushort.MaxValue) {
                throw new ArgumentOutOfRangeException(nameof(serverPort));
            }

            var buf = new byte[4 + 2 + 1 + appIdBytes.Length];
            Magic.CopyTo(buf, 0);
            BinaryPrimitives.WriteUInt16BigEndian(buf.AsSpan(4, 2), (ushort)serverPort);
            buf[6] = (byte)appIdBytes.Length;
            appIdBytes.CopyTo(buf.AsSpan(7));
            return buf;
        }

        public static bool TryDecode(ReadOnlySpan<byte> datagram, out string appId, out int serverPort) {
            appId = string.Empty;
            serverPort = 0;

            if (datagram.Length < 7 ||
                datagram[0] != Magic[0] || datagram[1] != Magic[1] ||
                datagram[2] != Magic[2] || datagram[3] != Magic[3]) {
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
#if NET8_0_OR_GREATER
            appId = Encoding.UTF8.GetString(datagram.Slice(7, appIdLength));
#else
            appId = Encoding.UTF8.GetString(datagram.Slice(7, appIdLength).ToArray());
#endif
            return true;
        }
    }
}
