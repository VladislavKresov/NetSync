using System;
using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using System.Text;

namespace NetSync.Relay {
    /// <summary>
    /// Wire format between relay clients/hosts and <see cref="NetRelayServer"/>. The relay
    /// never looks inside the NetSync packets it carries ("message" below) — with a key
    /// exchange configured they are authenticated envelopes it can't read or forge.
    ///
    /// TCP frames: [kind:1][bodyLength:4 BE][body]
    ///   HostHello     0x01 [roomLen:1][room]          host claims a room
    ///   HostWelcome   0x02 [secret:16]                room is yours; the secret authorizes your UDP side
    ///   ClientHello   0x03 [roomLen:1][room]          client joins a room's stream side
    ///   ClientWelcome 0x04 []
    ///   Refused       0x05 [reason utf8]
    ///   Data          0x10 [message]                  client ⇄ relay
    ///   PeerData      0x11 [clientId:4][message]      relay ⇄ host
    ///   PeerOpen      0x12 [clientId:4][endpoint]     relay → host: a stream client arrived
    ///   PeerClose     0x13 [clientId:4]               either way: that client is gone / kick it
    ///   Keepalive     0x14 []
    ///
    /// UDP datagrams: [kind:1][...]
    ///   HostRegister     0x80 [roomLen][room][secret:16]     host → relay (repeated: keeps NAT mappings)
    ///   HostRegistered   0x81 []
    ///   ClientRegister   0x82 [roomLen][room][nonce:8]       client → relay
    ///   ClientRegistered 0x83 [clientId:4][hostEndpoint]     relay → client (endpoint empty: no direct path)
    ///   Introduce        0x84 [clientId:4][nonce:8][endpoint] relay → host: punch towards this client
    ///   RelayData        0x85 client→relay [clientId:4][nonce:8][message]; relay→host [clientId:4][message];
    ///                         host→relay [clientId:4][message]; relay→client [message]
    ///   Punch            0x86 [clientId:4][nonce:8]           client ⇄ host, directly
    ///   DirectData       0x87 client→host [clientId:4][nonce:8][message]; host→client [message]
    ///
    /// Endpoint: [family:1 (4|6)][address 4|16][port:2 BE]; family 0 = none.
    /// </summary>
    internal static class RelayProtocol {
        public const byte HostHello = 0x01;
        public const byte HostWelcome = 0x02;
        public const byte ClientHello = 0x03;
        public const byte ClientWelcome = 0x04;
        public const byte Refused = 0x05;
        public const byte Data = 0x10;
        public const byte PeerData = 0x11;
        public const byte PeerOpen = 0x12;
        public const byte PeerClose = 0x13;
        public const byte Keepalive = 0x14;

        public const byte HostRegister = 0x80;
        public const byte HostRegistered = 0x81;
        public const byte ClientRegister = 0x82;
        public const byte ClientRegistered = 0x83;
        public const byte Introduce = 0x84;
        public const byte RelayData = 0x85;
        public const byte Punch = 0x86;
        public const byte DirectData = 0x87;

        public const int FrameHeaderSize = 5;
        public const int SecretSize = 16;
        public const int NonceSize = 8;
        public const int MaxRoomBytes = 64;
        /// <summary>Largest frame body: a full NetSync message plus the client id.</summary>
        public const int MaxFrameBody = Transports.MessageLink.MaxMessage + 16;

        public static byte[] EncodeRoom(string room) {
            var bytes = Encoding.UTF8.GetBytes(room ?? string.Empty);
            if (bytes.Length == 0 || bytes.Length > MaxRoomBytes) {
                throw new ArgumentException($"A room id is 1..{MaxRoomBytes} UTF-8 bytes", nameof(room));
            }
            var result = new byte[1 + bytes.Length];
            result[0] = (byte)bytes.Length;
            bytes.CopyTo(result, 1);
            return result;
        }

        public static bool TryReadRoom(ReadOnlySpan<byte> source, out string room, out int consumed) {
            room = string.Empty;
            consumed = 0;
            if (source.Length < 1 || source[0] == 0 || source[0] > MaxRoomBytes || source.Length < 1 + source[0]) {
                return false;
            }
            room = Encoding.UTF8.GetString(source.Slice(1, source[0]).ToArray());
            consumed = 1 + source[0];
            return true;
        }

        public static int EndPointSize(IPEndPoint? endPoint) {
            if (endPoint == null) {
                return 1;
            }
            return endPoint.AddressFamily == AddressFamily.InterNetworkV6 ? 1 + 16 + 2 : 1 + 4 + 2;
        }

        public static int WriteEndPoint(Span<byte> destination, IPEndPoint? endPoint) {
            if (endPoint == null) {
                destination[0] = 0;
                return 1;
            }
            var address = endPoint.Address.GetAddressBytes();
            destination[0] = (byte)(address.Length == 16 ? 6 : 4);
            address.CopyTo(destination.Slice(1));
            BinaryPrimitives.WriteUInt16BigEndian(destination.Slice(1 + address.Length), (ushort)endPoint.Port);
            return 1 + address.Length + 2;
        }

        public static bool TryReadEndPoint(ReadOnlySpan<byte> source, out IPEndPoint? endPoint, out int consumed) {
            endPoint = null;
            consumed = 0;
            if (source.Length < 1) {
                return false;
            }
            int addressLength = source[0] switch { 0 => 0, 4 => 4, 6 => 16, _ => -1 };
            if (addressLength < 0) {
                return false;
            }
            if (addressLength == 0) {
                consumed = 1;
                return true;
            }
            if (source.Length < 1 + addressLength + 2) {
                return false;
            }
            endPoint = new IPEndPoint(new IPAddress(source.Slice(1, addressLength).ToArray()),
                                      BinaryPrimitives.ReadUInt16BigEndian(source.Slice(1 + addressLength)));
            consumed = 1 + addressLength + 2;
            return true;
        }

        /// <summary>[kind][length][body] in a new array (control frames; data frames are built pooled).</summary>
        public static byte[] Frame(byte kind, ReadOnlySpan<byte> body) {
            var frame = new byte[FrameHeaderSize + body.Length];
            frame[0] = kind;
            BinaryPrimitives.WriteInt32BigEndian(frame.AsSpan(1), body.Length);
            body.CopyTo(frame.AsSpan(FrameHeaderSize));
            return frame;
        }
    }
}
