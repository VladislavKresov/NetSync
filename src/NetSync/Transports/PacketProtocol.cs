using System;
using System.Buffers;
using System.Buffers.Binary;

namespace NetSync.Transports {
    internal enum PacketType : byte {
        Ping = 0x01,
        Pong = 0x02,
        Data = 0x10,
        Fragment = 0x11,
        Batch = 0x12,       // UDP only: [type][len][packet][len][packet]… — see DatagramBatcher
        // Connectionless UDP (no peer, no handshake) — see UdpServerTransport.HandleUnconnected.
        Query = 0x30,       // [type][id:4][requestLength:2][request][zero padding]
        QueryReply = 0x31,  // [type][id:4][response]            (never longer than the query)
        MtuProbe = 0x32,    // [type][nonce:4][padding up to the probed size]
        MtuReply = 0x33     // [type][nonce:4][receivedSize:2]
    }

    /// <summary>
    /// Wire format helpers.
    /// UDP datagram: [type:1][payload]
    /// TCP frame:    [type:1][length:4 BE][payload]   (Ping/Pong: [type:1][timestamp:8 BE])
    ///
    /// Frame builders write into <see cref="ArrayPool{T}"/> buffers: the hot send path
    /// allocates nothing. Every Rent* result must go back via <see cref="Return"/>.
    /// Rented arrays are usually longer than requested — always pair them with the
    /// out length, never use buffer.Length.
    /// </summary>
    internal static class PacketProtocol {
        public const int TcpHeaderSize = 5;      // type + length
        public const int PingPacketSize = 9;     // type + 8-byte timestamp

        /// <summary>
        /// Hard ceiling for a TCP frame body. Senders fragment everything at or above
        /// MaxUnfragmentedSize, so a legitimate frame is always smaller; anything
        /// bigger is a protocol violation and must not trigger an allocation.
        /// </summary>
        public const int MaxTcpFrameBody = Pipeline.Fragmentation.PacketFragmenter.MaxUnfragmentedSize;

        /// <summary>Before the handshake a peer may only send small control frames (Hello is &lt; 100 B).</summary>
        public const int MaxUntrustedFrameBody = 1024;

        private static readonly ArrayPool<byte> Pool = ArrayPool<byte>.Shared;

        public static void Return(byte[] buffer) => Pool.Return(buffer);

        public static byte[] RentPingPacket(long timestampMs) {
            var buf = Pool.Rent(PingPacketSize);
            buf[0] = (byte)PacketType.Ping;
            BinaryPrimitives.WriteInt64BigEndian(buf.AsSpan(1), timestampMs);
            return buf;
        }

        public static byte[] RentPongPacket(long echoedTimestampMs) {
            var buf = Pool.Rent(PingPacketSize);
            buf[0] = (byte)PacketType.Pong;
            BinaryPrimitives.WriteInt64BigEndian(buf.AsSpan(1), echoedTimestampMs);
            return buf;
        }

        public static long ReadTimestamp(ReadOnlySpan<byte> packet) {
            return BinaryPrimitives.ReadInt64BigEndian(packet.Slice(1, 8));
        }

        /// <summary>UDP: [Data][prefix][payload] in one rented buffer.</summary>
        public static byte[] RentUdpDataPacket(ReadOnlySpan<byte> prefix, ReadOnlySpan<byte> payload, out int length) {
            length = 1 + prefix.Length + payload.Length;
            var buf = Pool.Rent(length);
            buf[0] = (byte)PacketType.Data;
            prefix.CopyTo(buf.AsSpan(1));
            payload.CopyTo(buf.AsSpan(1 + prefix.Length));
            return buf;
        }

        /// <summary>TCP: [Data][length][prefix][payload] in one rented buffer.</summary>
        public static byte[] RentTcpDataFrame(ReadOnlySpan<byte> prefix, ReadOnlySpan<byte> payload, out int length) {
            int bodyLength = prefix.Length + payload.Length;
            length = TcpHeaderSize + bodyLength;
            var buf = Pool.Rent(length);
            buf[0] = (byte)PacketType.Data;
            BinaryPrimitives.WriteInt32BigEndian(buf.AsSpan(1, 4), bodyLength);
            prefix.CopyTo(buf.AsSpan(TcpHeaderSize));
            payload.CopyTo(buf.AsSpan(TcpHeaderSize + prefix.Length));
            return buf;
        }

        /// <summary>
        /// TCP frame whose body is a fragment: [Data][length][Fragment][seq][idx][total][chunk].
        /// </summary>
        public static byte[] RentTcpFragmentFrame(uint sequenceId, uint index, uint total, ReadOnlySpan<byte> chunk, out int length) {
            int bodyLength = Pipeline.Fragmentation.PacketFragmenter.HeaderSize + chunk.Length;
            length = TcpHeaderSize + bodyLength;
            var buf = Pool.Rent(length);
            buf[0] = (byte)PacketType.Data;
            BinaryPrimitives.WriteInt32BigEndian(buf.AsSpan(1, 4), bodyLength);
            Pipeline.Fragmentation.PacketFragmenter.WriteFragmentHeader(buf.AsSpan(TcpHeaderSize), sequenceId, index, total);
            chunk.CopyTo(buf.AsSpan(TcpHeaderSize + Pipeline.Fragmentation.PacketFragmenter.HeaderSize));
            return buf;
        }

        /// <summary>
        /// Fragment frame cut directly from the logical concatenation prefix‖payload —
        /// lets bulk sends fragment without first copying the whole message.
        /// </summary>
        public static byte[] RentTcpFragmentFrame(uint sequenceId, uint index, uint total,
                                                  ReadOnlySpan<byte> prefix, ReadOnlySpan<byte> payload,
                                                  int offset, int chunkLength, out int length) {
            int bodyLength = Pipeline.Fragmentation.PacketFragmenter.HeaderSize + chunkLength;
            length = TcpHeaderSize + bodyLength;
            var buf = Pool.Rent(length);
            buf[0] = (byte)PacketType.Data;
            BinaryPrimitives.WriteInt32BigEndian(buf.AsSpan(1, 4), bodyLength);
            Pipeline.Fragmentation.PacketFragmenter.WriteFragmentHeader(buf.AsSpan(TcpHeaderSize), sequenceId, index, total);
            CopyRange(prefix, payload, offset, buf.AsSpan(TcpHeaderSize + Pipeline.Fragmentation.PacketFragmenter.HeaderSize, chunkLength));
            return buf;
        }

        /// <summary>
        /// Message-framed fragment (WebSocket): [Data][Fragment][seq][idx][total][chunk], the chunk
        /// cut from prefix‖payload at <paramref name="offset"/>.
        /// </summary>
        public static byte[] RentMessageFragment(uint sequenceId, uint index, uint total,
                                                 ReadOnlySpan<byte> prefix, ReadOnlySpan<byte> payload,
                                                 int offset, int chunkLength, out int length) {
            length = 1 + Pipeline.Fragmentation.PacketFragmenter.HeaderSize + chunkLength;
            var buf = Pool.Rent(length);
            buf[0] = (byte)PacketType.Data;
            Pipeline.Fragmentation.PacketFragmenter.WriteFragmentHeader(buf.AsSpan(1), sequenceId, index, total);
            CopyRange(prefix, payload, offset, buf.AsSpan(1 + Pipeline.Fragmentation.PacketFragmenter.HeaderSize, chunkLength));
            return buf;
        }

        /// <summary>Copies dest.Length bytes starting at <paramref name="offset"/> of first‖second.</summary>
        public static void CopyRange(ReadOnlySpan<byte> first, ReadOnlySpan<byte> second, int offset, Span<byte> dest) {
            int written = 0;
            if (offset < first.Length) {
                int fromFirst = Math.Min(first.Length - offset, dest.Length);
                first.Slice(offset, fromFirst).CopyTo(dest);
                written = fromFirst;
            }
            if (written < dest.Length) {
                int secondOffset = offset + written - first.Length;
                second.Slice(secondOffset, dest.Length - written).CopyTo(dest.Slice(written));
            }
        }

        public static int ReadTcpFrameLength(ReadOnlySpan<byte> header) {
            return BinaryPrimitives.ReadInt32BigEndian(header.Slice(1, 4));
        }
    }
}
