using System;
using System.Buffers;
using System.Buffers.Binary;

namespace NetSync.Transports {
    internal enum PacketType : byte {
        Ping = 0x01,
        Pong = 0x02,
        Data = 0x10,
        Fragment = 0x11
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

        public static int ReadTcpFrameLength(ReadOnlySpan<byte> header) {
            return BinaryPrimitives.ReadInt32BigEndian(header.Slice(1, 4));
        }
    }
}
