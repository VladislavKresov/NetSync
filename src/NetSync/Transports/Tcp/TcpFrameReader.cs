using System;
using System.Buffers.Binary;

namespace NetSync.Transports.Tcp {
    /// <summary>
    /// Incremental TCP frame parser over a <see cref="ReceiveBuffer"/>. A single socket read
    /// usually brings in many small frames; they are parsed and delivered straight from the
    /// buffer — no per-frame reads, no per-frame copies — and the buffer only grows while a
    /// large frame is in flight.
    ///
    /// Usage: read into <see cref="GetReadMemory"/>, call <see cref="Advance"/>, then
    /// <see cref="TryRead"/> until it returns <see cref="FrameKind.None"/>. A Data frame's
    /// bytes stay valid until the next <see cref="GetReadMemory"/>.
    /// </summary>
    internal sealed class TcpFrameReader : IDisposable {
        public enum FrameKind {
            /// <summary>No complete frame buffered: read more.</summary>
            None,
            Ping,
            Pong,
            Data,
            /// <summary>Protocol violation (unknown type or length over the limit): drop the link.</summary>
            Invalid
        }

        private readonly ReceiveBuffer _buffer = new ReceiveBuffer();
        private int _pendingFrameSize;

        public byte[] Buffer => _buffer.Array;

        public Memory<byte> GetReadMemory() => _buffer.GetReadMemory(_pendingFrameSize);

        public void Advance(int bytesRead) => _buffer.Advance(bytesRead);

        /// <summary>
        /// Parses the next frame. Data: body at Buffer[offset..offset+length).
        /// Ping/Pong: <paramref name="timestamp"/>. Invalid: <paramref name="offset"/> holds
        /// the offending type byte or length, for the log.
        /// </summary>
        public FrameKind TryRead(int maxBody, out int offset, out int length, out long timestamp) {
            offset = 0;
            length = 0;
            timestamp = 0;
            int available = _buffer.Available;
            if (available < 1) {
                return FrameKind.None;
            }

            var bytes = _buffer.Array;
            int start = _buffer.Start;
            var type = (PacketType)bytes[start];
            switch (type) {
                case PacketType.Ping:
                case PacketType.Pong:
                    if (available < PacketProtocol.PingPacketSize) {
                        return FrameKind.None;
                    }
                    timestamp = BinaryPrimitives.ReadInt64BigEndian(bytes.AsSpan(start + 1, 8));
                    _buffer.Consume(PacketProtocol.PingPacketSize);
                    _pendingFrameSize = 0;
                    return type == PacketType.Ping ? FrameKind.Ping : FrameKind.Pong;

                case PacketType.Data:
                    if (available < PacketProtocol.TcpHeaderSize) {
                        return FrameKind.None;
                    }
                    int bodyLength = BinaryPrimitives.ReadInt32BigEndian(bytes.AsSpan(start + 1, 4));
                    if (bodyLength < 0 || bodyLength > maxBody) {
                        offset = bodyLength;
                        return FrameKind.Invalid;
                    }
                    if (available < PacketProtocol.TcpHeaderSize + bodyLength) {
                        _pendingFrameSize = PacketProtocol.TcpHeaderSize + bodyLength; // make room for it
                        return FrameKind.None;
                    }
                    offset = start + PacketProtocol.TcpHeaderSize;
                    length = bodyLength;
                    _buffer.Consume(PacketProtocol.TcpHeaderSize + bodyLength);
                    _pendingFrameSize = 0;
                    return FrameKind.Data;

                default:
                    offset = (int)type;
                    return FrameKind.Invalid;
            }
        }

        public void Dispose() => _buffer.Dispose();
    }
}
