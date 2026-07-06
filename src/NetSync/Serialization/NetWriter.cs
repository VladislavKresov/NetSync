using System;
using System.Buffers.Binary;
using System.Text;

namespace NetSync.Serialization {
    /// <summary>
    /// Compact binary writer for building message payloads — the replacement for the
    /// 1.x string-based JsonMessage. Little-endian fixed-width primitives, LEB128
    /// var-ints, length-prefixed UTF-8 strings. Reusable: call <see cref="Reset"/>
    /// and write the next message into the same buffer.
    /// </summary>
    public sealed class NetWriter {
        private byte[] _buffer;
        private int _position;

        public int Length => _position;

        /// <summary>The written bytes; valid until the next write or Reset.</summary>
        public ReadOnlyMemory<byte> WrittenMemory => _buffer.AsMemory(0, _position);

        public NetWriter(int initialCapacity = 256) {
            _buffer = new byte[Math.Max(16, initialCapacity)];
        }

        public void Reset() => _position = 0;

        public byte[] ToArray() => _buffer.AsSpan(0, _position).ToArray();

        private Span<byte> Reserve(int count) {
            if (_position + count > _buffer.Length) {
                int newSize = Math.Max(_buffer.Length * 2, _position + count);
                Array.Resize(ref _buffer, newSize);
            }
            var span = _buffer.AsSpan(_position, count);
            _position += count;
            return span;
        }

        public NetWriter WriteByte(byte value) {
            Reserve(1)[0] = value;
            return this;
        }

        public NetWriter WriteBool(bool value) => WriteByte(value ? (byte)1 : (byte)0);

        public NetWriter WriteInt16(short value) {
            BinaryPrimitives.WriteInt16LittleEndian(Reserve(2), value);
            return this;
        }

        public NetWriter WriteUInt16(ushort value) {
            BinaryPrimitives.WriteUInt16LittleEndian(Reserve(2), value);
            return this;
        }

        public NetWriter WriteInt32(int value) {
            BinaryPrimitives.WriteInt32LittleEndian(Reserve(4), value);
            return this;
        }

        public NetWriter WriteUInt32(uint value) {
            BinaryPrimitives.WriteUInt32LittleEndian(Reserve(4), value);
            return this;
        }

        public NetWriter WriteInt64(long value) {
            BinaryPrimitives.WriteInt64LittleEndian(Reserve(8), value);
            return this;
        }

        public NetWriter WriteUInt64(ulong value) {
            BinaryPrimitives.WriteUInt64LittleEndian(Reserve(8), value);
            return this;
        }

        public NetWriter WriteSingle(float value) => WriteInt32(BitConverter.SingleToInt32Bits(value));

        public NetWriter WriteDouble(double value) => WriteInt64(BitConverter.DoubleToInt64Bits(value));

        /// <summary>LEB128: small values take 1 byte instead of 4.</summary>
        public NetWriter WriteVarUInt(uint value) {
            while (value >= 0x80) {
                WriteByte((byte)(value | 0x80));
                value >>= 7;
            }
            return WriteByte((byte)value);
        }

        /// <summary>ZigZag + LEB128: small magnitudes (positive or negative) take 1 byte.</summary>
        public NetWriter WriteVarInt(int value) => WriteVarUInt((uint)((value << 1) ^ (value >> 31)));

        /// <summary>UTF-8, var-int length prefix. Null is written as empty.</summary>
        public NetWriter WriteString(string? value) {
            if (string.IsNullOrEmpty(value)) {
                return WriteVarUInt(0);
            }
            int byteCount = Encoding.UTF8.GetByteCount(value);
            WriteVarUInt((uint)byteCount);
            Encoding.UTF8.GetBytes(value, 0, value.Length, _buffer, ReserveOffset(byteCount));
            return this;
        }

        private int ReserveOffset(int count) {
            Reserve(count);
            return _position - count;
        }

        /// <summary>Var-int length prefix + raw bytes.</summary>
        public NetWriter WriteBytes(ReadOnlySpan<byte> value) {
            WriteVarUInt((uint)value.Length);
            value.CopyTo(Reserve(value.Length));
            return this;
        }

        /// <summary>Raw bytes, no length prefix — the reader must know the size.</summary>
        public NetWriter WriteRaw(ReadOnlySpan<byte> value) {
            value.CopyTo(Reserve(value.Length));
            return this;
        }

        public NetWriter WriteGuid(Guid value) {
            value.TryWriteBytes(Reserve(16));
            return this;
        }
    }
}
