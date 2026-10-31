using System;
using System.Buffers.Binary;
using System.IO;
using System.Text;

namespace NetSync.Serialization {
    /// <summary>
    /// Counterpart of <see cref="NetWriter"/>: reads primitives from a received payload.
    /// Throws <see cref="EndOfStreamException"/> on malformed/truncated input — always
    /// wrap parsing of untrusted data in a try/catch.
    /// </summary>
    public sealed class NetReader {
        private ReadOnlyMemory<byte> _data;
        private int _position;

        public int Position => _position;
        public int Remaining => _data.Length - _position;

        public NetReader(ReadOnlyMemory<byte> data) {
            _data = data;
        }

        public NetReader(byte[] data) : this(new ReadOnlyMemory<byte>(data)) { }

        /// <summary>Points this reader at new data (lets hot paths reuse one instance).</summary>
        internal void Reset(ReadOnlyMemory<byte> data) {
            _data = data;
            _position = 0;
        }

        private ReadOnlySpan<byte> Consume(int count) {
            if (count < 0 || count > _data.Length - _position) { // no overflow for huge counts
                throw new EndOfStreamException($"Tried to read {count} bytes with {Remaining} remaining");
            }
            var span = _data.Span.Slice(_position, count);
            _position += count;
            return span;
        }

        public byte ReadByte() => Consume(1)[0];

        public bool ReadBool() => ReadByte() != 0;

        public short ReadInt16() => BinaryPrimitives.ReadInt16LittleEndian(Consume(2));

        public ushort ReadUInt16() => BinaryPrimitives.ReadUInt16LittleEndian(Consume(2));

        public int ReadInt32() => BinaryPrimitives.ReadInt32LittleEndian(Consume(4));

        public uint ReadUInt32() => BinaryPrimitives.ReadUInt32LittleEndian(Consume(4));

        public long ReadInt64() => BinaryPrimitives.ReadInt64LittleEndian(Consume(8));

        public ulong ReadUInt64() => BinaryPrimitives.ReadUInt64LittleEndian(Consume(8));

        public float ReadSingle() => BitConverter.Int32BitsToSingle(ReadInt32());

        public double ReadDouble() => BitConverter.Int64BitsToDouble(ReadInt64());

        public uint ReadVarUInt() {
            uint result = 0;
            int shift = 0;
            while (true) {
                if (shift > 28) {
                    throw new EndOfStreamException("Malformed var-int");
                }
                byte b = ReadByte();
                result |= (uint)(b & 0x7F) << shift;
                if ((b & 0x80) == 0) {
                    return result;
                }
                shift += 7;
            }
        }

        public int ReadVarInt() {
            uint encoded = ReadVarUInt();
            return (int)(encoded >> 1) ^ -(int)(encoded & 1);
        }

        /// <summary>
        /// A length prefix from untrusted data: anything beyond what is left — including values that
        /// don't fit an int — is malformed input (EndOfStreamException), never an overflow.
        /// </summary>
        private int ReadLength() {
            uint length = ReadVarUInt();
            if (length > (uint)Remaining) {
                throw new EndOfStreamException($"Length prefix {length} exceeds the {Remaining} bytes remaining");
            }
            return (int)length;
        }

        public string ReadString() {
            int byteCount = ReadLength();
            if (byteCount == 0) {
                return string.Empty;
            }
            var bytes = Consume(byteCount);
#if NET8_0_OR_GREATER
            return Encoding.UTF8.GetString(bytes);
#else
            return Encoding.UTF8.GetString(bytes.ToArray());
#endif
        }

        public byte[] ReadBytes() {
            return Consume(ReadLength()).ToArray();
        }

        /// <summary>Zero-copy variant: a slice of the underlying payload.</summary>
        public ReadOnlyMemory<byte> ReadBytesMemory() {
            int count = ReadLength();
            var memory = _data.Slice(_position, count);
            _position += count;
            return memory;
        }

        /// <summary>Raw bytes without a length prefix.</summary>
        public byte[] ReadRaw(int count) => Consume(count).ToArray();

        public Guid ReadGuid() {
#if NET8_0_OR_GREATER
            return new Guid(Consume(16));
#else
            return new Guid(Consume(16).ToArray());
#endif
        }
    }
}
