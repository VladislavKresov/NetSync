using System;
using System.Buffers.Binary;
using System.Threading;

namespace NetSync.Pipeline.Fragmentation {
    /// <summary>
    /// Splits large payloads into fragments and back.
    /// Fragment wire format: [Fragment:1][seqId:4 BE][index:4 BE][total:4 BE][data]
    /// </summary>
    public static class PacketFragmenter {
        /// <summary>Safe UDP fragment payload size: fits a typical MTU with headers to spare.</summary>
        public const int FragmentSizeUdp = 1200;

        /// <summary>TCP fragment payload size: keeps big transfers from starving small packets.</summary>
        public const int FragmentSizeTcp = 16 * 1024;

        /// <summary>Payloads at or above this size get fragmented.</summary>
        public const int MaxUnfragmentedSize = 64 * 1024;

        public const int HeaderSize = 13;

        private static int _sequenceIdCounter;

        public static uint NextSequenceId() {
            return unchecked((uint)Interlocked.Increment(ref _sequenceIdCounter));
        }

        public static byte[][] Fragment(ReadOnlySpan<byte> data, uint sequenceId, int fragmentSize) {
            if (fragmentSize <= 0) {
                throw new ArgumentOutOfRangeException(nameof(fragmentSize));
            }
            if (data.Length == 0) {
                return new[] { WrapFragment(sequenceId, 0, 1, ReadOnlySpan<byte>.Empty) };
            }

            int totalFragments = (data.Length + fragmentSize - 1) / fragmentSize;
            var fragments = new byte[totalFragments][];
            for (int i = 0; i < totalFragments; i++) {
                int offset = i * fragmentSize;
                int length = Math.Min(fragmentSize, data.Length - offset);
                fragments[i] = WrapFragment(sequenceId, (uint)i, (uint)totalFragments, data.Slice(offset, length));
            }
            return fragments;
        }

        private static byte[] WrapFragment(uint sequenceId, uint index, uint total, ReadOnlySpan<byte> data) {
            var buf = new byte[HeaderSize + data.Length];
            WriteFragmentHeader(buf, sequenceId, index, total);
            data.CopyTo(buf.AsSpan(HeaderSize));
            return buf;
        }

        /// <summary>Writes the 13-byte fragment header into <paramref name="dest"/> (no allocation).</summary>
        public static void WriteFragmentHeader(Span<byte> dest, uint sequenceId, uint index, uint total) {
            dest[0] = (byte)Transports.PacketType.Fragment;
            BinaryPrimitives.WriteUInt32BigEndian(dest.Slice(1, 4), sequenceId);
            BinaryPrimitives.WriteUInt32BigEndian(dest.Slice(5, 4), index);
            BinaryPrimitives.WriteUInt32BigEndian(dest.Slice(9, 4), total);
        }

        /// <summary>
        /// Returns true when <paramref name="packet"/> is a fragment; outputs its header
        /// fields and payload. Payload is a copy (fragments outlive the receive buffer).
        /// </summary>
        public static bool TryUnwrap(ReadOnlySpan<byte> packet, out uint sequenceId, out uint index, out uint total, out byte[] data) {
            sequenceId = 0;
            index = 0;
            total = 0;
            data = Array.Empty<byte>();

            if (packet.Length < HeaderSize || packet[0] != (byte)Transports.PacketType.Fragment) {
                return false;
            }

            sequenceId = BinaryPrimitives.ReadUInt32BigEndian(packet.Slice(1, 4));
            index = BinaryPrimitives.ReadUInt32BigEndian(packet.Slice(5, 4));
            total = BinaryPrimitives.ReadUInt32BigEndian(packet.Slice(9, 4));

            if (total == 0 || index >= total) {
                return false;
            }

            data = packet.Slice(HeaderSize).ToArray();
            return true;
        }
    }
}
