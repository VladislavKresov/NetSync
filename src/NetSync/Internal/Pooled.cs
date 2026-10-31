using System;
using System.Buffers;

namespace NetSync.Internal {
    /// <summary>
    /// The one buffer pool of the library. Rented arrays are usually longer than asked
    /// for — always carry the length alongside, never use array.Length.
    /// </summary>
    internal static class Pooled {
        private static readonly ArrayPool<byte> Pool = ArrayPool<byte>.Shared;

        public static byte[] Rent(int minimumLength) => Pool.Rent(Math.Max(1, minimumLength));

        public static void Return(byte[] buffer) => Pool.Return(buffer);

        /// <summary>Rented copy of <paramref name="source"/>; the caller returns it.</summary>
        public static byte[] Copy(ReadOnlySpan<byte> source) {
            var buffer = Pool.Rent(Math.Max(1, source.Length));
            source.CopyTo(buffer);
            return buffer;
        }

        /// <summary>Exact-size array (for APIs that hand ownership to user code).</summary>
        public static byte[] ToArray(byte[] buffer, int offset, int count) {
            if (count == 0) {
                return Array.Empty<byte>();
            }
            var copy = new byte[count];
            Buffer.BlockCopy(buffer, offset, copy, 0, count);
            return copy;
        }
    }
}
