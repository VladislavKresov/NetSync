using System;
using System.Buffers.Binary;
using System.IO;
using System.IO.Compression;
using NetSync.Diagnostics;
using NetSync.Internal;

namespace NetSync.Pipeline.Compression {
    /// <summary>
    /// Deflate payload compression. Format: [originalLength:4 BE][deflate stream].
    /// The explicit length lets the receiver allocate exactly once and enforce a
    /// decompression-bomb cap.
    ///
    /// Both directions work in pooled buffers; what still allocates is the Deflate
    /// stream itself (its native state) — compression is meant for bulk data.
    /// </summary>
    internal static class Compressor {
        private const int HeaderSize = 4;

        /// <summary>
        /// Compresses into a pooled buffer. Returns null when the result would not be smaller
        /// than the input (send it uncompressed); otherwise the caller returns the buffer.
        /// </summary>
        public static byte[]? TryCompress(ReadOnlySpan<byte> data, out int length) {
            length = 0;
            var buffer = Pooled.Rent(data.Length);
            try {
                // A fixed-size stream over the pooled buffer: output that doesn't fit isn't worth having.
                using var output = new MemoryStream(buffer, 0, data.Length, writable: true);
                Span<byte> header = stackalloc byte[HeaderSize];
                BinaryPrimitives.WriteInt32BigEndian(header, data.Length);
                output.Write(header);
                using (var deflate = new DeflateStream(output, CompressionLevel.Fastest, leaveOpen: true)) {
                    deflate.Write(data);
                }
                length = (int)output.Position;
                if (length < data.Length) {
                    return buffer;
                }
            }
            catch (NotSupportedException) {
                // the fixed-size stream is full: incompressible
            }
            Pooled.Return(buffer);
            return null;
        }

        /// <summary>
        /// Decompresses buffer[offset..offset+count) into a pooled buffer (<paramref name="length"/>
        /// bytes; the caller returns it). Null when the blob is malformed, truncated, or claims
        /// more than <paramref name="maxBytes"/> (decompression-bomb guard).
        /// </summary>
        public static byte[]? Decompress(byte[] buffer, int offset, int count, int maxBytes, INetLogger logger, out int length) {
            length = 0;
            if (count < HeaderSize) {
                return null;
            }
            int originalLength = BinaryPrimitives.ReadInt32BigEndian(buffer.AsSpan(offset, HeaderSize));
            if (originalLength < 0 || originalLength > maxBytes) {
                logger.Warning($"Dropping packet: declared decompressed size {originalLength} is out of range");
                return null;
            }

            var result = Pooled.Rent(originalLength);
            try {
                using var input = new MemoryStream(buffer, offset + HeaderSize, count - HeaderSize, writable: false);
                using var deflate = new DeflateStream(input, CompressionMode.Decompress);
                int total = 0;
                while (total < originalLength) {
                    int read = deflate.Read(result, total, originalLength - total);
                    if (read == 0) {
                        break; // truncated stream
                    }
                    total += read;
                }
                // Short, or anything left in the stream: the declared length lied.
                if (total == originalLength && deflate.ReadByte() == -1) {
                    length = originalLength;
                    return result;
                }
            }
            catch (InvalidDataException) {
                // malformed stream
            }
            Pooled.Return(result);
            return null;
        }
    }
}
