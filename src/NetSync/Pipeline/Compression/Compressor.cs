using System;
using System.Buffers.Binary;
using System.IO;
using System.IO.Compression;
using NetSync.Diagnostics;

namespace NetSync.Pipeline.Compression {
    /// <summary>
    /// Deflate payload compression. Format: [originalLength:4 BE][deflate stream].
    /// The explicit length lets the receiver allocate exactly once and enforce a
    /// decompression-bomb cap.
    /// </summary>
    internal static class Compressor {
        /// <summary>Refuse to inflate anything claiming to be larger than this.</summary>
        public const int MaxDecompressedBytes = 256 * 1024 * 1024;

        public static byte[] Compress(ReadOnlySpan<byte> data) {
            using var output = new MemoryStream(data.Length / 2 + 16);
            Span<byte> header = stackalloc byte[4];
            BinaryPrimitives.WriteInt32BigEndian(header, data.Length);
            output.Write(header);
            using (var deflate = new DeflateStream(output, CompressionLevel.Fastest, leaveOpen: true)) {
                deflate.Write(data);
            }
            return output.ToArray();
        }

        /// <summary>Returns null when the blob is malformed, oversized or truncated.</summary>
        public static byte[]? Decompress(ReadOnlySpan<byte> blob, INetLogger logger) {
            if (blob.Length < 4) {
                return null;
            }
            int originalLength = BinaryPrimitives.ReadInt32BigEndian(blob.Slice(0, 4));
            if (originalLength < 0 || originalLength > MaxDecompressedBytes) {
                logger.Warning($"Dropping packet: declared decompressed size {originalLength} is out of range");
                return null;
            }

            try {
                using var input = new MemoryStream(blob.Slice(4).ToArray());
                using var deflate = new DeflateStream(input, CompressionMode.Decompress);
                var result = new byte[originalLength];
                int total = 0;
                while (total < originalLength) {
                    int read = deflate.Read(result, total, originalLength - total);
                    if (read == 0) {
                        return null; // truncated stream
                    }
                    total += read;
                }
                // Anything left in the stream means the declared length lied.
                return deflate.ReadByte() == -1 ? result : null;
            }
            catch (InvalidDataException) {
                return null;
            }
        }
    }
}
