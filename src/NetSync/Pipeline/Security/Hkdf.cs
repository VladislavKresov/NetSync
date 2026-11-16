using System;
using System.Security.Cryptography;

namespace NetSync.Pipeline.Security {
    /// <summary>
    /// HKDF with HMAC-SHA256 (RFC 5869). The class library has one only on newer targets;
    /// this is the same function everywhere.
    /// </summary>
    internal static class Hkdf {
        public const int HashSize = 32;

        /// <summary>Extract: a pseudorandom key from input key material and a salt (empty salt = zeros).</summary>
        public static byte[] Extract(ReadOnlySpan<byte> salt, ReadOnlySpan<byte> inputKeyMaterial) {
            using var hmac = new HMACSHA256(salt.Length == 0 ? new byte[HashSize] : salt.ToArray());
            return hmac.ComputeHash(inputKeyMaterial.ToArray());
        }

        /// <summary>Expand: <paramref name="length"/> bytes of output keyed by a pseudorandom key, bound to <paramref name="info"/>.</summary>
        public static byte[] Expand(byte[] pseudorandomKey, ReadOnlySpan<byte> info, int length) {
            if (length < 0 || length > 255 * HashSize) {
                throw new ArgumentOutOfRangeException(nameof(length));
            }
            var output = new byte[length];
            var block = Array.Empty<byte>();
            using var hmac = new HMACSHA256(pseudorandomKey);
            int written = 0;
            for (byte counter = 1; written < length; counter++) {
                var input = new byte[block.Length + info.Length + 1];
                block.CopyTo(input, 0);
                info.CopyTo(input.AsSpan(block.Length));
                input[input.Length - 1] = counter;
                block = hmac.ComputeHash(input);
                int take = Math.Min(HashSize, length - written);
                Array.Copy(block, 0, output, written, take);
                written += take;
            }
            return output;
        }

        public static byte[] DeriveKey(ReadOnlySpan<byte> inputKeyMaterial, ReadOnlySpan<byte> salt, ReadOnlySpan<byte> info, int length) {
            var pseudorandomKey = Extract(salt, inputKeyMaterial);
            try {
                return Expand(pseudorandomKey, info, length);
            }
            finally {
                Array.Clear(pseudorandomKey, 0, pseudorandomKey.Length);
            }
        }
    }
}
