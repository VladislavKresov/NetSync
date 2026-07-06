using System;
using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;

namespace NetSync.Pipeline.Security {
    /// <summary>
    /// Per-connection symmetric cipher: AES-256-CBC with HMAC-SHA256, encrypt-then-MAC.
    /// Deliberately the same construction on every target framework so a
    /// netstandard2.1 (Unity) peer talks to a net8.0 peer over the wire
    /// (AesGcm is unavailable on netstandard2.1).
    ///
    /// Message layout: [iv:16][mac:32][ciphertext:n×16], MAC over iv‖ciphertext.
    /// </summary>
    internal sealed class ConnectionCrypto : IDisposable {
        private const int IvSize = 16;
        private const int MacSize = 32;

        private readonly byte[] _encKey;
        private readonly byte[] _macKey;
        private readonly Aes _aes;
        private readonly object _lock = new object();
        private bool _disposed;

        private ConnectionCrypto(byte[] encKey, byte[] macKey) {
            _encKey = encKey;
            _macKey = macKey;
            _aes = Aes.Create();
            _aes.Key = encKey;
            _aes.Mode = CipherMode.CBC;
            _aes.Padding = PaddingMode.PKCS7;
        }

        /// <summary>Derives independent encryption and MAC keys from a session secret.</summary>
        public static ConnectionCrypto FromSecret(byte[] sessionSecret) {
            return new ConnectionCrypto(
                DeriveKey(sessionSecret, "NetSync/1 enc"),
                DeriveKey(sessionSecret, "NetSync/1 mac"));
        }

        /// <summary>
        /// PSK mode: session secret = HMAC(psk, label ‖ sessionToken). The token is
        /// public, the PSK is not — an eavesdropper cannot derive the session keys.
        /// </summary>
        public static ConnectionCrypto FromPresharedKey(byte[] presharedKey, long sessionToken) {
            var salt = new byte[8];
            BinaryPrimitives.WriteInt64BigEndian(salt, sessionToken);
            using var hmac = new HMACSHA256(presharedKey);
            var label = Encoding.ASCII.GetBytes("NetSync/1 session");
            hmac.TransformBlock(label, 0, label.Length, null, 0);
            hmac.TransformFinalBlock(salt, 0, salt.Length);
            return FromSecret(hmac.Hash!);
        }

        private static byte[] DeriveKey(byte[] secret, string label) {
            using var hmac = new HMACSHA256(secret);
            return hmac.ComputeHash(Encoding.ASCII.GetBytes(label));
        }

        public byte[] Encrypt(ReadOnlySpan<byte> plaintext) {
            byte[] plainArray = plaintext.ToArray();
            lock (_lock) {
                if (_disposed) {
                    throw new ObjectDisposedException(nameof(ConnectionCrypto));
                }
                _aes.GenerateIV();
                byte[] iv = _aes.IV;
                byte[] ciphertext;
                using (var encryptor = _aes.CreateEncryptor()) {
                    ciphertext = encryptor.TransformFinalBlock(plainArray, 0, plainArray.Length);
                }

                var result = new byte[IvSize + MacSize + ciphertext.Length];
                iv.CopyTo(result, 0);
                ciphertext.CopyTo(result.AsSpan(IvSize + MacSize));
                ComputeMac(result, ciphertext.Length).CopyTo(result.AsSpan(IvSize, MacSize));
                return result;
            }
        }

        /// <summary>Returns null when the message is malformed or fails authentication.</summary>
        public byte[]? Decrypt(ReadOnlySpan<byte> message) {
            // Minimum: iv + mac + one AES block (PKCS7 always pads at least one byte).
            if (message.Length < IvSize + MacSize + 16 || (message.Length - IvSize - MacSize) % 16 != 0) {
                return null;
            }

            byte[] messageArray = message.ToArray();
            lock (_lock) {
                if (_disposed) {
                    return null;
                }
                int ciphertextLength = messageArray.Length - IvSize - MacSize;
                var expectedMac = ComputeMac(messageArray, ciphertextLength);
                if (!FixedTimeEquals(expectedMac, messageArray.AsSpan(IvSize, MacSize))) {
                    return null; // tampered or wrong key
                }

                try {
                    var iv = new byte[IvSize];
                    Array.Copy(messageArray, 0, iv, 0, IvSize);
                    using var decryptor = _aes.CreateDecryptor(_encKey, iv);
                    return decryptor.TransformFinalBlock(messageArray, IvSize + MacSize, ciphertextLength);
                }
                catch (CryptographicException) {
                    return null; // bad padding — should be unreachable after MAC check
                }
            }
        }

        /// <summary>MAC over iv ‖ ciphertext of a [iv][mac][ciphertext] message.</summary>
        private byte[] ComputeMac(byte[] message, int ciphertextLength) {
            using var hmac = new HMACSHA256(_macKey);
            hmac.TransformBlock(message, 0, IvSize, null, 0);
            hmac.TransformFinalBlock(message, IvSize + MacSize, ciphertextLength);
            return hmac.Hash!;
        }

        // Constant-time comparison; implemented manually so it exists on every target.
        private static bool FixedTimeEquals(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b) {
            if (a.Length != b.Length) {
                return false;
            }
            int diff = 0;
            for (int i = 0; i < a.Length; i++) {
                diff |= a[i] ^ b[i];
            }
            return diff == 0;
        }

        public void Dispose() {
            lock (_lock) {
                if (_disposed) {
                    return;
                }
                _disposed = true;
                _aes.Dispose();
                Array.Clear(_encKey, 0, _encKey.Length);
                Array.Clear(_macKey, 0, _macKey.Length);
            }
        }
    }
}
