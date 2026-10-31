using System;
using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;
using System.Threading;

namespace NetSync.Pipeline.Security {
    /// <summary>
    /// Per-connection key material and the payload cipher, for one side (client or
    /// server) of one connection.
    ///
    /// Every key is per direction (client→server, server→client): a peer never encrypts
    /// or authenticates with the key it verifies with, so its own packets can't be
    /// reflected back to it, and GCM nonces can't collide between directions.
    ///
    /// Payload encryption only provides confidentiality on top of an already
    /// authenticated packet: every post-handshake packet travels in a
    /// <see cref="LinkSecurity"/> envelope whose tag covers the ciphertext. Hence:
    /// - AesCbcHmacSha256: [iv:16][ciphertext] — no second MAC (the envelope's HMAC is it).
    /// - AesGcm: [nonce:12][tag:16][ciphertext] — GCM's own tag comes for free.
    /// Both are allocation-free on net8.0; netstandard2.1 runs CBC through the classic
    /// transform API, which allocates per message.
    /// </summary>
    internal sealed class ConnectionCrypto : IDisposable {
        private const int CbcIvSize = 16;
        private const int AesBlock = 16;
        private const int GcmNonceSize = 12;
        private const int GcmTagSize = 16;

        public CipherSuite Suite { get; }

        /// <summary>Largest number of bytes <see cref="Encrypt"/> adds to a plaintext.</summary>
        public int MaxOverhead => Suite == CipherSuite.AesGcm ? GcmNonceSize + GcmTagSize : CbcIvSize + AesBlock;

        private readonly byte[] _sendKey;
        private readonly byte[] _receiveKey;
        private readonly byte[] _authSendKey;
        private readonly byte[] _authReceiveKey;
        private readonly object _sendLock = new object();
        private readonly object _receiveLock = new object();
        private readonly Aes? _sendAes;
        private readonly Aes? _receiveAes;
#if NET8_0_OR_GREATER
        private readonly AesGcm? _sendGcm;
        private readonly AesGcm? _receiveGcm;
        private long _sendCounter; // GCM nonces
#endif
        private bool _disposed;

        private ConnectionCrypto(byte[] sessionSecret, CipherSuite suite, bool isClient) {
            if (suite == CipherSuite.None) {
                throw new ArgumentException("An encrypted connection needs a cipher suite", nameof(suite));
            }
            Suite = suite;
            // The suite is part of every label: peers that disagree on it get unrelated keys
            // and fail at the first packet instead of misinterpreting each other.
            var c2s = DeriveKey(sessionSecret, $"NetSync/3 enc c2s {(byte)suite}");
            var s2c = DeriveKey(sessionSecret, $"NetSync/3 enc s2c {(byte)suite}");
            var authC2s = DeriveKey(sessionSecret, $"NetSync/3 auth c2s {(byte)suite}");
            var authS2c = DeriveKey(sessionSecret, $"NetSync/3 auth s2c {(byte)suite}");
            _sendKey = isClient ? c2s : s2c;
            _receiveKey = isClient ? s2c : c2s;
            _authSendKey = isClient ? authC2s : authS2c;
            _authReceiveKey = isClient ? authS2c : authC2s;

            if (suite == CipherSuite.AesGcm) {
#if NET8_0_OR_GREATER
                _sendGcm = new AesGcm(_sendKey, GcmTagSize);
                _receiveGcm = new AesGcm(_receiveKey, GcmTagSize);
#else
                throw new PlatformNotSupportedException("AES-GCM is not available on this target");
#endif
            }
            else {
                _sendAes = CreateCbc(_sendKey);
                _receiveAes = CreateCbc(_receiveKey);
            }
        }

        private static Aes CreateCbc(byte[] key) {
            var aes = Aes.Create();
            aes.Key = key;
            aes.Mode = CipherMode.CBC;
            aes.Padding = PaddingMode.PKCS7;
            return aes;
        }

        /// <summary>Derives every per-direction key from a session secret.</summary>
        public static ConnectionCrypto FromSecret(byte[] sessionSecret, CipherSuite suite, bool isClient) {
            return new ConnectionCrypto(sessionSecret, suite, isClient);
        }

        /// <summary>
        /// PSK mode: session secret = HMAC(psk, label ‖ sessionToken). The token is
        /// public, the PSK is not — an eavesdropper cannot derive the session keys.
        /// </summary>
        public static ConnectionCrypto FromPresharedKey(byte[] presharedKey, long sessionToken, CipherSuite suite, bool isClient) {
            var secret = PresharedSecret(presharedKey, sessionToken);
            try {
                return FromSecret(secret, suite, isClient);
            }
            finally {
                Array.Clear(secret, 0, secret.Length);
            }
        }

        private static byte[] PresharedSecret(byte[] presharedKey, long sessionToken) {
            var salt = new byte[8];
            BinaryPrimitives.WriteInt64BigEndian(salt, sessionToken);
            using var hmac = new HMACSHA256(presharedKey);
            var label = Encoding.ASCII.GetBytes("NetSync/3 session");
            hmac.TransformBlock(label, 0, label.Length, null, 0);
            hmac.TransformFinalBlock(salt, 0, salt.Length);
            return hmac.Hash!;
        }

        private static byte[] DeriveKey(byte[] secret, string label) {
            using var hmac = new HMACSHA256(secret);
            return hmac.ComputeHash(Encoding.ASCII.GetBytes(label));
        }

        /// <summary>Packet authenticator for one link of this connection.</summary>
        public LinkSecurity CreateLinkSecurity(TransportType slot) {
            return new LinkSecurity((byte)slot, Suite, sendKey: _authSendKey, receiveKey: _authReceiveKey);
        }

        /// <summary>
        /// Encrypts <paramref name="plaintext"/> into <paramref name="destination"/>, which
        /// must hold plaintext.Length + <see cref="MaxOverhead"/> bytes. Returns the bytes written.
        /// </summary>
        public int Encrypt(ReadOnlySpan<byte> plaintext, Span<byte> destination) {
            lock (_sendLock) {
                if (_disposed) {
                    throw new ObjectDisposedException(nameof(ConnectionCrypto));
                }
#if NET8_0_OR_GREATER
                if (_sendGcm != null) {
                    // Nonce = per-direction counter: unique for the life of this key.
                    var nonce = destination.Slice(0, GcmNonceSize);
                    nonce.Clear();
                    BinaryPrimitives.WriteInt64BigEndian(nonce.Slice(4), Interlocked.Increment(ref _sendCounter));
                    _sendGcm.Encrypt(nonce, plaintext,
                                     destination.Slice(GcmNonceSize + GcmTagSize, plaintext.Length),
                                     destination.Slice(GcmNonceSize, GcmTagSize));
                    return GcmNonceSize + GcmTagSize + plaintext.Length;
                }
                var iv = destination.Slice(0, CbcIvSize);
                RandomNumberGenerator.Fill(iv);
                int written = _sendAes!.EncryptCbc(plaintext, iv, destination.Slice(CbcIvSize), PaddingMode.PKCS7);
                return CbcIvSize + written;
#else
                var ivArray = new byte[CbcIvSize];
                RandomNumberGenerator.Fill(ivArray);
                using var encryptor = _sendAes!.CreateEncryptor(_sendKey, ivArray);
                var plainArray = plaintext.ToArray();
                var ciphertext = encryptor.TransformFinalBlock(plainArray, 0, plainArray.Length);
                ivArray.CopyTo(destination);
                ciphertext.CopyTo(destination.Slice(CbcIvSize));
                return CbcIvSize + ciphertext.Length;
#endif
            }
        }

        /// <summary>
        /// Decrypts into <paramref name="destination"/> (at least message.Length bytes).
        /// Returns the plaintext length, or -1 when the message is malformed or (GCM) fails
        /// its tag.
        /// </summary>
        public int Decrypt(ReadOnlySpan<byte> message, Span<byte> destination) {
            lock (_receiveLock) {
                if (_disposed) {
                    return -1;
                }
                try {
#if NET8_0_OR_GREATER
                    if (_receiveGcm != null) {
                        if (message.Length < GcmNonceSize + GcmTagSize) {
                            return -1;
                        }
                        int length = message.Length - GcmNonceSize - GcmTagSize;
                        _receiveGcm.Decrypt(message.Slice(0, GcmNonceSize), message.Slice(GcmNonceSize + GcmTagSize),
                                            message.Slice(GcmNonceSize, GcmTagSize), destination.Slice(0, length));
                        return length;
                    }
                    if (message.Length < CbcIvSize + AesBlock || (message.Length - CbcIvSize) % AesBlock != 0) {
                        return -1;
                    }
                    return _receiveAes!.DecryptCbc(message.Slice(CbcIvSize), message.Slice(0, CbcIvSize), destination, PaddingMode.PKCS7);
#else
                    if (message.Length < CbcIvSize + AesBlock || (message.Length - CbcIvSize) % AesBlock != 0) {
                        return -1;
                    }
                    using var decryptor = _receiveAes!.CreateDecryptor(_receiveKey, message.Slice(0, CbcIvSize).ToArray());
                    var ciphertext = message.Slice(CbcIvSize).ToArray();
                    var plaintext = decryptor.TransformFinalBlock(ciphertext, 0, ciphertext.Length);
                    plaintext.CopyTo(destination);
                    return plaintext.Length;
#endif
                }
                catch (CryptographicException) {
                    return -1; // bad padding / tag: only reachable with mismatched keys
                }
            }
        }

        // Constant-time comparison; implemented manually so it exists on every target.
        internal static bool FixedTimeEquals(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b) {
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
            lock (_sendLock) {
                lock (_receiveLock) {
                    if (_disposed) {
                        return;
                    }
                    _disposed = true;
                    _sendAes?.Dispose();
                    _receiveAes?.Dispose();
#if NET8_0_OR_GREATER
                    _sendGcm?.Dispose();
                    _receiveGcm?.Dispose();
#endif
                    Array.Clear(_sendKey, 0, _sendKey.Length);
                    Array.Clear(_receiveKey, 0, _receiveKey.Length);
                    Array.Clear(_authSendKey, 0, _authSendKey.Length);
                    Array.Clear(_authReceiveKey, 0, _authReceiveKey.Length);
                }
            }
        }
    }
}
