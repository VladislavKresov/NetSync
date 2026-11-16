using System;
using System.Security.Cryptography;
using System.Text;
using NetSync.Pipeline.Security;

namespace NetSync {
    /// <summary>
    /// A server's long-term identity: an X25519 key pair. Every handshake mixes an exchange
    /// with this key into the session keys, so only the holder of the private key can
    /// complete it; clients that pin the public key
    /// (<see cref="EncryptionConfig.TrustedServerKeys"/>) refuse anyone else — this is
    /// what makes <see cref="KeyExchangeMode.Ecdh"/> safe against an active
    /// man-in-the-middle, the way a pinned certificate does for TLS.
    ///
    /// Generate once, store the private key (<see cref="ExportPrivateKey"/>) like any
    /// secret, ship the public key (or its <see cref="Fingerprint"/>) with your clients.
    /// Works on every runtime: the implementation is part of the library.
    /// </summary>
    public sealed class NetIdentity : IDisposable {
        /// <summary>Size of <see cref="PublicKey"/>.</summary>
        public const int PublicKeySize = X25519.KeySize;
        /// <summary>Size of the exported private key.</summary>
        public const int PrivateKeySize = X25519.KeySize;

        private readonly byte[] _privateKey;
        private bool _disposed;

        /// <summary>The public key clients pin.</summary>
        public byte[] PublicKey { get; }

        /// <summary>Short human-readable form of <see cref="PublicKey"/> (for logs and UIs).</summary>
        public string Fingerprint => GetFingerprint(PublicKey);

        /// <summary>Always true: identities no longer depend on what the runtime's class library offers.</summary>
        public static bool IsSupported => true;

        private NetIdentity(byte[] privateKey) {
            _privateKey = privateKey;
            PublicKey = new byte[PublicKeySize];
            X25519.PublicKey(privateKey, PublicKey);
        }

        /// <summary>Creates a new random identity.</summary>
        public static NetIdentity Generate() {
            var privateKey = new byte[PrivateKeySize];
            X25519.GeneratePrivateKey(privateKey);
            return new NetIdentity(privateKey);
        }

        /// <summary>Restores an identity from <see cref="ExportPrivateKey"/> output.</summary>
        public static NetIdentity FromPrivateKey(byte[] privateKey) {
            if (privateKey == null || privateKey.Length != PrivateKeySize) {
                throw new ArgumentException($"A NetIdentity private key is {PrivateKeySize} bytes", nameof(privateKey));
            }
            return new NetIdentity((byte[])privateKey.Clone());
        }

        /// <summary>The private key. Treat it like a password.</summary>
        public byte[] ExportPrivateKey() {
            ThrowIfDisposed();
            return (byte[])_privateKey.Clone();
        }

        /// <summary>"SHA256:" + the first 16 bytes of SHA-256(public key), hex.</summary>
        public static string GetFingerprint(byte[] publicKey) {
            if (publicKey == null) {
                throw new ArgumentNullException(nameof(publicKey));
            }
            using var sha = SHA256.Create();
            var hash = sha.ComputeHash(publicKey);
            var builder = new StringBuilder("SHA256:");
            for (int i = 0; i < 16; i++) {
                if (i > 0) {
                    builder.Append(':');
                }
                builder.Append(hash[i].ToString("x2"));
            }
            return builder.ToString();
        }

        /// <summary>The exchange between this identity and a client's ephemeral key; false for an unusable key.</summary>
        internal bool TrySharedSecret(ReadOnlySpan<byte> clientPublicKey, Span<byte> sharedSecret) {
            ThrowIfDisposed();
            return X25519.TrySharedSecret(_privateKey, clientPublicKey, sharedSecret);
        }

        private void ThrowIfDisposed() {
            if (_disposed) {
                throw new ObjectDisposedException(nameof(NetIdentity));
            }
        }

        public void Dispose() {
            _disposed = true;
            Array.Clear(_privateKey, 0, _privateKey.Length);
        }
    }
}
