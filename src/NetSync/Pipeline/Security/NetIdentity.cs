using System;
using System.Security.Cryptography;
using System.Text;

namespace NetSync {
    /// <summary>
    /// A server's long-term identity: an ECDSA P-256 key pair. The server signs every
    /// ECDH handshake with it; clients that pin the public key
    /// (<see cref="EncryptionConfig.TrustedServerKeys"/>) refuse anyone else — this is
    /// what makes <see cref="KeyExchangeMode.Ecdh"/> safe against an active
    /// man-in-the-middle, the way a pinned certificate does for TLS.
    ///
    /// Generate once, store the private key (<see cref="ExportPrivateKey"/>) like any
    /// secret, ship the public key (or its <see cref="Fingerprint"/>) with your clients.
    /// </summary>
    public sealed class NetIdentity : IDisposable {
        /// <summary>Raw public key: X ‖ Y, 32 bytes each.</summary>
        public const int PublicKeySize = 64;
        /// <summary>Exported private key: D ‖ X ‖ Y.</summary>
        public const int PrivateKeySize = 96;
        internal const int SignatureSize = 64; // IEEE P1363 r ‖ s

        private static readonly Lazy<bool> Supported = new Lazy<bool>(Probe);

        private readonly ECDsa _key;

        /// <summary>The public key clients pin.</summary>
        public byte[] PublicKey { get; }

        /// <summary>Short human-readable form of <see cref="PublicKey"/> (for logs and UIs).</summary>
        public string Fingerprint => GetFingerprint(PublicKey);

        /// <summary>
        /// Whether this runtime can sign and verify with ECDSA P-256 (some Mono/IL2CPP builds
        /// can't). Clients without it can't pin a server identity.
        /// </summary>
        public static bool IsSupported => Supported.Value;

        private NetIdentity(ECDsa key) {
            _key = key;
            var parameters = key.ExportParameters(includePrivateParameters: false);
            PublicKey = new byte[PublicKeySize];
            PadTo32(parameters.Q.X!).CopyTo(PublicKey, 0);
            PadTo32(parameters.Q.Y!).CopyTo(PublicKey, 32);
        }

        /// <summary>Creates a new random identity.</summary>
        public static NetIdentity Generate() => new NetIdentity(ECDsa.Create(ECCurve.NamedCurves.nistP256));

        /// <summary>Restores an identity from <see cref="ExportPrivateKey"/> output.</summary>
        public static NetIdentity FromPrivateKey(byte[] privateKey) {
            if (privateKey == null || privateKey.Length != PrivateKeySize) {
                throw new ArgumentException($"A NetIdentity private key is {PrivateKeySize} bytes (D ‖ X ‖ Y)", nameof(privateKey));
            }
            var parameters = new ECParameters {
                Curve = ECCurve.NamedCurves.nistP256,
                D = privateKey.AsSpan(0, 32).ToArray(),
                Q = new ECPoint {
                    X = privateKey.AsSpan(32, 32).ToArray(),
                    Y = privateKey.AsSpan(64, 32).ToArray()
                }
            };
            var key = ECDsa.Create();
            try {
                key.ImportParameters(parameters);
            }
            catch {
                key.Dispose();
                throw;
            }
            return new NetIdentity(key);
        }

        /// <summary>The private key (D ‖ X ‖ Y). Treat it like a password.</summary>
        public byte[] ExportPrivateKey() {
            var parameters = _key.ExportParameters(includePrivateParameters: true);
            var result = new byte[PrivateKeySize];
            PadTo32(parameters.D!).CopyTo(result, 0);
            PublicKey.CopyTo(result, 32);
            Array.Clear(parameters.D!, 0, parameters.D!.Length);
            return result;
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

        internal byte[] Sign(byte[] data) {
            var signature = _key.SignData(data, HashAlgorithmName.SHA256);
            if (signature.Length != SignatureSize) {
                throw new CryptographicException($"Unexpected ECDSA signature size {signature.Length}");
            }
            return signature;
        }

        internal static bool Verify(byte[] publicKey, byte[] data, byte[] signature) {
            if (publicKey.Length != PublicKeySize || signature.Length != SignatureSize) {
                return false;
            }
            try {
                using var key = ECDsa.Create(new ECParameters {
                    Curve = ECCurve.NamedCurves.nistP256,
                    Q = new ECPoint {
                        X = publicKey.AsSpan(0, 32).ToArray(),
                        Y = publicKey.AsSpan(32, 32).ToArray()
                    }
                });
                return key.VerifyData(data, signature, HashAlgorithmName.SHA256);
            }
            catch (CryptographicException) {
                return false; // not a point on the curve, etc.
            }
        }

        private static bool Probe() {
            try {
                using var identity = Generate();
                var data = new byte[] { 1, 2, 3 };
                return Verify(identity.PublicKey, data, identity.Sign(data));
            }
            catch (Exception) {
                return false;
            }
        }

        // Curve values can export shorter than 32 bytes (leading zeros stripped).
        private static byte[] PadTo32(byte[] value) {
            if (value.Length == 32) {
                return value;
            }
            if (value.Length > 32) {
                throw new CryptographicException("Unexpected P-256 value length");
            }
            var padded = new byte[32];
            value.CopyTo(padded, 32 - value.Length);
            return padded;
        }

        public void Dispose() => _key.Dispose();
    }
}
