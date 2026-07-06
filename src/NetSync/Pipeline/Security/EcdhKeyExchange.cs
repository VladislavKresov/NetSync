using System;
using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;

namespace NetSync.Pipeline.Security {
    /// <summary>
    /// Ephemeral ECDH (NIST P-256) helpers. Public keys travel as raw 64-byte X‖Y
    /// points — no ASN.1, works on both target frameworks.
    /// The exchange is unauthenticated: it defeats passive eavesdropping, not an
    /// active MITM (documented in <see cref="KeyExchangeMode.Ecdh"/>).
    /// </summary>
    internal static class EcdhKeyExchange {
        public const int PublicKeySize = 64;

        public static ECDiffieHellman Create() {
            return ECDiffieHellman.Create(ECCurve.NamedCurves.nistP256);
        }

        public static byte[] ExportPublicKey(ECDiffieHellman ecdh) {
            var parameters = ecdh.ExportParameters(includePrivateParameters: false);
            var publicKey = new byte[PublicKeySize];
            PadTo32(parameters.Q.X!).CopyTo(publicKey, 0);
            PadTo32(parameters.Q.Y!).CopyTo(publicKey, 32);
            return publicKey;
        }

        /// <summary>Derives the session secret from our keypair, their public key and the session token.</summary>
        public static byte[] DeriveSessionSecret(ECDiffieHellman mine, ReadOnlySpan<byte> theirPublicKey, long sessionToken) {
            if (theirPublicKey.Length != PublicKeySize) {
                throw new ArgumentException("ECDH public key must be 64 bytes (X‖Y)", nameof(theirPublicKey));
            }
            var parameters = new ECParameters {
                Curve = ECCurve.NamedCurves.nistP256,
                Q = new ECPoint {
                    X = theirPublicKey.Slice(0, 32).ToArray(),
                    Y = theirPublicKey.Slice(32, 32).ToArray()
                }
            };

            using var theirs = ECDiffieHellman.Create();
            theirs.ImportParameters(parameters);
            byte[] shared = mine.DeriveKeyFromHash(theirs.PublicKey, HashAlgorithmName.SHA256);

            // Bind the secret to this session.
            var salt = new byte[8];
            BinaryPrimitives.WriteInt64BigEndian(salt, sessionToken);
            using var hmac = new HMACSHA256(shared);
            var label = Encoding.ASCII.GetBytes("NetSync/1 session");
            hmac.TransformBlock(label, 0, label.Length, null, 0);
            hmac.TransformFinalBlock(salt, 0, salt.Length);
            Array.Clear(shared, 0, shared.Length);
            return hmac.Hash!;
        }

        // Curve coordinates can export shorter than 32 bytes (leading zeros stripped).
        private static byte[] PadTo32(byte[] value) {
            if (value.Length == 32) {
                return value;
            }
            if (value.Length > 32) {
                throw new CryptographicException("Unexpected P-256 coordinate length");
            }
            var padded = new byte[32];
            value.CopyTo(padded, 32 - value.Length);
            return padded;
        }
    }
}
