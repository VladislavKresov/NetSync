using System;
using NetSync.Pipeline.Security;
using Xunit;

namespace NetSync.Tests {
    /// <summary>The library's own primitives against the published test vectors.</summary>
    public class CryptoPrimitivesTests {
        private static byte[] Hex(string hex) => Convert.FromHexString(hex);

        private static string ToHex(ReadOnlySpan<byte> bytes) => Convert.ToHexString(bytes).ToLowerInvariant();

        [Theory] // RFC 7748, section 5.2
        [InlineData("a546e36bf0527c9d3b16154b82465edd62144c0ac1fc5a18506a2244ba449ac4",
                    "e6db6867583030db3594c1a424b15f7c726624ec26b3353b10a903a6d0ab1c4c",
                    "c3da55379de9c6908e94ea4df28d084f32eccf03491c71f754b4075577a28552")]
        [InlineData("4b66e9d4d1b4673c5ad22691957d6af5c11b6421e0ea01d42ca4169e7918ba0d",
                    "e5210f12786811d3f4b7959d0538ae2c31dbe7106fc03c3efc4cd549c715a493",
                    "95cbde9476e8907d7aade45cb4b873f88b595a68799fa152e6f8f7647aac7957")]
        public void X25519_Matches_Rfc7748_Vectors(string scalar, string point, string expected) {
            var result = new byte[32];
            X25519.ScalarMultiply(Hex(scalar), Hex(point), result);
            Assert.Equal(expected, ToHex(result));
        }

        [Fact] // RFC 7748, section 5.2: iterated
        public void X25519_Matches_Rfc7748_After_A_Thousand_Iterations() {
            var k = new byte[32];
            var u = new byte[32];
            k[0] = 9;
            u[0] = 9;
            var result = new byte[32];
            for (int i = 0; i < 1000; i++) {
                X25519.ScalarMultiply(k, u, result);
                if (i == 0) {
                    Assert.Equal("422c8e7a6227d7bca1350b3e2bb7279f7897b87bb6854b783c60e80311ae3079", ToHex(result));
                }
                k.CopyTo(u, 0);
                result.CopyTo(k, 0);
            }
            Assert.Equal("684cf59ba83309552800ef566f2f4d3c1c3887c49360e3875f2eb94d99532c51", ToHex(k));
        }

        [Fact] // RFC 7748, section 6.1
        public void X25519_Key_Agreement_Matches_Rfc7748() {
            var alicePrivate = Hex("77076d0a7318a57d3c16c17251b26645df4c2f87ebc0992ab177fba51db92c2a");
            var bobPrivate = Hex("5dab087e624a8a4b79e17f8b83800ee66f3bb1292618b6fd1c2f8b27ff88e0eb");
            var alicePublic = new byte[32];
            var bobPublic = new byte[32];
            X25519.PublicKey(alicePrivate, alicePublic);
            X25519.PublicKey(bobPrivate, bobPublic);
            Assert.Equal("8520f0098930a754748b7ddcb43ef75a0dbf3a0d26381af4eba4a98eaa9b4e6a", ToHex(alicePublic));
            Assert.Equal("de9edb7d7b7dc1b4d35b61c2ece435373f8343c85b78674dadfc7e146f882b4f", ToHex(bobPublic));

            var fromAlice = new byte[32];
            var fromBob = new byte[32];
            Assert.True(X25519.TrySharedSecret(alicePrivate, bobPublic, fromAlice));
            Assert.True(X25519.TrySharedSecret(bobPrivate, alicePublic, fromBob));
            Assert.Equal("4a5d9d5ba4ce2de1728e3bf480350f25e07e21c947d19e3376f09b3c1e161742", ToHex(fromAlice));
            Assert.Equal(fromAlice, fromBob);
        }

        [Fact]
        public void X25519_Generated_Keys_Agree_And_Zero_Points_Are_Refused() {
            var a = new byte[32];
            var b = new byte[32];
            X25519.GeneratePrivateKey(a);
            X25519.GeneratePrivateKey(b);
            var aPublic = new byte[32];
            var bPublic = new byte[32];
            X25519.PublicKey(a, aPublic);
            X25519.PublicKey(b, bPublic);
            var first = new byte[32];
            var second = new byte[32];
            Assert.True(X25519.TrySharedSecret(a, bPublic, first));
            Assert.True(X25519.TrySharedSecret(b, aPublic, second));
            Assert.Equal(first, second);

            Assert.False(X25519.TrySharedSecret(a, new byte[32], first)); // the all-zero point gives an all-zero secret
        }

        [Fact] // RFC 5869, test case 1
        public void Hkdf_Matches_Rfc5869_Case_1() {
            var ikm = Hex("0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b");
            var salt = Hex("000102030405060708090a0b0c");
            var info = Hex("f0f1f2f3f4f5f6f7f8f9");
            Assert.Equal("077709362c2e32df0ddc3f0dc47bba6390b6c73bb50f9c3122ec844ad7c2b3e5", ToHex(Hkdf.Extract(salt, ikm)));
            Assert.Equal("3cb25f25faacd57a90434f64d0362f2a2d2d0a90cf1a5a4c5db02d56ecc4c5bf34007208d5b887185865",
                         ToHex(Hkdf.DeriveKey(ikm, salt, info, 42)));
        }

        [Fact] // RFC 5869, test case 3: empty salt and info
        public void Hkdf_Matches_Rfc5869_Case_3() {
            var ikm = Hex("0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b");
            Assert.Equal("8da4e775a563c18f715f802a063c5a31b8a11f5c5ee1879ec3454e5f3c738d2d9d201395faa4b61a96c8",
                         ToHex(Hkdf.DeriveKey(ikm, ReadOnlySpan<byte>.Empty, ReadOnlySpan<byte>.Empty, 42)));
        }
    }
}
