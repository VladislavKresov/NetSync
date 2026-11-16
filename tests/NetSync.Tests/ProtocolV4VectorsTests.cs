using System;
using System.Linq;
using System.Text;
using NetSync.Peers;
using NetSync.Pipeline.Security;
using NetSync.Relay;
using Xunit;

namespace NetSync.Tests {
    /// <summary>
    /// Protocol v4 byte for byte. These values are the contract: a change that breaks one of
    /// these tests breaks compatibility with every released v4 peer. They are reproduced
    /// independently by tools/protocol-v4-vectors.py, and any other implementation (a browser
    /// client) must arrive at the same bytes.
    /// </summary>
    public class ProtocolV4VectorsTests {
        private const long Token = 0x0102030405060708;
        private const uint ConfigHash = 0xA1B2C3D4;
        private const ulong AppId = 0x1122334455667788;
        private const ushort AppVersion = 7;

        private static readonly byte[] ClientPrivate = Hex("77076d0a7318a57d3c16c17251b26645df4c2f87ebc0992ab177fba51db92c2a");
        private static readonly byte[] ServerPrivate = Hex("5dab087e624a8a4b79e17f8b83800ee66f3bb1292618b6fd1c2f8b27ff88e0eb");
        private static readonly byte[] IdentityPrivate = Enumerable.Repeat((byte)0x55, 32).ToArray();

        private static byte[] Hex(string hex) => Convert.FromHexString(hex);
        private static string ToHex(byte[] bytes) => Convert.ToHexString(bytes).ToLowerInvariant();
        private static byte[] Sequence(int from, int count) => Enumerable.Range(from, count).Select(i => (byte)i).ToArray();

        private static byte[] Public(byte[] privateKey) {
            var publicKey = new byte[32];
            X25519.PublicKey(privateKey, publicKey);
            return publicKey;
        }

        private static byte[] Shared(byte[] privateKey, byte[] publicKey) {
            var shared = new byte[32];
            Assert.True(X25519.TrySharedSecret(privateKey, publicKey, shared));
            return shared;
        }

        [Fact]
        public void Hello_Bytes() {
            var hello = PeerProtocol.EncodeHello(Token, ConfigHash, KeyExchangeMode.Ecdh, 3, Sequence(0, 32), null, AppId, AppVersion, 0);
            Assert.Equal("01040102030405060708a1b2c3d4020311223344556677880007000000002000" +
                         "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f000000", ToHex(hello));
        }

        [Fact]
        public void Welcome_Bytes() {
            var welcome = PeerProtocol.EncodeWelcome(0x0A0B0C0D0E0F1011, CipherSuite.AesCbcHmacSha256, 0, Sequence(0x20, 32), Sequence(0x40, 32));
            Assert.Equal("020a0b0c0d0e0f1011010000000020202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f" +
                         "20404142434445464748494a4b4c4d4e4f505152535455565758595a5b5c5d5e5f0000", ToHex(welcome));
        }

        [Fact]
        public void Session_Secret_With_A_Server_Identity() {
            var identityPublic = Public(IdentityPrivate);
            Assert.Equal("38ab664bd86f77d7e66bdd9ae0792913a94fd8b33a1260027e4b46c1f4884c67", ToHex(identityPublic));

            var transcript = HandshakeKeys.Transcript(Token, ConfigHash, (byte)KeyExchangeMode.Ecdh, 3, AppId, AppVersion, 0, Public(ClientPrivate),
                                                      CipherSuite.AesCbcHmacSha256, 0, Public(ServerPrivate), identityPublic);
            Assert.Equal("4e657453796e632f342068616e647368616b650102030405060708a1b2c3d40203112233445566778800070000000020" +
                         "8520f0098930a754748b7ddcb43ef75a0dbf3a0d26381af4eba4a98eaa9b4e6a010000000020" +
                         "de9edb7d7b7dc1b4d35b61c2ece435373f8343c85b78674dadfc7e146f882b4f20" +
                         "38ab664bd86f77d7e66bdd9ae0792913a94fd8b33a1260027e4b46c1f4884c67", ToHex(transcript));

            // The client computes it from its ephemeral key, the server from its own two keys.
            var fromClient = HandshakeKeys.SessionSecret(transcript, Shared(ClientPrivate, Public(ServerPrivate)),
                                                         Shared(ClientPrivate, identityPublic), ReadOnlySpan<byte>.Empty);
            var fromServer = HandshakeKeys.SessionSecret(transcript, Shared(ServerPrivate, Public(ClientPrivate)),
                                                         Shared(IdentityPrivate, Public(ClientPrivate)), ReadOnlySpan<byte>.Empty);
            Assert.Equal("d9bf75ad5d1673abbce8a7846dc6d4748f11ea76104428f6b056e7fca36500e5", ToHex(fromClient));
            Assert.Equal(fromClient, fromServer);
        }

        [Fact]
        public void Session_Secret_With_A_Preshared_Key() {
            var transcript = HandshakeKeys.Transcript(Token, ConfigHash, (byte)KeyExchangeMode.PresharedKey, 3, AppId, AppVersion, 0,
                                                      Public(ClientPrivate), CipherSuite.AesCbcHmacSha256, 0, Public(ServerPrivate),
                                                      ReadOnlySpan<byte>.Empty);
            var secret = HandshakeKeys.SessionSecret(transcript, Shared(ClientPrivate, Public(ServerPrivate)), ReadOnlySpan<byte>.Empty,
                                                     Encoding.ASCII.GetBytes("0123456789abcdef0123456789abcdef"));
            Assert.Equal("783117ce6c4d95efead0e74e6c11b0cef0e66f1265a2ea4821db1fea49385b37", ToHex(secret));
        }

        [Fact]
        public void Authenticated_Envelope_Bytes() {
            var secret = Hex("d9bf75ad5d1673abbce8a7846dc6d4748f11ea76104428f6b056e7fca36500e5");
            using var crypto = ConnectionCrypto.FromSecret(secret, CipherSuite.AesCbcHmacSha256, isClient: true);
            using var link = crypto.CreateLinkSecurity(TransportType.Udp);
            var envelope = link.Wrap(new byte[] { 0x10, 0x02 }, Encoding.ASCII.GetBytes("hello"));
            Assert.Equal("400000000000000001ac8e4d811c82936333371c57e6cdecac100268656c6c6f", ToHex(envelope));

            using var server = ConnectionCrypto.FromSecret(secret, CipherSuite.AesCbcHmacSha256, isClient: false);
            using var serverLink = server.CreateLinkSecurity(TransportType.Udp);
            Assert.True(serverLink.TryUnwrap(envelope, 0, envelope.Length, out int offset, out int count));
            Assert.Equal(7, count);
            Assert.Equal(0x10, envelope[offset]);
        }

        [Fact]
        public void Relay_Hello_Bytes() {
            Assert.Equal("010000000004524f4f4d036b6579", ToHex(RelayProtocol.EncodeHello("ROOM", "key")));
        }
    }
}
