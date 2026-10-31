using System;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NetSync;
using NetSync.Diagnostics;
using NetSync.Peers;
using NetSync.Pipeline.Compression;
using NetSync.Pipeline.Security;
using Xunit;

namespace NetSync.Tests {
    public class SecurityTests {
        private static readonly byte[] Psk = Encoding.ASCII.GetBytes("0123456789abcdef0123456789abcdef");

        public static TheoryData<CipherSuite> Suites() => HeaderSecurityTests.Suites();

        private static byte[] Encrypt(ConnectionCrypto crypto, byte[] plaintext) {
            var buffer = new byte[plaintext.Length + crypto.MaxOverhead];
            int length = crypto.Encrypt(plaintext, buffer);
            return buffer.AsSpan(0, length).ToArray();
        }

        private static byte[]? Decrypt(ConnectionCrypto crypto, byte[] message) {
            var buffer = new byte[message.Length];
            int length = crypto.Decrypt(message, buffer);
            return length < 0 ? null : buffer.AsSpan(0, length).ToArray();
        }

        [Theory]
        [MemberData(nameof(Suites))]
        public void Crypto_Roundtrips_Without_Leaking_Plaintext(CipherSuite suite) {
            using var client = ConnectionCrypto.FromPresharedKey(Psk, sessionToken: 42, suite, isClient: true);
            using var server = ConnectionCrypto.FromPresharedKey(Psk, sessionToken: 42, suite, isClient: false);

            var plaintext = new byte[777];
            new Random(1).NextBytes(plaintext);

            var encrypted = Encrypt(client, plaintext);
            // No plaintext block may appear anywhere in the ciphertext.
            var firstBlock = plaintext.Take(16).ToArray();
            Assert.DoesNotContain(Chunks(encrypted, 16), chunk => chunk.SequenceEqual(firstBlock));
            Assert.Equal(plaintext, Decrypt(server, encrypted));
            // Each direction has its own key: a client can't read its own traffic reflected back.
            Assert.NotEqual(plaintext, Decrypt(client, encrypted));
        }

        [Fact]
        public void Gcm_Payloads_Also_Carry_Their_Own_Tag() {
            if (!CipherSuites.GcmSupported) {
                return;
            }
            using var client = ConnectionCrypto.FromPresharedKey(Psk, 42, CipherSuite.AesGcm, isClient: true);
            using var server = ConnectionCrypto.FromPresharedKey(Psk, 42, CipherSuite.AesGcm, isClient: false);
            var encrypted = Encrypt(client, new byte[] { 1, 2, 3, 4 });
            encrypted[^1] ^= 0xFF;
            Assert.Null(Decrypt(server, encrypted));
            // CBC payloads rely on the packet envelope instead — covered by HeaderSecurityTests.
        }

        [Theory]
        [MemberData(nameof(Suites))]
        public void Different_Psk_Token_Or_Suite_Yields_Incompatible_Keys(CipherSuite suite) {
            var (client, server) = (ConnectionCrypto.FromPresharedKey(Psk, 42, suite, true).CreateLinkSecurity(TransportType.Tcp),
                                    ConnectionCrypto.FromPresharedKey(Psk, 42, suite, false).CreateLinkSecurity(TransportType.Tcp));
            var wrongKey = ConnectionCrypto.FromPresharedKey(Encoding.ASCII.GetBytes("another-key-another-key-32bytes!"), 42, suite, false)
                                           .CreateLinkSecurity(TransportType.Tcp);
            var wrongToken = ConnectionCrypto.FromPresharedKey(Psk, 43, suite, false).CreateLinkSecurity(TransportType.Tcp);
            var otherSuite = suite == CipherSuite.AesGcm ? CipherSuite.AesCbcHmacSha256 : (CipherSuites.GcmSupported ? CipherSuite.AesGcm : suite);

            var envelope = client.Wrap(ReadOnlySpan<byte>.Empty, new byte[] { 0x10, 0, 1, 2, 3 });
            Assert.True(server.TryUnwrap((byte[])envelope.Clone(), 0, envelope.Length, out _, out _));
            Assert.False(wrongKey.TryUnwrap((byte[])envelope.Clone(), 0, envelope.Length, out _, out _));
            Assert.False(wrongToken.TryUnwrap((byte[])envelope.Clone(), 0, envelope.Length, out _, out _));
            if (otherSuite != suite) {
                var mismatched = ConnectionCrypto.FromPresharedKey(Psk, 42, otherSuite, false).CreateLinkSecurity(TransportType.Tcp);
                Assert.False(mismatched.TryUnwrap((byte[])envelope.Clone(), 0, envelope.Length, out _, out _));
            }
        }

        [Fact]
        public void Ecdh_Both_Sides_Derive_The_Same_Secret() {
            using var client = EcdhKeyExchange.Create();
            using var server = EcdhKeyExchange.Create();
            var clientPub = EcdhKeyExchange.ExportPublicKey(client);
            var serverPub = EcdhKeyExchange.ExportPublicKey(server);

            var clientSecret = EcdhKeyExchange.DeriveSessionSecret(client, serverPub, sessionToken: 7);
            var serverSecret = EcdhKeyExchange.DeriveSessionSecret(server, clientPub, sessionToken: 7);

            Assert.Equal(clientSecret, serverSecret);
            Assert.Equal(32, clientSecret.Length);
        }

        [Fact]
        public void Compressor_Roundtrips_And_Rejects_Garbage() {
            var compressible = Encoding.ASCII.GetBytes(string.Concat(Enumerable.Repeat("NetSync compresses this. ", 400)));
            var blob = Compressor.TryCompress(compressible, out int blobLength)!;
            Assert.True(blobLength < compressible.Length);
            var inflated = Compressor.Decompress(blob, 0, blobLength, 1 << 20, NullNetLogger.Instance, out int inflatedLength);
            Assert.Equal(compressible, inflated!.AsSpan(0, inflatedLength).ToArray());

            Assert.Null(Compressor.Decompress(blob, 0, blobLength - 1, 1 << 20, NullNetLogger.Instance, out _)); // truncated
            Assert.Null(Compressor.Decompress(blob, 0, blobLength, compressible.Length - 1, NullNetLogger.Instance, out _)); // over the cap
            blob[7] ^= 0xFF; // corrupt the deflate stream
            Assert.Null(Compressor.Decompress(blob, 0, blobLength, 1 << 20, NullNetLogger.Instance, out _));

            var random = new byte[1000];
            new Random(1).NextBytes(random);
            Assert.Null(Compressor.TryCompress(random, out _)); // incompressible: not worth sending compressed
        }

        [Fact]
        public void Transform_Skips_Compression_When_It_Does_Not_Help() {
            var channel = new ChannelConfig(TransportType.Tcp, compression: true);
            var incompressible = new byte[4096];
            new Random(2).NextBytes(incompressible);

            var encoded = ChannelTransform.Encode(channel, null, incompressible, out int encodedLength);
            Assert.Equal(incompressible.Length + 1, encodedLength); // flags byte only
            var decoded = ChannelTransform.Decode(channel, null, encoded, 0, encodedLength, 1 << 20, NullNetLogger.Instance, out int decodedLength);
            Assert.Equal(incompressible, decoded!.AsSpan(0, decodedLength).ToArray());
        }

        [Fact]
        public void Transform_Rejects_Plaintext_On_Encrypted_Channel() {
            var channel = new ChannelConfig(TransportType.Tcp, encryption: true);
            using var crypto = ConnectionCrypto.FromPresharedKey(Psk, 1, CipherSuite.AesCbcHmacSha256, isClient: false);
            // flags byte says "not encrypted" — downgrade attempt must be dropped
            var forged = new byte[] { 0x00, 1, 2, 3 };
            Assert.Null(ChannelTransform.Decode(channel, crypto, forged, 0, forged.Length, 1 << 20, NullNetLogger.Instance, out _));
        }

        [Fact]
        public async Task Encrypted_And_Compressed_Channels_Roundtrip_EndToEnd() {
            var (clientConfig, serverConfig) = MakeSecureConfigs(KeyExchangeMode.PresharedKey);
            using var server = new NetServer(serverConfig);
            server.DataReceived += (conn, channel, data) => _ = conn.SendAsync(channel, data);
            int port = await server.StartAsync(0);

            using var client = new NetClient(clientConfig);
            var tcpEcho = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
            var udpEcho = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
            client.DataReceived += (channel, data) => {
                if (channel == 0) tcpEcho.TrySetResult(data);
                if (channel == 2) udpEcho.TrySetResult(data);
            };
            await client.ConnectAsync("127.0.0.1", port);

            // Compressible payload over encrypted+compressed TCP channel.
            var text = Encoding.ASCII.GetBytes(string.Concat(Enumerable.Repeat("secret payload ", 2000)));
            await client.SendAsync(0, text);
            // Large binary payload over encrypted reliable-ordered UDP channel.
            var binary = new byte[200_000];
            new Random(3).NextBytes(binary);
            await client.SendAsync(2, binary);

            Assert.Equal(text, await WithTimeout(tcpEcho.Task, 15_000));
            Assert.Equal(binary, await WithTimeout(udpEcho.Task, 30_000));
            await server.StopAsync();
        }

        [Fact]
        public async Task Ecdh_Handshake_Establishes_Working_Encryption() {
            var (clientConfig, serverConfig) = MakeSecureConfigs(KeyExchangeMode.Ecdh);
            using var server = new NetServer(serverConfig);
            server.DataReceived += (conn, channel, data) => _ = conn.SendAsync(channel, data);
            int port = await server.StartAsync(0);

            using var client = new NetClient(clientConfig);
            var echo = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
            client.DataReceived += (channel, data) => {
                if (channel == 0) echo.TrySetResult(data);
            };
            await client.ConnectAsync("127.0.0.1", port);

            await client.SendAsync(0, new byte[] { 4, 5, 6 });
            Assert.Equal(new byte[] { 4, 5, 6 }, await WithTimeout(echo.Task, 10_000));
            await server.StopAsync();
        }

        [Fact]
        public async Task Encryption_Mode_Mismatch_Is_Rejected_At_Handshake() {
            var (_, serverConfig) = MakeSecureConfigs(KeyExchangeMode.PresharedKey);
            using var server = new NetServer(serverConfig);
            int port = await server.StartAsync(0);

            // Same channel table, different key exchange (ECDH client vs PSK server).
            var (clientConfig, _) = MakeSecureConfigs(KeyExchangeMode.Ecdh);
            using var client = new NetClient(clientConfig);

            var ex = await Assert.ThrowsAsync<ConnectionRejectedException>(async () =>
                await client.ConnectAsync("127.0.0.1", port));
            Assert.Equal(ConnectionRejection.Encryption, ex.Rejection);
            Assert.Contains("encryption mode mismatch", ex.Message);
            await server.StopAsync();
        }

        [Fact]
        public void Encrypted_Channel_Without_Key_Config_Throws() {
            var config = new NetConfig();
            config.Channels[0] = new ChannelConfig(TransportType.Tcp, encryption: true);
            Assert.Throws<InvalidOperationException>(() => new NetClient(config));
        }

        [Fact]
        public async Task Wrong_Psk_Fails_The_Connect_With_A_Clear_Error() {
            var (clientConfig, serverConfig) = MakeSecureConfigs(KeyExchangeMode.PresharedKey);
            clientConfig.Encryption.PresharedKey = Encoding.ASCII.GetBytes("wrong-key-wrong-key-wrong-key-32");

            using var server = new NetServer(serverConfig);
            int opened = 0;
            server.ConnectionOpened += _ => Interlocked.Increment(ref opened);
            int port = await server.StartAsync(0);

            using var client = new NetClient(clientConfig);
            // The Join is enveloped with the wrong key: the server can't verify it and says so,
            // instead of letting the client believe it is connected while all data is dropped.
            var ex = await Assert.ThrowsAsync<ConnectionRejectedException>(() => client.ConnectAsync("127.0.0.1", port));
            Assert.Equal(ConnectionRejection.Authentication, ex.Rejection);
            Assert.False(client.IsConnected);
            Assert.Equal(0, Volatile.Read(ref opened));
            await server.StopAsync();
        }

        private static (NetConfig client, NetConfig server) MakeSecureConfigs(KeyExchangeMode mode) {
            NetConfig Make() {
                var config = new NetConfig {
                    EventDelivery = EventDelivery.Immediate,
                    PingIntervalMs = 200,
                    ConnectTimeoutMs = 5000,
                    Encryption = new EncryptionConfig {
                        Mode = mode,
                        PresharedKey = mode == KeyExchangeMode.PresharedKey ? (byte[])Psk.Clone() : null
                    }
                };
                config.Channels[0] = new ChannelConfig(TransportType.Tcp, compression: true, encryption: true);
                config.Channels[1] = new ChannelConfig(TransportType.Udp);
                config.Channels[2] = new ChannelConfig(TransportType.Udp, ReliabilityMode.ReliableOrdered, encryption: true);
                return config;
            }
            return (Make(), Make());
        }

        private static byte[][] Chunks(byte[] data, int size) {
            return Enumerable.Range(0, data.Length / size).Select(i => data.Skip(i * size).Take(size).ToArray()).ToArray();
        }

        private static async Task<T> WithTimeout<T>(Task<T> task, int timeoutMs) {
            var completed = await Task.WhenAny(task, Task.Delay(timeoutMs));
            Assert.Same(task, completed);
            return await task;
        }
    }
}
