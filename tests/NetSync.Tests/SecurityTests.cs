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

        [Fact]
        public void Crypto_Roundtrips_And_Rejects_Tampering() {
            using var alice = ConnectionCrypto.FromPresharedKey(Psk, sessionToken: 42);
            using var bob = ConnectionCrypto.FromPresharedKey(Psk, sessionToken: 42);

            var plaintext = new byte[777];
            new Random(1).NextBytes(plaintext);

            var encrypted = alice.Encrypt(plaintext);
            // No plaintext block may appear anywhere in the ciphertext.
            var firstBlock = plaintext.Take(16).ToArray();
            Assert.DoesNotContain(Chunks(encrypted, 16), chunk => chunk.SequenceEqual(firstBlock));
            Assert.Equal(plaintext, bob.Decrypt(encrypted));

            // Tamper with one ciphertext byte → authentication fails.
            encrypted[encrypted.Length - 1] ^= 0xFF;
            Assert.Null(bob.Decrypt(encrypted));
        }

        [Fact]
        public void Different_Psk_Or_Token_Yields_Incompatible_Keys() {
            using var alice = ConnectionCrypto.FromPresharedKey(Psk, 42);
            using var wrongKey = ConnectionCrypto.FromPresharedKey(Encoding.ASCII.GetBytes("another-key-another-key-32bytes!"), 42);
            using var wrongToken = ConnectionCrypto.FromPresharedKey(Psk, 43);

            var encrypted = alice.Encrypt(new byte[] { 1, 2, 3 });
            Assert.Null(wrongKey.Decrypt(encrypted));
            Assert.Null(wrongToken.Decrypt(encrypted));
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
            var blob = Compressor.Compress(compressible);
            Assert.True(blob.Length < compressible.Length);
            Assert.Equal(compressible, Compressor.Decompress(blob, NullNetLogger.Instance));

            blob[7] ^= 0xFF; // corrupt the deflate stream
            Assert.Null(Compressor.Decompress(blob, NullNetLogger.Instance));
        }

        [Fact]
        public void Transform_Skips_Compression_When_It_Does_Not_Help() {
            var channel = new ChannelConfig(TransportType.Tcp, compression: true);
            var incompressible = new byte[4096];
            new Random(2).NextBytes(incompressible);

            var encoded = ChannelTransform.Encode(channel, null, incompressible);
            Assert.Equal(incompressible.Length + 1, encoded.Length); // flags byte only
            Assert.Equal(incompressible, ChannelTransform.Decode(channel, null, encoded.ToArray(), NullNetLogger.Instance));
        }

        [Fact]
        public void Transform_Rejects_Plaintext_On_Encrypted_Channel() {
            var channel = new ChannelConfig(TransportType.Tcp, encryption: true);
            using var crypto = ConnectionCrypto.FromPresharedKey(Psk, 1);
            // flags byte says "not encrypted" — downgrade attempt must be dropped
            var forged = new byte[] { 0x00, 1, 2, 3 };
            Assert.Null(ChannelTransform.Decode(channel, crypto, forged, NullNetLogger.Instance));
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

            // Client with no encryption tries to join a PSK server.
            var clientConfig = new NetConfig { EventDelivery = EventDelivery.Immediate, ConnectTimeoutMs = 5000 };
            clientConfig.Channels[0] = new ChannelConfig(TransportType.Tcp);
            clientConfig.Channels[2] = new ChannelConfig(TransportType.Udp, ReliabilityMode.ReliableOrdered);
            using var client = new NetClient(clientConfig);

            var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                await client.ConnectAsync("127.0.0.1", port));
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
        public async Task Wrong_Psk_Drops_Data_Instead_Of_Delivering_Garbage() {
            var (clientConfig, serverConfig) = MakeSecureConfigs(KeyExchangeMode.PresharedKey);
            clientConfig.Encryption.PresharedKey = Encoding.ASCII.GetBytes("wrong-key-wrong-key-wrong-key-32");

            using var server = new NetServer(serverConfig);
            int received = 0;
            server.DataReceived += (_, _, _) => Interlocked.Increment(ref received);
            int port = await server.StartAsync(0);

            using var client = new NetClient(clientConfig);
            await client.ConnectAsync("127.0.0.1", port); // handshake itself is not keyed

            await client.SendAsync(0, new byte[] { 9, 9, 9 }); // encrypted with the wrong key
            await Task.Delay(700);

            Assert.Equal(0, received); // MAC check failed server-side → dropped, not delivered
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
