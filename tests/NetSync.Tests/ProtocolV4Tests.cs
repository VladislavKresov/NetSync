using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NetSync.Peers;
using Xunit;

namespace NetSync.Tests {
    /// <summary>What protocol v4 promises: fresh keys per session, room to grow, applications kept apart.</summary>
    public class ProtocolV4Tests {
        private static NetConfig Config(KeyExchangeMode mode) {
            var config = new NetConfig { EventDelivery = EventDelivery.Immediate, ConnectTimeoutMs = 5000 };
            if (mode != KeyExchangeMode.None) {
                config.Encryption = new EncryptionConfig { Mode = mode };
                if (mode == KeyExchangeMode.PresharedKey) {
                    config.Encryption.PresharedKey = Encoding.ASCII.GetBytes("0123456789abcdef0123456789abcdef");
                }
            }
            config.Channels[0] = new ChannelConfig(TransportType.Tcp, encryption: mode != KeyExchangeMode.None);
            return config;
        }

        [Theory]
        [InlineData(KeyExchangeMode.PresharedKey)]
        [InlineData(KeyExchangeMode.Ecdh)]
        public async Task A_Recorded_Session_Sent_Again_Is_Not_Accepted(KeyExchangeMode mode) {
            // Both sides put fresh randomness into every session's keys, so bytes captured
            // from one session mean nothing in another.
            using var server = new NetServer(Config(mode));
            var seen = new List<string>();
            int opened = 0;
            server.ConnectionOpened += _ => Interlocked.Increment(ref opened);
            server.DataReceived += (connection, channel, data) => {
                lock (seen) {
                    seen.Add(Encoding.UTF8.GetString(data));
                }
            };
            int serverPort = await server.StartAsync(0);

            // A pass-through between client and server that keeps what the client sent.
            var recorded = new List<byte[]>();
            var listener = new TcpListener(IPAddress.Loopback, 0);
            listener.Start();
            int relayPort = ((IPEndPoint)listener.LocalEndpoint).Port;
            _ = Task.Run(async () => {
                using var fromClient = await listener.AcceptTcpClientAsync();
                using var toServer = new TcpClient();
                await toServer.ConnectAsync(IPAddress.Loopback, serverPort);
                var up = Task.Run(async () => {
                    var buffer = new byte[65536];
                    int read;
                    while ((read = await fromClient.GetStream().ReadAsync(buffer)) > 0) {
                        lock (recorded) {
                            recorded.Add(buffer.AsSpan(0, read).ToArray());
                        }
                        await toServer.GetStream().WriteAsync(buffer.AsMemory(0, read));
                    }
                });
                var down = toServer.GetStream().CopyToAsync(fromClient.GetStream());
                await Task.WhenAny(up, down);
            });

            using (var client = new NetClient(Config(mode))) {
                await client.ConnectAsync(new IPEndPoint(IPAddress.Loopback, relayPort));
                await client.SendAsync(0, Encoding.UTF8.GetBytes("once"));
                Assert.True(await Eventually.HoldsAsync(() => seen.Count == 1));
                client.Disconnect();
            }
            Assert.True(await Eventually.HoldsAsync(() => server.ConnectionCount == 0));
            listener.Stop();

            using var second = new TcpClient();
            await second.ConnectAsync(IPAddress.Loopback, serverPort);
            byte[][] chunks;
            lock (recorded) {
                chunks = recorded.ToArray();
            }
            foreach (var chunk in chunks) {
                try {
                    await second.GetStream().WriteAsync(chunk);
                }
                catch (Exception ex) when (ex is System.IO.IOException || ex is SocketException) {
                    break; // the server already dropped the link
                }
                await Task.Delay(30);
            }
            await Task.Delay(500);

            Assert.Equal(new[] { "once" }, seen.ToArray());
            Assert.Equal(1, Volatile.Read(ref opened));
        }

        [Theory]
        [InlineData("game-a", 1, "game-b", 1)]
        [InlineData("game-a", 1, "game-a", 2)]
        [InlineData("", 0, "game-a", 0)]
        public async Task Another_Application_Or_Version_Is_Refused(string serverApp, int serverVersion, string clientApp, int clientVersion) {
            var serverConfig = Config(KeyExchangeMode.None);
            serverConfig.AppId = serverApp;
            serverConfig.AppProtocolVersion = (ushort)serverVersion;
            using var server = new NetServer(serverConfig);
            int port = await server.StartAsync(0);

            var clientConfig = Config(KeyExchangeMode.None);
            clientConfig.AppId = clientApp;
            clientConfig.AppProtocolVersion = (ushort)clientVersion;
            using var client = new NetClient(clientConfig);
            var ex = await Assert.ThrowsAsync<ConnectionRejectedException>(() => client.ConnectAsync("127.0.0.1", port));
            Assert.Equal(ConnectionRejection.ApplicationMismatch, ex.Rejection);

            clientConfig.AppId = serverApp;
            clientConfig.AppProtocolVersion = (ushort)serverVersion;
            using var matching = new NetClient(clientConfig);
            await matching.ConnectAsync("127.0.0.1", port);
            Assert.True(matching.IsConnected);
        }

        [Fact]
        public void Hello_From_A_Newer_Peer_Is_Understood() {
            // A later release may set feature bits and append extensions this build has never
            // heard of. They must be skipped, not rejected.
            var hello = PeerProtocol.EncodeHello(42, 7, KeyExchangeMode.Ecdh, 3, new byte[32], cookie: null, appId: 5, appVersion: 6,
                                                 features: 0xFFFF_FFFF);
            var extensions = new byte[] { 0x7F, 0, 3, 1, 2, 3, 0x80, 0, 0 }; // two unknown entries
            var extended = new byte[hello.Length + extensions.Length];
            hello.CopyTo(extended, 0);
            extensions.CopyTo(extended, hello.Length);
            BinaryPrimitives.WriteUInt16BigEndian(extended.AsSpan(hello.Length - 2), (ushort)extensions.Length);

            Assert.True(PeerProtocol.TryParseHello(extended, 0, extended.Length, out var parsed));
            Assert.Equal(42, parsed.Token);
            Assert.Equal(5UL, parsed.AppId);
            Assert.Equal(6, parsed.AppVersion);
            Assert.Equal(0xFFFF_FFFF, parsed.Features);
            Assert.Equal(32, parsed.PublicKeyLength);

            // An extension block that runs past the message is malformed.
            BinaryPrimitives.WriteUInt16BigEndian(extended.AsSpan(hello.Length - 2), (ushort)(extensions.Length + 1));
            Assert.False(PeerProtocol.TryParseHello(extended, 0, extended.Length, out _));
        }

        [Fact]
        public void Welcome_From_A_Newer_Peer_Is_Understood() {
            var welcome = PeerProtocol.EncodeWelcome(9, CipherSuite.AesCbcHmacSha256, features: 0, new byte[32], new byte[32]);
            var extended = new byte[welcome.Length + 4];
            welcome.CopyTo(extended, 0);
            new byte[] { 0x55, 0, 1, 9 }.CopyTo(extended, welcome.Length);
            BinaryPrimitives.WriteUInt16BigEndian(extended.AsSpan(welcome.Length - 2), 4);

            Assert.True(PeerProtocol.TryParseWelcome(extended, 0, extended.Length, out var parsed));
            Assert.Equal(9, parsed.ConnectionId);
            Assert.Equal(32, parsed.PublicKey.Length);
            Assert.Equal(32, parsed.IdentityKey.Length);
        }

        [Fact]
        public async Task Unknown_Feature_Bits_Are_Not_Agreed_On() {
            // The server answers with the features both sides know: bits it never heard of drop out.
            using var server = new NetServer(Config(KeyExchangeMode.None));
            int port = await server.StartAsync(0);
            var config = Config(KeyExchangeMode.None);

            using var raw = new TcpClient();
            await raw.ConnectAsync(IPAddress.Loopback, port);
            var hello = PeerProtocol.EncodeHello(77, config.ComputeConfigHash(config.BuildChannelTable()), KeyExchangeMode.None, 0,
                                                 null, null, features: 0xFFFF_FFFF);
            var frame = new byte[5 + hello.Length];
            frame[0] = 0x10; // transport Data frame
            BinaryPrimitives.WriteInt32BigEndian(frame.AsSpan(1), hello.Length);
            hello.CopyTo(frame, 5);
            await raw.GetStream().WriteAsync(frame);

            var reply = new byte[256];
            int total = 0;
            using var timeout = new CancellationTokenSource(5000);
            while (total < 5 || total < 5 + BinaryPrimitives.ReadInt32BigEndian(reply.AsSpan(1))) {
                int read = await raw.GetStream().ReadAsync(reply.AsMemory(total), timeout.Token);
                Assert.True(read > 0, "the server closed the link instead of answering");
                total += read;
                if (total >= 1 && reply[0] != 0x10) { // skip keepalive frames (9 bytes)
                    Array.Copy(reply, 9, reply, 0, total - 9);
                    total -= 9;
                }
            }
            int length = BinaryPrimitives.ReadInt32BigEndian(reply.AsSpan(1));
            Assert.True(PeerProtocol.TryParseWelcome(reply, 5, length, out var welcome));
            Assert.Equal(PeerProtocol.LocalFeatures, welcome.Features);
        }
    }
}
