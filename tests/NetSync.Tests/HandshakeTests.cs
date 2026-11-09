using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NetSync.Peers;
using NetSync.Transports;
using Xunit;

namespace NetSync.Tests {
    /// <summary>Stage 16: protocol v3 — server identity, cipher suites, Join and approval, address proof.</summary>
    public class HandshakeTests {
        private static readonly byte[] Psk = Encoding.ASCII.GetBytes("handshake-tests-psk-32-bytes!!!!");

        private static NetConfig Config(KeyExchangeMode mode = KeyExchangeMode.None, bool udp = true) {
            var config = new NetConfig { EventDelivery = EventDelivery.Immediate, ConnectTimeoutMs = 5000, PingIntervalMs = 200 };
            config.Channels[0] = new ChannelConfig(TransportType.Tcp, encryption: mode != KeyExchangeMode.None);
            if (udp) {
                config.Channels[1] = new ChannelConfig(TransportType.Udp, ReliabilityMode.ReliableOrdered);
            }
            config.Encryption = new EncryptionConfig {
                Mode = mode,
                PresharedKey = mode == KeyExchangeMode.PresharedKey ? Psk : null
            };
            return config;
        }

        private static CipherSuite ExpectedSuite => CipherSuites.GcmSupported ? CipherSuite.AesGcm : CipherSuite.AesCbcHmacSha256;

        // ------------------------------------------------------------ identity

        [Fact]
        public async Task Pinned_Server_Identity_Is_Verified_And_The_Best_Suite_Negotiated() {
            using var identity = NetIdentity.Generate();
            var serverConfig = Config(KeyExchangeMode.Ecdh);
            serverConfig.Encryption.ServerIdentity = identity;
            using var server = new NetServer(serverConfig);
            server.MessageReceived += (connection, channel, data) => _ = connection.SendAsync(channel, data);
            int port = await server.StartAsync(0);

            var clientConfig = Config(KeyExchangeMode.Ecdh);
            clientConfig.Encryption.TrustedServerKeys.Add(identity.PublicKey);
            using var client = new NetClient(clientConfig);
            var echo = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
            client.DataReceived += (channel, data) => echo.TrySetResult(data);
            await client.ConnectAsync("127.0.0.1", port);

            Assert.Equal(identity.PublicKey, client.ServerIdentityKey);
            Assert.Equal(ExpectedSuite, client.CipherSuite);
            Assert.Equal(ExpectedSuite, Assert.Single(server.Connections).CipherSuite);
            await client.SendAsync(0, new byte[] { 7, 8, 9 });
            Assert.Equal(new byte[] { 7, 8, 9 }, await WithTimeout(echo.Task));
            await server.StopAsync();
        }

        [Fact]
        public async Task A_Server_Without_The_Pinned_Key_Is_Refused() {
            using var genuine = NetIdentity.Generate();
            using var impostor = NetIdentity.Generate();
            var serverConfig = Config(KeyExchangeMode.Ecdh);
            serverConfig.Encryption.ServerIdentity = impostor;
            using var server = new NetServer(serverConfig);
            int opened = 0;
            server.ConnectionOpened += _ => Interlocked.Increment(ref opened);
            int port = await server.StartAsync(0);

            var clientConfig = Config(KeyExchangeMode.Ecdh);
            clientConfig.Encryption.TrustedServerKeys.Add(genuine.PublicKey);
            using var client = new NetClient(clientConfig);
            var ex = await Assert.ThrowsAsync<ConnectionRejectedException>(() => client.ConnectAsync("127.0.0.1", port));
            Assert.Equal(ConnectionRejection.ServerIdentity, ex.Rejection);
            Assert.Contains(impostor.Fingerprint, ex.Message);
            Assert.Equal(0, Volatile.Read(ref opened));

            // A server with no identity at all is refused just the same.
            await server.StopAsync();
            using var anonymous = new NetServer(Config(KeyExchangeMode.Ecdh));
            port = await anonymous.StartAsync(0);
            ex = await Assert.ThrowsAsync<ConnectionRejectedException>(() => client.ConnectAsync("127.0.0.1", port));
            Assert.Equal(ConnectionRejection.ServerIdentity, ex.Rejection);
            await anonymous.StopAsync();
        }

        [Fact]
        public async Task A_Man_In_The_Middle_Swapping_The_Ecdh_Key_Is_Detected() {
            // The attacker sits on the path and replaces the server's ECDH key in Welcome with
            // its own (here: a corrupted one) — the classic attack on unauthenticated ECDH.
            // The identity signature covers both ECDH keys, so the client notices.
            using var identity = NetIdentity.Generate();
            var network = new InMemoryNetwork();
            var serverConfig = Config(KeyExchangeMode.Ecdh, udp: false);
            serverConfig.TransportFactories[TransportType.Tcp] = network.Factory;
            serverConfig.Encryption.ServerIdentity = identity;
            using var server = new NetServer(serverConfig);
            int port = await server.StartAsync(0);

            var clientConfig = Config(KeyExchangeMode.Ecdh, udp: false);
            clientConfig.TransportFactories[TransportType.Tcp] = new TamperingFactory(network.Factory, packet => {
                if (packet.Length > 12 && packet[0] == PeerProtocol.MsgWelcome) {
                    packet[11] ^= 0x5A; // first byte of the server's ECDH public key
                }
                return packet;
            });
            clientConfig.Encryption.TrustedServerKeys.Add(identity.PublicKey);
            using var client = new NetClient(clientConfig);

            var ex = await Assert.ThrowsAsync<ConnectionRejectedException>(() => client.ConnectAsync("127.0.0.1", port));
            Assert.Equal(ConnectionRejection.ServerIdentity, ex.Rejection);
            await server.StopAsync();
        }

        [Fact]
        public void Identity_Survives_Export_And_Import() {
            using var original = NetIdentity.Generate();
            using var restored = NetIdentity.FromPrivateKey(original.ExportPrivateKey());
            Assert.Equal(original.PublicKey, restored.PublicKey);
            Assert.Equal(original.Fingerprint, restored.Fingerprint);
            Assert.StartsWith("SHA256:", original.Fingerprint);
        }

        [Fact]
        public void Identity_Requires_Ecdh() {
            var config = Config(KeyExchangeMode.PresharedKey);
            config.Encryption.TrustedServerKeys.Add(new byte[NetIdentity.PublicKeySize]);
            Assert.Throws<InvalidOperationException>(() => new NetClient(config));
        }

        // ------------------------------------------------------------ join / approval

        [Theory]
        [InlineData(KeyExchangeMode.None)]
        [InlineData(KeyExchangeMode.PresharedKey)]
        [InlineData(KeyExchangeMode.Ecdh)]
        public async Task Approver_Sees_Connect_Data_And_Can_Refuse_With_A_Reason(KeyExchangeMode mode) {
            using var server = new NetServer(Config(mode));
            server.ConnectionApprover = request => {
                var token = Encoding.UTF8.GetString(request.ConnectData.Span);
                if (token != "let-me-in") {
                    return ConnectionDecision.Reject($"unknown token '{token}'");
                }
                request.Connection.Tag = "player-1";
                return ConnectionDecision.Accept();
            };
            object? openedTag = null;
            server.ConnectionOpened += connection => openedTag = connection.Tag;
            int port = await server.StartAsync(0);

            using var refused = new NetClient(Config(mode));
            var ex = await Assert.ThrowsAsync<ConnectionRejectedException>(() =>
                refused.ConnectAsync("127.0.0.1", port, Encoding.UTF8.GetBytes("guess")));
            Assert.Equal(ConnectionRejection.Application, ex.Rejection);
            Assert.Equal("unknown token 'guess'", ex.Reason);
            Assert.False(refused.IsConnected);

            using var admitted = new NetClient(Config(mode));
            await admitted.ConnectAsync("127.0.0.1", port, Encoding.UTF8.GetBytes("let-me-in"));
            Assert.True(admitted.IsConnected);
            Assert.Equal("player-1", openedTag);
            Assert.Equal(1, server.ConnectionCount);
            await server.StopAsync();
        }

        [Fact]
        public async Task Async_Approver_Can_Take_Its_Time() {
            using var server = new NetServer(Config());
            server.ConnectionApprover = async request => {
                await Task.Delay(300); // e.g. a database lookup
                return ConnectionDecision.Accept();
            };
            int port = await server.StartAsync(0);
            using var client = new NetClient(Config());
            await client.ConnectAsync("127.0.0.1", port);
            Assert.True(client.IsConnected);
            await server.StopAsync();
        }

        [Fact]
        public async Task Throwing_Approver_Refuses_Instead_Of_Hanging() {
            using var server = new NetServer(Config());
            server.ConnectionApprover = _ => throw new InvalidOperationException("db down");
            int port = await server.StartAsync(0);
            using var client = new NetClient(Config());
            var ex = await Assert.ThrowsAsync<ConnectionRejectedException>(() => client.ConnectAsync("127.0.0.1", port));
            Assert.Equal(ConnectionRejection.Application, ex.Rejection);
            await server.StopAsync();
        }

        [Fact]
        public async Task Full_Server_Refuses_New_Clients() {
            var serverConfig = Config();
            serverConfig.MaxConnections = 1;
            using var server = new NetServer(serverConfig);
            int port = await server.StartAsync(0);

            using var first = new NetClient(Config());
            await first.ConnectAsync("127.0.0.1", port);
            using var second = new NetClient(Config());
            var ex = await Assert.ThrowsAsync<ConnectionRejectedException>(() => second.ConnectAsync("127.0.0.1", port));
            Assert.Equal("server full", ex.Reason);
            await server.StopAsync();
        }

        /// <summary>A link whose connect never completes — a firewall that drops the SYN.</summary>
        private sealed class BlackholeTransport : ITransport, IServerTransport, ITransportFactory {
            public bool IsConnected => false;
            public int PingMs => -1;
            public int Port => 0;
            public bool IsRunning => false;
            public event Action? Connected { add { } remove { } }
            public event Action<DisconnectReason>? Disconnected { add { } remove { } }
            public event Action<byte[]>? DataReceived { add { } remove { } }
            public event Action<TransportPeer>? PeerConnected { add { } remove { } }
            public event Action<TransportPeer, DisconnectReason>? PeerDisconnected { add { } remove { } }
            event Action<TransportPeer, byte[]>? IServerTransport.DataReceived { add { } remove { } }
            public Task ConnectAsync(IPEndPoint endpoint, CancellationToken ct = default) => Task.Delay(Timeout.Infinite, ct);
            public ValueTask SendAsync(ReadOnlyMemory<byte> data, CancellationToken ct = default) => default;
            public ValueTask SendAsync(ReadOnlyMemory<byte> prefix, ReadOnlyMemory<byte> payload, CancellationToken ct = default) => default;
            public Task<int> StartAsync(int port, CancellationToken ct = default) => Task.FromResult(0);
            public ValueTask SendAsync(TransportPeer peer, ReadOnlyMemory<byte> data, CancellationToken ct = default) => default;
            public ValueTask SendAsync(TransportPeer peer, ReadOnlyMemory<byte> prefix, ReadOnlyMemory<byte> payload, CancellationToken ct = default) => default;
            public void DisconnectPeer(TransportPeer peer) { }
            public Task StopAsync() => Task.CompletedTask;
            public void Disconnect() { }
            public void Dispose() { }
            public ITransport CreateClient(TransportSettings settings) => this;
            public IServerTransport CreateServer(TransportSettings settings) => this;
        }

        [Fact]
        public async Task Connect_Timeout_Covers_A_Link_That_Never_Connects() {
            var config = Config(udp: false);
            config.ConnectTimeoutMs = 500;
            config.TransportFactories[TransportType.Tcp] = new BlackholeTransport();
            using var client = new NetClient(config);
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            await Assert.ThrowsAsync<TimeoutException>(() => client.ConnectAsync(new IPEndPoint(IPAddress.Loopback, 9)));
            Assert.True(stopwatch.ElapsedMilliseconds >= 400, "gave up before the timeout"); // ThrowsAsync returning at all proves it is honoured
            Assert.False(client.IsConnected);
        }

        [Fact]
        public async Task Simultaneous_Joins_Never_Exceed_MaxConnections() {
            var serverConfig = Config();
            serverConfig.MaxConnections = 3;
            using var server = new NetServer(serverConfig);
            // A slow approver keeps every join in flight at once.
            server.ConnectionApprover = async request => {
                await Task.Delay(200);
                return ConnectionDecision.Accept();
            };
            int port = await server.StartAsync(0);

            var clients = Enumerable.Range(0, 10).Select(_ => new NetClient(Config())).ToArray();
            try {
                var attempts = clients.Select(client => client.ConnectAsync("127.0.0.1", port)).ToArray();
                try {
                    await Task.WhenAll(attempts);
                }
                catch (ConnectionRejectedException) { }
                Assert.Equal(3, attempts.Count(a => a.Status == TaskStatus.RanToCompletion));
                Assert.All(attempts.Where(a => a.IsFaulted), a => Assert.Equal("server full", ((ConnectionRejectedException)a.Exception!.InnerException!).Reason));
                Assert.Equal(3, server.ConnectionCount);

                // Slots come back when connections close.
                clients.First(c => c.IsConnected).Disconnect();
                for (int i = 0; i < 300 && server.ConnectionCount > 2; i++) {
                    await Task.Delay(10);
                }
                using var late = new NetClient(Config());
                await late.ConnectAsync("127.0.0.1", port);
            }
            finally {
                foreach (var client in clients) {
                    client.Dispose();
                }
            }
            await server.StopAsync();
        }

        [Fact]
        public async Task Connect_Data_Is_Encrypted_On_The_Wire() {
            var network = new InMemoryNetwork();
            var captured = new ConcurrentQueue<byte[]>();
            network.ClientToServerTap = packet => captured.Enqueue(packet);
            NetConfig Make() {
                var config = Config(KeyExchangeMode.PresharedKey, udp: false);
                config.TransportFactories[TransportType.Tcp] = network.Factory;
                return config;
            }
            using var server = new NetServer(Make());
            string? seen = null;
            server.ConnectionApprover = request => {
                seen = Encoding.ASCII.GetString(request.ConnectData.Span);
                return ConnectionDecision.Accept();
            };
            int port = await server.StartAsync(0);
            using var client = new NetClient(Make());
            const string secret = "SECRET-LOGIN-TOKEN-0123456789";
            await client.ConnectAsync("127.0.0.1", port, Encoding.ASCII.GetBytes(secret));

            Assert.Equal(secret, seen);
            var needle = Encoding.ASCII.GetBytes("SECRET-LOGIN");
            Assert.DoesNotContain(captured, packet => IndexOf(packet, needle) >= 0);
            await server.StopAsync();
        }

        // ------------------------------------------------------------ address proof

        [Fact]
        public async Task Udp_Only_Server_Makes_Clients_Prove_Their_Address() {
            var config = new NetConfig { EventDelivery = EventDelivery.Immediate, ConnectTimeoutMs = 5000 };
            config.Channels[0] = new ChannelConfig(TransportType.Udp, ReliabilityMode.ReliableOrdered);
            using var server = new NetServer(config);
            server.MessageReceived += (connection, channel, data) => _ = connection.SendAsync(channel, data);
            int port = await server.StartAsync(0);

            // A raw Hello (as a spoofer would send) gets a Retry cookie, not a connection.
            using (var raw = new UdpClient(AddressFamily.InterNetwork)) {
                raw.Connect(IPAddress.Loopback, port);
                var hello = PeerProtocol.EncodeHello(42, config.ComputeConfigHash(config.BuildChannelTable()),
                                                     KeyExchangeMode.None, 0, null, null);
                var datagram = new byte[1 + hello.Length];
                datagram[0] = (byte)PacketType.Data;
                hello.CopyTo(datagram, 1);
                var reply = await TestUdp.RequestAsync(raw, datagram);
                Assert.Equal(PeerProtocol.MsgRetry, reply.Buffer[1]);
            }
            Assert.Equal(0, server.ConnectionCount);

            // A real client answers the cookie and gets in.
            using var client = new NetClient(config);
            var echo = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
            client.DataReceived += (_, data) => echo.TrySetResult(data);
            await client.ConnectAsync("127.0.0.1", port);
            await client.SendAsync(0, new byte[] { 1, 2 });
            Assert.Equal(new byte[] { 1, 2 }, await WithTimeout(echo.Task));
            await server.StopAsync();
        }

        [Fact]
        public async Task Udp_Hello_Cannot_Open_A_Connection_When_Tcp_Exists() {
            var config = Config();
            using var server = new NetServer(config);
            int port = await server.StartAsync(0);

            using var raw = new UdpClient(AddressFamily.InterNetwork);
            raw.Connect(IPAddress.Loopback, port);
            var hello = PeerProtocol.EncodeHello(43, config.ComputeConfigHash(config.BuildChannelTable()), KeyExchangeMode.None, 0, null, null);
            var datagram = new byte[1 + hello.Length];
            datagram[0] = (byte)PacketType.Data;
            hello.CopyTo(datagram, 1);
            await raw.SendAsync(datagram, datagram.Length);

            var receive = raw.ReceiveAsync();
            Assert.NotSame(receive, await Task.WhenAny(receive, Task.Delay(500))); // silence: no Welcome, no work
            await server.StopAsync();
        }

        // ------------------------------------------------------------ helpers

        private static int IndexOf(byte[] haystack, byte[] needle) {
            for (int i = 0; i + needle.Length <= haystack.Length; i++) {
                if (haystack.AsSpan(i, needle.Length).SequenceEqual(needle)) {
                    return i;
                }
            }
            return -1;
        }

        private static async Task<T> WithTimeout<T>(Task<T> task, int timeoutMs = 10_000) {
            Assert.Same(task, await Task.WhenAny(task, Task.Delay(timeoutMs)));
            return await task;
        }

        /// <summary>A path attacker: rewrites what the server sends before the client sees it.</summary>
        private sealed class TamperingFactory : ITransportFactory {
            private readonly ITransportFactory _inner;
            private readonly Func<byte[], byte[]> _serverToClient;

            public TamperingFactory(ITransportFactory inner, Func<byte[], byte[]> serverToClient) {
                _inner = inner;
                _serverToClient = serverToClient;
            }

            public ITransport CreateClient(TransportSettings settings) => new Client(_inner.CreateClient(settings), _serverToClient);
            public IServerTransport CreateServer(TransportSettings settings) => _inner.CreateServer(settings);

            private sealed class Client : ITransport {
                private readonly ITransport _inner;
                public Client(ITransport inner, Func<byte[], byte[]> rewrite) {
                    _inner = inner;
                    _inner.Connected += () => Connected?.Invoke();
                    _inner.Disconnected += reason => Disconnected?.Invoke(reason);
                    _inner.DataReceived += data => DataReceived?.Invoke(rewrite(data));
                }
                public bool IsConnected => _inner.IsConnected;
                public int PingMs => _inner.PingMs;
                public event Action? Connected;
                public event Action<DisconnectReason>? Disconnected;
                public event Action<byte[]>? DataReceived;
                public Task ConnectAsync(IPEndPoint endpoint, CancellationToken ct = default) => _inner.ConnectAsync(endpoint, ct);
                public ValueTask SendAsync(ReadOnlyMemory<byte> data, CancellationToken ct = default) => _inner.SendAsync(data, ct);
                public ValueTask SendAsync(ReadOnlyMemory<byte> prefix, ReadOnlyMemory<byte> payload, CancellationToken ct = default) =>
                    _inner.SendAsync(prefix, payload, ct);
                public void Disconnect() => _inner.Disconnect();
                public void Dispose() => _inner.Dispose();
            }
        }
    }
}
