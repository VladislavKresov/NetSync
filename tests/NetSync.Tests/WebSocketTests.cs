using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Net;
using System.Net.WebSockets;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NetSync.Peers;
using NetSync.Transports;
using NetSync.Transports.WebSockets;
using Xunit;

namespace NetSync.Tests {
    /// <summary>Stage 18: several listeners per slot, WebSocket transport (ws/wss), UDP-less clients.</summary>
    public class WebSocketTests {
        private static NetConfig Config() {
            // Generous fallback window: under a loaded parallel test run the UDP handshake of the
            // native client can take seconds, and these tests assert it did NOT degrade.
            var config = new NetConfig { EventDelivery = EventDelivery.Immediate, ConnectTimeoutMs = 15000,
                                         TransportFallbackTimeoutMs = 7000, PingIntervalMs = 200 };
            config.Channels[0] = new ChannelConfig(TransportType.Tcp);
            config.Channels[1] = new ChannelConfig(TransportType.Udp, ReliabilityMode.ReliableOrdered);
            config.Channels[2] = new ChannelConfig(TransportType.Udp, ReliabilityMode.UnreliableSequenced);
            return config;
        }

        /// <summary>A "browser": no sockets for UDP at all.</summary>
        private sealed class NoUdpFactory : ITransportFactory {
            public ITransport CreateClient(TransportSettings settings) => throw new PlatformNotSupportedException("no UDP in this sandbox");
            public IServerTransport CreateServer(TransportSettings settings) => throw new NotSupportedException();
        }

        private static NetConfig BrowserConfig(WebSocketTransportFactory webSocket) {
            var config = Config();
            config.TransportFactories[TransportType.Tcp] = webSocket;
            config.TransportFactories[TransportType.Udp] = new NoUdpFactory();
            return config;
        }

        private static async Task<(NetServer Server, int NativePort, NetListener WebSocket)> StartEchoServerAsync(WebSocketTransportFactory? factory = null) {
            var server = new NetServer(Config());
            server.MessageReceived += (connection, channel, data) => _ = connection.SendAsync(channel, data);
            var listener = server.AddListener(TransportType.Tcp, factory ?? new WebSocketTransportFactory(), 0);
            int port = await server.StartAsync(0);
            return (server, port, listener);
        }

        [Fact]
        public async Task Native_And_WebSocket_Clients_Share_One_Server() {
            var (server, nativePort, webSocket) = await StartEchoServerAsync();
            using var owned = server;
            Assert.NotEqual(0, webSocket.Port);

            using var native = new NetClient(Config());
            await native.ConnectAsync("127.0.0.1", nativePort);
            using var browser = new NetClient(BrowserConfig(new WebSocketTransportFactory()));
            await browser.ConnectAsync("127.0.0.1", webSocket.Port);

            Assert.False(native.IsDegraded);
            Assert.True(browser.IsDegraded); // its UDP channels ride the WebSocket
            Assert.Equal(2, server.ConnectionCount);

            foreach (var client in new[] { native, browser }) {
                var echoes = new ConcurrentDictionary<byte, byte[]>();
                client.DataReceived += (channel, data) => echoes[channel] = data;
                foreach (byte channel in new byte[] { 0, 1, 2 }) {
                    await client.SendAsync(channel, new[] { channel, (byte)42 });
                }
                await WaitUntilAsync(() => echoes.Count == 3);
                Assert.All(echoes, kvp => Assert.Equal(new[] { kvp.Key, (byte)42 }, kvp.Value));
            }
            await server.StopAsync();
        }

        [Fact]
        public async Task Large_Messages_Cross_A_WebSocket_Intact() {
            var (server, _, webSocket) = await StartEchoServerAsync();
            using var owned = server;
            using var client = new NetClient(BrowserConfig(new WebSocketTransportFactory()));
            var echo = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
            client.DataReceived += (channel, data) => echo.TrySetResult(data);
            await client.ConnectAsync("127.0.0.1", webSocket.Port);

            var payload = new byte[3 * 1024 * 1024];
            new Random(9).NextBytes(payload);
            await client.SendAsync(1, payload); // reliable UDP channel, degraded onto the WebSocket, fragmented
            Assert.Equal(payload, await WithTimeout(echo.Task, 30_000));
            await server.StopAsync();
        }

        [Fact]
        public async Task Secure_WebSocket_With_A_Pinned_Certificate() {
            using var certificate = CreateSelfSignedCertificate();
            var serverFactory = new WebSocketTransportFactory { ServerCertificate = certificate };
            var (server, _, webSocket) = await StartEchoServerAsync(serverFactory);
            using var owned = server;

            var clientFactory = new WebSocketTransportFactory {
                UseTls = true,
                // A development certificate: trust exactly this one.
                RemoteCertificateValidation = (_, presented, _, _) =>
                    presented != null && presented.GetCertHashString() == certificate.GetCertHashString()
            };
            using var client = new NetClient(BrowserConfig(clientFactory));
            var echo = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
            client.DataReceived += (channel, data) => echo.TrySetResult(data);
            await client.ConnectAsync("127.0.0.1", webSocket.Port);
            await client.SendAsync(0, Encoding.UTF8.GetBytes("over TLS"));
            Assert.Equal("over TLS", Encoding.UTF8.GetString(await WithTimeout(echo.Task)));

            // And a client that doesn't trust the certificate can't get in.
            using var untrusting = new NetClient(BrowserConfig(new WebSocketTransportFactory { UseTls = true }));
            await Assert.ThrowsAnyAsync<Exception>(() => untrusting.ConnectAsync("127.0.0.1", webSocket.Port));
            await server.StopAsync();
        }

        [Fact]
        public async Task Origin_Allow_List_Is_Enforced() {
            var factory = new WebSocketTransportFactory();
            factory.AllowedOrigins.Add("https://game.example");
            var (server, _, webSocket) = await StartEchoServerAsync(factory);
            using var owned = server;
            var uri = new Uri($"ws://127.0.0.1:{webSocket.Port}/netsync");

            using (var foreign = new ClientWebSocket()) {
                foreign.Options.SetRequestHeader("Origin", "https://evil.example");
                await Assert.ThrowsAsync<WebSocketException>(() => foreign.ConnectAsync(uri, CancellationToken.None));
            }
            using (var allowed = new ClientWebSocket()) {
                allowed.Options.SetRequestHeader("Origin", "https://game.example");
                await allowed.ConnectAsync(uri, CancellationToken.None);
                Assert.Equal(WebSocketState.Open, allowed.State);
            }
            await server.StopAsync();
        }

        [Fact]
        public async Task Wrong_Path_Is_Refused() {
            var (server, _, webSocket) = await StartEchoServerAsync();
            using var owned = server;
            using var client = new NetClient(BrowserConfig(new WebSocketTransportFactory { Path = "/elsewhere" }));
            await Assert.ThrowsAnyAsync<Exception>(() => client.ConnectAsync("127.0.0.1", webSocket.Port));
            await server.StopAsync();
        }

        [Fact]
        public async Task Big_Messages_Before_The_Handshake_Get_The_Peer_Dropped() {
            var (server, _, webSocket) = await StartEchoServerAsync();
            using var owned = server;
            using var raw = new ClientWebSocket();
            await raw.ConnectAsync(new Uri($"ws://127.0.0.1:{webSocket.Port}/netsync"), CancellationToken.None);
            var big = new byte[8 * 1024];
            big[0] = (byte)PacketType.Data;
            await raw.SendAsync(new ArraySegment<byte>(big), WebSocketMessageType.Binary, true, CancellationToken.None);

            var receive = raw.ReceiveAsync(new ArraySegment<byte>(new byte[64]), CancellationToken.None);
            // The server aborts the connection instead of buffering an unauthenticated stranger's data.
            await Assert.ThrowsAnyAsync<Exception>(async () => {
                var completed = await Task.WhenAny(receive, Task.Delay(5000));
                Assert.Same(receive, completed);
                var result = await receive;
                if (result.MessageType == WebSocketMessageType.Close) {
                    throw new WebSocketException("closed");
                }
            });
            await server.StopAsync();
        }

        [Fact]
        public void Add_Listener_Validates_Its_Slot_And_Timing() {
            var config = new NetConfig();
            config.Channels[0] = new ChannelConfig(TransportType.Tcp);
            using var server = new NetServer(config);
            Assert.Throws<ArgumentException>(() => server.AddListener(TransportType.Udp, new WebSocketTransportFactory(), 0));
        }

        private static X509Certificate2 CreateSelfSignedCertificate() {
            using var key = RSA.Create(2048);
            var request = new CertificateRequest("CN=localhost", key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
            using var ephemeral = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(1));
            // Round-trip through PFX: SslStream on Windows can't use an ephemeral key directly.
#pragma warning disable SYSLIB0057 // X509Certificate2 constructor: fine for a test certificate
            return new X509Certificate2(ephemeral.Export(X509ContentType.Pfx));
#pragma warning restore SYSLIB0057
        }

        private static async Task WaitUntilAsync(Func<bool> condition, int timeoutMs = 10_000) {
            var deadline = DateTime.UtcNow.AddMilliseconds(timeoutMs);
            while (!condition()) {
                if (DateTime.UtcNow > deadline) {
                    throw new TimeoutException("Condition not met in time");
                }
                await Task.Delay(10);
            }
        }

        private static async Task<T> WithTimeout<T>(Task<T> task, int timeoutMs = 10_000) {
            Assert.Same(task, await Task.WhenAny(task, Task.Delay(timeoutMs)));
            return await task;
        }
    }
}
