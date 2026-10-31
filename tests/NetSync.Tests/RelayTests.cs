using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using NetSync.Peers;
using NetSync.Relay;
using NetSync.Transports;
using Xunit;
using Xunit.Abstractions;

namespace NetSync.Tests {
    /// <summary>Stage 19: relay server, relay transports, UDP hole punching under simulated NATs.</summary>
    public class RelayTests {
        private readonly ITestOutputHelper _output;

        public RelayTests(ITestOutputHelper output) => _output = output;

        // ------------------------------------------------------------ NAT simulations

        /// <summary>
        /// Port-restricted cone NAT: one external port for everything; a datagram gets in only
        /// from an endpoint we've already sent to. Hole punching works through it.
        /// </summary>
        private sealed class ConeNatSocket : IDatagramSocket {
            private readonly DatagramSocket _inner;
            private readonly ConcurrentDictionary<IPEndPoint, bool> _contacted = new ConcurrentDictionary<IPEndPoint, bool>();
            public int Dropped;

            public ConeNatSocket(AddressFamily family) => _inner = new DatagramSocket(family);

            public void SendTo(byte[] buffer, int offset, int count, IPEndPoint remote) {
                _contacted[remote] = true;
                _inner.SendTo(buffer, offset, count, remote);
            }

            public int ReceiveFrom(byte[] buffer, out IPEndPoint remote) {
                while (true) {
                    int received = _inner.ReceiveFrom(buffer, out remote);
                    if (_contacted.ContainsKey(remote)) {
                        return received;
                    }
                    Interlocked.Increment(ref Dropped); // unsolicited: the NAT drops it
                }
            }

            public void Dispose() => _inner.Dispose();
        }

        /// <summary>
        /// Symmetric NAT: a new external port for every destination, each accepting only that
        /// destination. Peers learn a mapping the other side can't reuse — punching fails.
        /// </summary>
        private sealed class SymmetricNatSocket : IDatagramSocket {
            private readonly AddressFamily _family;
            private readonly ConcurrentDictionary<IPEndPoint, DatagramSocket> _mappings = new ConcurrentDictionary<IPEndPoint, DatagramSocket>();
            private readonly BlockingCollection<(byte[] Data, IPEndPoint From)> _inbox = new BlockingCollection<(byte[], IPEndPoint)>();
            private volatile bool _disposed;

            public SymmetricNatSocket(AddressFamily family) => _family = family;

            public void SendTo(byte[] buffer, int offset, int count, IPEndPoint remote) {
                var mapping = _mappings.GetOrAdd(remote, destination => {
                    var socket = new DatagramSocket(_family);
                    new Thread(() => Pump(socket, destination)) { IsBackground = true }.Start();
                    return socket;
                });
                mapping.SendTo(buffer, offset, count, remote);
            }

            private void Pump(DatagramSocket socket, IPEndPoint destination) {
                var buffer = new byte[65536];
                try {
                    while (!_disposed) {
                        int received = socket.ReceiveFrom(buffer, out var from);
                        if (from.Equals(destination)) { // this mapping only answers its own destination
                            _inbox.Add((buffer.AsSpan(0, received).ToArray(), from));
                        }
                    }
                }
                catch (Exception) {
                    // disposed
                }
            }

            public int ReceiveFrom(byte[] buffer, out IPEndPoint remote) {
                if (_disposed) {
                    throw new ObjectDisposedException(nameof(SymmetricNatSocket));
                }
                try {
                    var (data, from) = _inbox.Take();
                    data.CopyTo(buffer, 0);
                    remote = from;
                    return data.Length;
                }
                catch (InvalidOperationException) {
                    throw new ObjectDisposedException(nameof(SymmetricNatSocket)); // completed on dispose
                }
            }

            public void Dispose() {
                _disposed = true;
                _inbox.CompleteAdding();
                foreach (var socket in _mappings.Values) {
                    socket.Dispose();
                }
            }
        }

        // ------------------------------------------------------------ harness

        private static NetConfig Config() {
            // Generous fallback window: a loaded test run must not push UDP into fallback.
            var config = new NetConfig { EventDelivery = EventDelivery.Immediate, ConnectTimeoutMs = 15000,
                                         TransportFallbackTimeoutMs = 7000, PingIntervalMs = 200 };
            config.Channels[0] = new ChannelConfig(TransportType.Tcp);
            config.Channels[1] = new ChannelConfig(TransportType.Udp, ReliabilityMode.ReliableOrdered);
            config.Channels[2] = new ChannelConfig(TransportType.Udp);
            return config;
        }

        private sealed class Setup : IDisposable {
            public NetRelayServer Relay = null!;
            public NetServer Host = null!;
            public int HostPort;
            public string RoomId = RelayRoom.NewRoomCode();
            public NetClient Client = null!;

            public void Dispose() {
                Client?.Dispose();
                Host?.Dispose();
                Relay?.Dispose();
            }
        }

        private static async Task<Setup> StartAsync(bool allowDirect, Func<AddressFamily, IDatagramSocket>? hostNat = null,
                                                    Func<AddressFamily, IDatagramSocket>? clientNat = null, KeyExchangeMode mode = KeyExchangeMode.None) {
            var setup = new Setup { Relay = new NetRelayServer() };
            int relayPort = await setup.Relay.StartAsync(0);
            var relayEndPoint = new IPEndPoint(IPAddress.Loopback, relayPort);

            var hostConfig = Config();
            Secure(hostConfig, mode);
            setup.Host = new NetServer(hostConfig);
            setup.Host.MessageReceived += (connection, channel, data) => _ = connection.SendAsync(channel, data);
            var hostRoom = new RelayRoom(setup.RoomId) { RelayEndPoint = relayEndPoint, AllowDirect = allowDirect };
            if (hostNat != null) {
                hostRoom.SocketFactory = hostNat;
            }
            setup.Host.AddListener(TransportType.Tcp, hostRoom.Tcp, 0);
            setup.Host.AddListener(TransportType.Udp, hostRoom.Udp, 0);
            setup.HostPort = await setup.Host.StartAsync(0);

            var clientConfig = Config();
            Secure(clientConfig, mode);
            var clientRoom = new RelayRoom(setup.RoomId) { AllowDirect = allowDirect };
            if (clientNat != null) {
                clientRoom.SocketFactory = clientNat;
            }
            clientConfig.TransportFactories[TransportType.Tcp] = clientRoom.Tcp;
            clientConfig.TransportFactories[TransportType.Udp] = clientRoom.Udp;
            setup.Client = new NetClient(clientConfig);
            await setup.Client.ConnectAsync(relayEndPoint);
            return setup;
        }

        private static void Secure(NetConfig config, KeyExchangeMode mode) {
            if (mode == KeyExchangeMode.PresharedKey) {
                config.Encryption = new EncryptionConfig { Mode = mode, PresharedKey = System.Text.Encoding.ASCII.GetBytes("relay-tests-psk-relay-tests-psk!") };
            }
        }

        /// <summary>
        /// UDP bytes the relay forwarded while <paramref name="traffic"/> ran, once punching has had
        /// time to finish: it runs in the background, and a loaded test machine slows it down —
        /// so measure again (up to five rounds) while the result is still over the limit.
        /// </summary>
        private static async Task<long> RelayedUdpOnceDirectAsync(NetRelayServer relay, Func<Task> traffic, long limit) {
            long relayed = long.MaxValue;
            for (int round = 0; round < 5 && relayed >= limit; round++) {
                await Task.Delay(round == 0 ? 1500 : 1000);
                long before = relay.RelayedUdpBytes;
                await traffic();
                relayed = relay.RelayedUdpBytes - before;
            }
            return relayed;
        }

        /// <summary>Sends on every channel and waits for every echo.</summary>
        private static async Task EchoAllChannelsAsync(NetClient client, int perChannel = 20, int size = 900) {
            var received = new ConcurrentDictionary<byte, int>();
            void OnData(byte channel, ReadOnlyMemory<byte> data) => received.AddOrUpdate(channel, 1, (_, count) => count + 1);
            client.MessageReceived += OnData;
            try {
                foreach (byte channel in new byte[] { 0, 1, 2 }) {
                    for (int i = 0; i < perChannel; i++) {
                        await client.SendAsync(channel, new byte[size]);
                    }
                }
                // Channel 2 is unreliable: on loopback nothing is lost, but don't demand perfection.
                await WaitUntilAsync(() => received.GetValueOrDefault((byte)0) == perChannel &&
                                           received.GetValueOrDefault((byte)1) == perChannel &&
                                           received.GetValueOrDefault((byte)2) >= perChannel * 9 / 10);
            }
            finally {
                client.MessageReceived -= OnData;
            }
        }

        // ------------------------------------------------------------ tests

        [Fact]
        public async Task Relay_Carries_Every_Channel_And_The_Host_Still_Serves_Lan_Clients() {
            using var setup = await StartAsync(allowDirect: false);
            Assert.False(setup.Client.IsDegraded);
            await EchoAllChannelsAsync(setup.Client);
            Assert.True(setup.Relay.RelayedTcpBytes > 0);
            Assert.True(setup.Relay.RelayedUdpBytes > 0);

            // The same server keeps accepting direct (LAN) clients on its own port.
            using var lan = new NetClient(Config());
            await lan.ConnectAsync("127.0.0.1", setup.HostPort);
            await EchoAllChannelsAsync(lan);
            Assert.Equal(2, setup.Host.ConnectionCount);
        }

        [Fact]
        public async Task Hole_Punching_Takes_Udp_Off_The_Relay_Behind_Cone_Nats() {
            using var setup = await StartAsync(allowDirect: true,
                                               hostNat: family => new ConeNatSocket(family),
                                               clientNat: family => new ConeNatSocket(family));
            long relayed = await RelayedUdpOnceDirectAsync(setup.Relay, () => EchoAllChannelsAsync(setup.Client, perChannel: 100, size: 1000), limit: 20_000);
            _output.WriteLine($"UDP bytes through the relay during 200 KB of UDP echo: {relayed}");
            Assert.True(relayed < 20_000, $"{relayed} UDP bytes still went through the relay");
        }

        [Fact]
        public async Task Symmetric_Nats_Fall_Back_To_The_Relay() {
            using var setup = await StartAsync(allowDirect: true,
                                               hostNat: family => new SymmetricNatSocket(family),
                                               clientNat: family => new SymmetricNatSocket(family));
            await Task.Delay(1500);
            long before = setup.Relay.RelayedUdpBytes;
            await EchoAllChannelsAsync(setup.Client, perChannel: 100, size: 1000);
            long relayed = setup.Relay.RelayedUdpBytes - before;
            _output.WriteLine($"UDP bytes through the relay: {relayed}");
            Assert.True(relayed > 300_000, "punching can't work through two symmetric NATs: traffic must stay on the relay");
        }

        [Fact]
        public async Task Encrypted_Connections_Work_End_To_End_Through_The_Relay() {
            using var setup = await StartAsync(allowDirect: true, mode: KeyExchangeMode.PresharedKey);
            await EchoAllChannelsAsync(setup.Client);
            Assert.NotEqual(CipherSuite.None, setup.Client.CipherSuite);
        }

        [Fact]
        public async Task Kicks_And_Host_Shutdown_Reach_Relayed_Clients() {
            using var setup = await StartAsync(allowDirect: false);
            var reason = new TaskCompletionSource<DisconnectReason>(TaskCreationOptions.RunContinuationsAsynchronously);
            setup.Client.Disconnected += r => reason.TrySetResult(r);
            await Assert.Single(setup.Host.Connections).DisconnectAsync();
            Assert.Equal(DisconnectReason.Kicked, await WithTimeout(reason.Task));
        }

        [Fact]
        public async Task Unknown_Rooms_And_Taken_Rooms_Are_Refused() {
            using var relay = new NetRelayServer();
            int port = await relay.StartAsync(0);
            var relayEndPoint = new IPEndPoint(IPAddress.Loopback, port);

            var config = Config();
            var nowhere = new RelayRoom("NO-SUCH-ROOM");
            config.TransportFactories[TransportType.Tcp] = nowhere.Tcp;
            config.TransportFactories[TransportType.Udp] = nowhere.Udp;
            using var client = new NetClient(config);
            await Assert.ThrowsAnyAsync<Exception>(() => client.ConnectAsync(relayEndPoint));

            using var first = new NetServer(Config());
            var room = new RelayRoom("TAKEN") { RelayEndPoint = relayEndPoint };
            first.AddListener(TransportType.Tcp, room.Tcp, 0);
            first.AddListener(TransportType.Udp, room.Udp, 0);
            await first.StartAsync(0);
            Assert.Equal(1, relay.RoomCount);

            using var second = new NetServer(Config());
            var same = new RelayRoom("TAKEN") { RelayEndPoint = relayEndPoint };
            second.AddListener(TransportType.Tcp, same.Tcp, 0);
            var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => second.StartAsync(0));
            Assert.Contains("taken", ex.Message);
        }

        [Fact]
        public async Task Udp_Only_Configuration_Takes_All_Traffic_Off_The_Relay() {
            // The stream slot can't be hole-punched; a config without TCP channels has none.
            static NetConfig UdpOnly() {
                var config = new NetConfig { EventDelivery = EventDelivery.Immediate, ConnectTimeoutMs = 15000, PingIntervalMs = 200 };
                config.Channels[0] = new ChannelConfig(TransportType.Udp, ReliabilityMode.ReliableOrdered);
                config.Channels[1] = new ChannelConfig(TransportType.Udp);
                return config;
            }
            using var relay = new NetRelayServer();
            var relayEndPoint = new IPEndPoint(IPAddress.Loopback, await relay.StartAsync(0));
            string roomId = RelayRoom.NewRoomCode();

            using var host = new NetServer(UdpOnly());
            host.MessageReceived += (connection, channel, data) => _ = connection.SendAsync(channel, data);
            var hostRoom = new RelayRoom(roomId) { RelayEndPoint = relayEndPoint, SocketFactory = family => new ConeNatSocket(family) };
            host.AddListener(TransportType.Udp, hostRoom.Udp, 0);
            await host.StartAsync(0);

            var clientConfig = UdpOnly();
            var clientRoom = new RelayRoom(roomId) { SocketFactory = family => new ConeNatSocket(family) };
            clientConfig.TransportFactories[TransportType.Udp] = clientRoom.Udp;
            using var client = new NetClient(clientConfig);
            await client.ConnectAsync(relayEndPoint);

            int echoed = 0;
            client.MessageReceived += (channel, data) => Interlocked.Increment(ref echoed);
            long relayed = await RelayedUdpOnceDirectAsync(relay, async () => {
                int target = Volatile.Read(ref echoed) + 100;
                for (int i = 0; i < 100; i++) {
                    await client.SendAsync(0, new byte[1000]);
                }
                await WaitUntilAsync(() => Volatile.Read(ref echoed) >= target);
            }, limit: 10_000);
            _output.WriteLine($"through the relay: {relayed} UDP B, {relay.RelayedTcpBytes} TCP B");
            Assert.Equal(0, relay.RelayedTcpBytes);
            Assert.True(relayed < 10_000, "UDP should flow on the direct path");
        }

        [Fact]
        public async Task Host_Gets_Back_Onto_A_Restarted_Relay_By_Itself() {
            var setup = await StartAsync(allowDirect: true);
            try {
                await EchoAllChannelsAsync(setup.Client);
                var lost = new TaskCompletionSource<DisconnectReason>(TaskCreationOptions.RunContinuationsAsynchronously);
                setup.Client.Disconnected += reason => lost.TrySetResult(reason);

                int port = setup.Relay.Port;
                setup.Relay.Dispose();
                await WithTimeout(lost.Task); // relayed clients notice the relay is gone
                setup.Relay = await StartRelayOnAsync(port);

                // No action on the host: its room comes back, and new clients get through on every channel.
                var config = Config();
                var room = new RelayRoom(setup.RoomId);
                config.TransportFactories[TransportType.Tcp] = room.Tcp;
                config.TransportFactories[TransportType.Udp] = room.Udp;
                using var client = new NetClient(config);
                await WaitUntilAsync(() => setup.Relay.RoomCount == 1);
                await client.ConnectAsync(new IPEndPoint(IPAddress.Loopback, port));
                await EchoAllChannelsAsync(client);
            }
            finally {
                setup.Dispose();
            }
        }

        /// <summary>Restarts a relay on a fixed port (the OS may need a moment to release it).</summary>
        private static async Task<NetRelayServer> StartRelayOnAsync(int port) {
            for (int attempt = 0; ; attempt++) {
                var relay = new NetRelayServer();
                try {
                    await relay.StartAsync(port);
                    return relay;
                }
                catch (SocketException) when (attempt < 50) {
                    relay.Dispose();
                    await Task.Delay(100);
                }
            }
        }

        [Fact]
        public void Room_Codes_Are_Readable() {
            var code = RelayRoom.NewRoomCode();
            Assert.Equal(6, code.Length);
            Assert.DoesNotContain(code, c => "01OI".Contains(c));
        }

        private static async Task WaitUntilAsync(Func<bool> condition, int timeoutMs = 15_000) {
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
