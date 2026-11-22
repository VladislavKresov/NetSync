using System;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using NetSync.Peers;
using NetSync.Relay;
using NetSync.Transports;
using Xunit;

namespace NetSync.Tests {
    /// <summary>A server can listen on one local address: loopback (machine-local, no firewall prompt) or one adapter.</summary>
    public class BindAddressTests {
        private static NetConfig Config() {
            var config = new NetConfig { EventDelivery = EventDelivery.Immediate };
            config.Channels[0] = new ChannelConfig(TransportType.Tcp);
            config.Channels[1] = new ChannelConfig(TransportType.Udp, ReliabilityMode.ReliableOrdered);
            return config;
        }

        private static async Task EchoAsync(string host, int port) {
            using var client = new NetClient(Config());
            int echoes = 0;
            client.MessageReceived += (_, _) => Interlocked.Increment(ref echoes);
            await client.ConnectAsync(host, port);
            await client.SendAsync(0, new byte[] { 1 });
            await client.SendAsync(1, new byte[] { 2 });
            Assert.True(await Eventually.HoldsAsync(() => Volatile.Read(ref echoes) == 2));
            Assert.False(client.IsDegraded, "UDP should work on the bound address too");
        }

        /// <summary>An IPv4 address of this machine other than loopback, if it has one.</summary>
        private static IPAddress? LanAddress() {
            return NetworkInterface.GetAllNetworkInterfaces()
                .Where(nic => nic.OperationalStatus == OperationalStatus.Up && nic.NetworkInterfaceType != NetworkInterfaceType.Loopback)
                .SelectMany(nic => nic.GetIPProperties().UnicastAddresses)
                .Select(unicast => unicast.Address)
                .FirstOrDefault(address => address.AddressFamily == AddressFamily.InterNetwork && !IPAddress.IsLoopback(address));
        }

        [Fact]
        public async Task A_Loopback_Server_Serves_This_Machine_Only() {
            using var server = new NetServer(Config());
            server.MessageReceived += (connection, channel, data) => _ = connection.SendAsync(channel, data);
            int port = await server.StartAsync(IPAddress.Loopback, 0);
            await EchoAsync("127.0.0.1", port);

            var lan = LanAddress();
            if (lan != null) {
                using var outsider = new TcpClient();
                var refused = await Assert.ThrowsAsync<SocketException>(() => outsider.ConnectAsync(lan, port));
                Assert.Equal(SocketError.ConnectionRefused, refused.SocketErrorCode);
            }
            await server.StopAsync();
        }

        [Fact]
        public async Task The_Config_Can_Choose_The_Address_Too() {
            var config = Config();
            config.BindAddress = IPAddress.Loopback;
            using var server = new NetServer(config);
            server.MessageReceived += (connection, channel, data) => _ = connection.SendAsync(channel, data);
            int port = await server.StartAsync(0);
            await EchoAsync("127.0.0.1", port);
            await server.StopAsync();
        }

        [Fact]
        public async Task IPv6_Loopback_Works() {
            if (!UsabilityTests.CanUseIPv6Loopback()) {
                return; // no usable IPv6 loopback here (some firewalls and VPN clients block ::1)
            }
            using var server = new NetServer(Config());
            server.MessageReceived += (connection, channel, data) => _ = connection.SendAsync(channel, data);
            int port = await server.StartAsync(IPAddress.IPv6Loopback, 0);
            await EchoAsync("::1", port);
            await server.StopAsync();
        }

        [Fact]
        public async Task An_Address_Of_Another_Machine_Fails_At_Once() {
            using var server = new NetServer(Config());
            var clock = Stopwatch.StartNew();
            var error = await Assert.ThrowsAsync<SocketException>(() => server.StartAsync(IPAddress.Parse("203.0.113.7"), 0)); // TEST-NET-3
            Assert.Equal(SocketError.AddressNotAvailable, error.SocketErrorCode);
            Assert.True(clock.ElapsedMilliseconds < 5000, "no retries for an address that can't work");
        }

        [Fact]
        public async Task A_Relay_Can_Listen_On_One_Address() {
            using var relay = new NetRelayServer { BindAddress = IPAddress.Loopback };
            int port = await relay.StartAsync(0);
            Assert.True(port > 0);
            using var tcp = new TcpClient();
            await tcp.ConnectAsync(IPAddress.Loopback, port);
            var lan = LanAddress();
            if (lan != null) {
                using var outsider = new TcpClient();
                await Assert.ThrowsAsync<SocketException>(() => outsider.ConnectAsync(lan, port));
            }
        }
    }
}
