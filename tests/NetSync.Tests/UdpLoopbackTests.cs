using System;
using System.Net;
using System.Threading.Tasks;
using NetSync.Transports.Udp;
using Xunit;

namespace NetSync.Tests {
    public class UdpLoopbackTests {
        [Fact]
        public async Task Echo_Small_Packet() {
            using var server = new UdpServerTransport();
            server.DataReceived += (peer, data) => _ = server.SendAsync(peer, data);
            int port = await server.StartAsync(0);

            using var client = new UdpTransport();
            var received = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
            client.DataReceived += data => received.TrySetResult(data);

            await client.ConnectAsync(new IPEndPoint(IPAddress.Loopback, port));
            var payload = new byte[] { 10, 20, 30 };
            await client.SendAsync(payload);

            var echoed = await WithTimeout(received.Task);
            Assert.Equal(payload, echoed);
            await server.StopAsync();
        }

        [Fact]
        public async Task Server_Tracks_Peer_And_Measures_Ping() {
            using var server = new UdpServerTransport(pingIntervalMs: 100);
            var peerSeen = new TaskCompletionSource<NetSync.Transports.TransportPeer>(TaskCreationOptions.RunContinuationsAsynchronously);
            server.PeerConnected += peer => peerSeen.TrySetResult(peer);
            int port = await server.StartAsync(0);

            using var client = new UdpTransport(pingIntervalMs: 100);
            await client.ConnectAsync(new IPEndPoint(IPAddress.Loopback, port));

            var peer = await WithTimeout(peerSeen.Task);
            await Task.Delay(600);

            Assert.True(client.PingMs >= 0, $"client ping should be measured, got {client.PingMs}");
            Assert.True(peer.PingMs >= 0, $"server-side peer ping should be measured, got {peer.PingMs}");
            await server.StopAsync();
        }

        [Fact]
        public async Task Oversized_Payload_Is_Rejected_With_Clear_Error() {
            using var server = new UdpServerTransport();
            int port = await server.StartAsync(0);

            using var client = new UdpTransport();
            await client.ConnectAsync(new IPEndPoint(IPAddress.Loopback, port));

            await Assert.ThrowsAsync<ArgumentException>(async () =>
                await client.SendAsync(new byte[UdpTransport.MaxDatagramPayload + 1]));
            await server.StopAsync();
        }

        [Fact]
        public async Task Silent_Peer_Times_Out_On_Server() {
            using var server = new UdpServerTransport(pingIntervalMs: 100, pingTimeoutMs: 500);
            var disconnected = new TaskCompletionSource<NetSync.Transports.DisconnectReason>(TaskCreationOptions.RunContinuationsAsynchronously);
            server.PeerDisconnected += (peer, reason) => disconnected.TrySetResult(reason);
            int port = await server.StartAsync(0);

            using var client = new UdpTransport(pingIntervalMs: 100);
            await client.ConnectAsync(new IPEndPoint(IPAddress.Loopback, port));
            await Task.Delay(200);
            client.Disconnect(); // goes silent; server must expire the peer

            var reason = await WithTimeout(disconnected.Task, 5000);
            Assert.Equal(NetSync.Transports.DisconnectReason.Timeout, reason);
            await server.StopAsync();
        }

        private static async Task<T> WithTimeout<T>(Task<T> task, int timeoutMs = 5000) {
            var completed = await Task.WhenAny(task, Task.Delay(timeoutMs));
            Assert.Same(task, completed);
            return await task;
        }
    }
}
