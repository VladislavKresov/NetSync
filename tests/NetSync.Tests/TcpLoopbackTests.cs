using System;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using NetSync.Transports;
using NetSync.Transports.Tcp;
using Xunit;

namespace NetSync.Tests {
    public class TcpLoopbackTests {
        [Fact]
        public async Task Echo_Small_Packet() {
            using var server = new TcpServerTransport();
            server.DataReceived += (peer, data) => _ = server.SendAsync(peer, data);
            int port = await server.StartAsync(0);

            using var client = new TcpTransport();
            var received = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
            client.DataReceived += data => received.TrySetResult(data);

            await client.ConnectAsync(new IPEndPoint(IPAddress.Loopback, port));
            var payload = new byte[] { 1, 2, 3, 4, 5 };
            await client.SendAsync(payload);

            var echoed = await WithTimeout(received.Task);
            Assert.Equal(payload, echoed);
            await server.StopAsync();
        }

        [Fact]
        public async Task Echo_Large_Packet_Uses_Fragmentation() {
            using var server = new TcpServerTransport();
            server.DataReceived += (peer, data) => _ = server.SendAsync(peer, data);
            int port = await server.StartAsync(0);

            using var client = new TcpTransport();
            var received = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
            client.DataReceived += data => received.TrySetResult(data);

            await client.ConnectAsync(new IPEndPoint(IPAddress.Loopback, port));

            var payload = new byte[300_000]; // > MaxUnfragmentedSize → fragmented path
            new Random(7).NextBytes(payload);
            await client.SendAsync(payload);

            var echoed = await WithTimeout(received.Task, 15000);
            Assert.Equal(payload, echoed);
            Assert.True(client.Metrics!.FragmentsSent > 1);
            await server.StopAsync();
        }

        [Fact]
        public async Task Transport_Is_Reconnectable() {
            // 1.x bug: after Disconnect() the instance was permanently dead.
            using var server = new TcpServerTransport();
            server.DataReceived += (peer, data) => _ = server.SendAsync(peer, data);
            int port = await server.StartAsync(0);

            using var client = new TcpTransport();
            await client.ConnectAsync(new IPEndPoint(IPAddress.Loopback, port));
            Assert.True(client.IsConnected);
            client.Disconnect();
            Assert.False(client.IsConnected);

            await client.ConnectAsync(new IPEndPoint(IPAddress.Loopback, port));
            Assert.True(client.IsConnected);

            var received = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
            client.DataReceived += data => received.TrySetResult(data);
            await client.SendAsync(new byte[] { 9 });
            var echoed = await WithTimeout(received.Task);
            Assert.Equal(new byte[] { 9 }, echoed);
            await server.StopAsync();
        }

        [Fact]
        public async Task Disconnect_Fires_Single_Event_With_Local_Reason() {
            using var server = new TcpServerTransport();
            int port = await server.StartAsync(0);

            using var client = new TcpTransport();
            int disconnectCount = 0;
            DisconnectReason? reason = null;
            client.Disconnected += r => {
                Interlocked.Increment(ref disconnectCount);
                reason = r;
            };

            await client.ConnectAsync(new IPEndPoint(IPAddress.Loopback, port));
            client.Disconnect();
            await Task.Delay(300); // let racing loops finish

            Assert.Equal(1, disconnectCount);
            Assert.Equal(DisconnectReason.Local, reason);
            await server.StopAsync();
        }

        [Fact]
        public async Task Server_Detects_Client_Disconnect() {
            using var server = new TcpServerTransport();
            var disconnected = new TaskCompletionSource<DisconnectReason>(TaskCreationOptions.RunContinuationsAsynchronously);
            server.PeerDisconnected += (peer, reason) => disconnected.TrySetResult(reason);
            int port = await server.StartAsync(0);

            using var client = new TcpTransport();
            await client.ConnectAsync(new IPEndPoint(IPAddress.Loopback, port));
            await Task.Delay(100);
            client.Disconnect();

            var reason = await WithTimeout(disconnected.Task);
            Assert.Equal(DisconnectReason.Remote, reason);
            await server.StopAsync();
        }

        [Fact]
        public async Task Ping_Is_Measured() {
            using var server = new TcpServerTransport(pingIntervalMs: 100);
            int port = await server.StartAsync(0);

            using var client = new TcpTransport(pingIntervalMs: 100);
            await client.ConnectAsync(new IPEndPoint(IPAddress.Loopback, port));
            await Task.Delay(600);

            Assert.True(client.PingMs >= 0, $"client ping should be measured, got {client.PingMs}");
            await server.StopAsync();
        }

        private static async Task<T> WithTimeout<T>(Task<T> task, int timeoutMs = 5000) {
            var completed = await Task.WhenAny(task, Task.Delay(timeoutMs));
            Assert.Same(task, completed);
            return await task;
        }
    }
}
