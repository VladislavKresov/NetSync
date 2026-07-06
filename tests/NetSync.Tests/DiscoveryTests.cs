using System;
using System.Net;
using System.Threading.Tasks;
using NetSync.Discovery;
using Xunit;

namespace NetSync.Tests {
    public class DiscoveryTests {
        [Fact]
        public void Message_Roundtrips() {
            var datagram = DiscoveryMessage.Encode("MyGame", 7777);
            Assert.True(DiscoveryMessage.TryDecode(datagram, out var appId, out var port));
            Assert.Equal("MyGame", appId);
            Assert.Equal(7777, port);
        }

        [Fact]
        public void Garbage_And_Foreign_Datagrams_Are_Ignored() {
            Assert.False(DiscoveryMessage.TryDecode(new byte[] { 1, 2, 3 }, out _, out _));
            Assert.False(DiscoveryMessage.TryDecode(new byte[64], out _, out _)); // wrong magic
            var truncated = DiscoveryMessage.Encode("MyGame", 7777).AsSpan(0, 8);
            Assert.False(DiscoveryMessage.TryDecode(truncated, out _, out _));
        }

        [Fact]
        public async Task Listener_Finds_Broadcasting_Server() {
            using var listener = new LanServerListener("DiscoveryTestApp", discoveryPort: 0);
            var found = new TaskCompletionSource<IPEndPoint>(TaskCreationOptions.RunContinuationsAsynchronously);
            listener.ServerFound += endpoint => found.TrySetResult(endpoint);
            listener.Start();

            using var broadcaster = new LanServerBroadcaster("DiscoveryTestApp", serverPort: 7777, intervalMs: 100) {
                // Deterministic in CI: point straight at the listener instead of broadcasting.
                TargetOverride = new IPEndPoint(IPAddress.Loopback, listener.Port)
            };
            broadcaster.Start();

            var completed = await Task.WhenAny(found.Task, Task.Delay(10_000));
            Assert.Same(found.Task, completed);
            var server = await found.Task;
            Assert.Equal(7777, server.Port);
        }

        [Fact]
        public async Task Listener_Ignores_Other_AppIds() {
            using var listener = new LanServerListener("AppA", discoveryPort: 0);
            int foundCount = 0;
            listener.ServerFound += _ => foundCount++;
            listener.Start();

            using var broadcaster = new LanServerBroadcaster("AppB", serverPort: 7778, intervalMs: 50) {
                TargetOverride = new IPEndPoint(IPAddress.Loopback, listener.Port)
            };
            broadcaster.Start();

            await Task.Delay(500);
            Assert.Equal(0, foundCount);
        }
    }
}
