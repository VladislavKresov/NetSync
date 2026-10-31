using System;
using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using NetSync.Diagnostics;
using NetSync.Peers;
using NetSync.Pipeline.Fragmentation;
using NetSync.Sending;
using NetSync.Transports;
using NetSync.Transports.Tcp;
using Xunit;

namespace NetSync.Tests {
    /// <summary>Stage 9: hostile-input limits, slow consumers, bounded queues.</summary>
    public class LimitsTests {
        [Fact]
        public async Task Huge_Tcp_Frame_Header_Drops_Peer_Without_Allocating() {
            using var server = new TcpServerTransport();
            var dropped = new TaskCompletionSource<DisconnectReason>(TaskCreationOptions.RunContinuationsAsynchronously);
            server.PeerDisconnected += (_, reason) => dropped.TrySetResult(reason);
            int port = await server.StartAsync(0);

            using var raw = new TcpClient();
            await raw.ConnectAsync(IPAddress.Loopback, port);
            var frame = new byte[5];
            frame[0] = 0x10; // Data
            BinaryPrimitives.WriteInt32BigEndian(frame.AsSpan(1), int.MaxValue); // "2 GB follows"
            await raw.GetStream().WriteAsync(frame, 0, frame.Length);

            Assert.Equal(DisconnectReason.Error, await WithTimeout(dropped.Task));
            await server.StopAsync();
        }

        [Fact]
        public async Task NetServer_Rejects_Large_Frames_Before_Handshake() {
            using var server = new NetServer(MakeConfig());
            int port = await server.StartAsync(0);

            using var raw = new TcpClient();
            await raw.ConnectAsync(IPAddress.Loopback, port);
            var stream = raw.GetStream();
            // A 4 KB frame before Hello: legal post-handshake, hostile pre-handshake.
            var frame = new byte[5 + 4096];
            frame[0] = 0x10;
            BinaryPrimitives.WriteInt32BigEndian(frame.AsSpan(1), 4096);
            await stream.WriteAsync(frame, 0, frame.Length);

            // Server must close the socket: the next read returns 0 (or throws on RST).
            var buffer = new byte[64];
            int read;
            try {
                var readTask = stream.ReadAsync(buffer, 0, buffer.Length);
                Assert.Same(readTask, await Task.WhenAny(readTask, Task.Delay(5000)));
                read = await readTask;
            }
            catch (System.IO.IOException) {
                read = 0;
            }
            Assert.Equal(0, read);

            // Legit clients are unaffected.
            using var client = new NetClient(MakeConfig());
            await client.ConnectAsync("127.0.0.1", port);
            Assert.True(client.IsConnected);
            await server.StopAsync();
        }

        [Fact]
        public async Task Slow_Consumer_Is_Dropped_And_Does_Not_Stall_Other_Peers() {
            var settings = new TransportSettings { PingIntervalMs = 100, PingTimeoutMs = 0, WriteTimeoutMs = 500 };
            using var server = new TcpServerTransport(settings);
            var peers = new System.Collections.Concurrent.ConcurrentQueue<TransportPeer>();
            var slowDropped = new TaskCompletionSource<DisconnectReason>(TaskCreationOptions.RunContinuationsAsynchronously);
            server.PeerConnected += peer => peers.Enqueue(peer);
            int port = await server.StartAsync(0);

            // Slow consumer: connects, never reads.
            using var slow = new TcpClient { ReceiveBufferSize = 4096 };
            await slow.ConnectAsync(IPAddress.Loopback, port);
            await WaitUntilAsync(() => peers.Count == 1);
            peers.TryPeek(out var slowPeer);
            server.PeerDisconnected += (peer, reason) => {
                if (ReferenceEquals(peer, slowPeer)) slowDropped.TrySetResult(reason);
            };

            // Healthy client alongside it.
            using var healthy = new TcpTransport(pingIntervalMs: 100);
            await healthy.ConnectAsync(new IPEndPoint(IPAddress.Loopback, port));

            // Flood the slow peer; the send must not hang the caller forever.
            var flood = new byte[32 * 1024 * 1024];
            var sendTask = server.SendAsync(slowPeer!, flood).AsTask();

            Assert.Equal(DisconnectReason.Timeout, await WithTimeout(slowDropped.Task, 15_000));
            // The send either completed (accepted by the queue) or failed with the peer вЂ”
            // what matters is that the caller is never stuck.
            Assert.Same(sendTask, await Task.WhenAny(sendTask, Task.Delay(5_000)));

            // Keepalive to the healthy peer kept flowing the whole time.
            Assert.True(healthy.IsConnected);
            Assert.True(healthy.PingMs >= 0, $"healthy client should have a ping, got {healthy.PingMs}");
            await server.StopAsync();
        }

        [Fact]
        public void FragmentBuffer_Rejects_Oversized_Packets() {
            var buffer = new FragmentBuffer { MaxPacketBytes = 1000 };
            Assert.True(buffer.TryAddFragment(1, 0, 3, new byte[600], out _));
            Assert.False(buffer.TryAddFragment(1, 1, 3, new byte[600], out _)); // 1200 > 1000
            Assert.Equal(0, buffer.ActiveCount);
            Assert.Equal(0, buffer.BufferedBytes);
        }

        [Fact]
        public void FragmentBuffer_Rejects_Absurd_Fragment_Counts_Without_Allocating_Them() {
            var buffer = new FragmentBuffer { MaxFragmentsPerPacket = 100 };
            Assert.False(buffer.TryAddFragment(1, 0, 1_000_000, new byte[1], out _));
            Assert.Equal(0, buffer.ActiveCount);
        }

        [Fact]
        public void FragmentBuffer_Caps_Total_Buffered_Bytes() {
            var buffer = new FragmentBuffer { MaxPacketBytes = 10_000, MaxBufferedBytes = 2_500 };
            for (uint seq = 0; seq < 10; seq++) {
                buffer.TryAddFragment(seq, 0, 2, new byte[1000], out _); // each packet stays incomplete
            }
            Assert.True(buffer.BufferedBytes <= 2_500, $"buffered {buffer.BufferedBytes}");
        }

        [Fact]
        public async Task SendQueue_EnqueueAsync_Waits_For_Space_Instead_Of_Failing() {
            int sent = 0;
            using var queue = new SendQueue((_, _, _) => {
                Interlocked.Increment(ref sent);
                return Task.CompletedTask;
            }, NullNetLogger.Instance, maxQueueSize: 2);

            Assert.True(queue.TryEnqueue(new byte[1], 1, false, SendPriority.Low));
            Assert.True(queue.TryEnqueue(new byte[1], 1, false, SendPriority.Low));
            Assert.False(queue.TryEnqueue(new byte[1], 1, false, SendPriority.Low)); // full, fail-fast path

            var waiting = queue.EnqueueAsync(new byte[1], 1, false, SendPriority.Low, CancellationToken.None).AsTask();
            await Task.Delay(100);
            Assert.False(waiting.IsCompleted); // backpressure, not an exception

            queue.Start();
            await WithTimeout(waiting);
            await WaitUntilAsync(() => Volatile.Read(ref sent) == 3);
        }

        [Fact]
        public void Polled_Event_Queue_Drops_Data_But_Never_Lifecycle_Events() {
            var dispatcher = new EventDispatcher(EventDelivery.Polled, NullNetLogger.Instance, maxPendingEvents: 5);
            int data = 0, lifecycle = 0;
            for (int i = 0; i < 10; i++) {
                dispatcher.DispatchData(() => data++);
            }
            dispatcher.Dispatch(() => lifecycle++);

            dispatcher.Poll(int.MaxValue);
            Assert.Equal(5, data);
            Assert.Equal(1, lifecycle);
            Assert.Equal(5, dispatcher.DroppedEvents);
        }

        [Fact]
        public async Task Sending_Above_MaxMessageSize_Throws() {
            var config = MakeConfig();
            config.MaxMessageSize = 64 * 1024;
            using var server = new NetServer(config);
            int port = await server.StartAsync(0);

            using var client = new NetClient(config);
            await client.ConnectAsync("127.0.0.1", port);
            await Assert.ThrowsAsync<ArgumentException>(async () => await client.SendAsync(0, new byte[64 * 1024 + 1]));
            await server.StopAsync();
        }

        [Fact]
        public async Task Hostile_Client_Sending_Above_MaxMessageSize_Is_Disconnected() {
            // TCP-only server so a raw socket can complete the whole handshake.
            var config = new NetConfig { EventDelivery = EventDelivery.Immediate, MaxMessageSize = 16 * 1024 };
            config.Channels[0] = new ChannelConfig(TransportType.Tcp);
            using var server = new NetServer(config);
            var closed = new TaskCompletionSource<DisconnectReason>(TaskCreationOptions.RunContinuationsAsynchronously);
            int delivered = 0;
            server.ConnectionClosed += (_, reason) => closed.TrySetResult(reason);
            server.DataReceived += (_, _, _) => Interlocked.Increment(ref delivered);
            int port = await server.StartAsync(0);

            using var raw = new TcpClient();
            await raw.ConnectAsync(IPAddress.Loopback, port);
            var stream = raw.GetStream();
            // A well-formed Hello carrying the server's own config hash...
            uint hash = config.ComputeConfigHash(config.BuildChannelTable());
            await WriteFrameAsync(stream, PeerProtocol.EncodeHello(12345, hash, KeyExchangeMode.None, 0, null, null));
            await Task.Delay(100);
            await WriteFrameAsync(stream, new[] { PeerProtocol.MsgJoin }); // ...and a Join: the connection opens
            await Task.Delay(200);
            // ...then a 50 KB message on channel 0: unfragmented on TCP, 3x the limit.
            var message = new byte[2 + 50 * 1024];
            message[0] = PeerProtocol.MsgData;
            await WriteFrameAsync(stream, message);

            Assert.Equal(DisconnectReason.Error, await WithTimeout(closed.Task, 10_000));
            Assert.Equal(0, Volatile.Read(ref delivered));
            await server.StopAsync();
        }

        private static async Task WriteFrameAsync(System.IO.Stream stream, byte[] body) {
            var frame = PacketProtocol.RentTcpDataFrame(ReadOnlySpan<byte>.Empty, body, out int length);
            try {
                await stream.WriteAsync(frame, 0, length);
            }
            finally {
                PacketProtocol.Return(frame);
            }
        }

        private static NetConfig MakeConfig() {
            var config = new NetConfig {
                EventDelivery = EventDelivery.Immediate,
                PingIntervalMs = 200,
                ConnectTimeoutMs = 5000
            };
            config.Channels[0] = new ChannelConfig(TransportType.Tcp);
            config.Channels[1] = new ChannelConfig(TransportType.Udp);
            config.Channels[2] = new ChannelConfig(TransportType.Udp, ReliabilityMode.ReliableOrdered);
            return config;
        }

        private static async Task<T> WithTimeout<T>(Task<T> task, int timeoutMs = 5000) {
            var completed = await Task.WhenAny(task, Task.Delay(timeoutMs));
            Assert.Same(task, completed);
            return await task;
        }

        private static async Task WithTimeout(Task task, int timeoutMs = 5000) {
            var completed = await Task.WhenAny(task, Task.Delay(timeoutMs));
            Assert.Same(task, completed);
            await task;
        }

        private static async Task WaitUntilAsync(Func<bool> condition, int timeoutMs = 5000) {
            var deadline = DateTime.UtcNow.AddMilliseconds(timeoutMs);
            while (!condition()) {
                if (DateTime.UtcNow > deadline) {
                    throw new TimeoutException("Condition not met in time");
                }
                await Task.Delay(10);
            }
        }
    }
}
