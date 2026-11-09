using System;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using NetSync.Diagnostics;
using NetSync.Peers;
using NetSync.Pipeline.Reliability;
using NetSync.Transports;
using NetSync.Transports.Udp;
using Xunit;

namespace NetSync.Tests {
    public class DatagramBatchingTests {
        /// <summary>Captures what the batcher's drain loop sends.</summary>
        private sealed class Capture {
            public readonly BlockingCollection<byte[]> Datagrams = new BlockingCollection<byte[]>();
            public ManualResetEventSlim Gate = new ManualResetEventSlim(true);
            public readonly ManualResetEventSlim Entered = new ManualResetEventSlim(false);

            public void Send(object? destination, byte[] buffer, int offset, int length) {
                Entered.Set();
                Gate.Wait();
                Datagrams.Add(buffer.AsSpan(offset, length).ToArray());
            }

            public byte[] Next() {
                Assert.True(Datagrams.TryTake(out var datagram, 5000), "no datagram sent");
                return datagram!;
            }
        }

        private static List<byte[]> Unpack(byte[] datagram, int maxPackets = int.MaxValue) {
            var packets = new List<byte[]>();
            DatagramBatcher.Unpack(datagram, datagram.Length, maxPackets, packets,
                                   (list, buffer, offset, count) => list.Add(buffer.AsSpan(offset, count).ToArray()));
            return packets;
        }

        [Fact]
        public void Packets_Queued_Together_Share_One_Datagram() {
            var capture = new Capture();
            using var batcher = new DatagramBatcher(capture.Send, NullNetLogger.Instance);
            var target = batcher.CreateTarget(null);

            batcher.Cork(); // as the receive thread does: nothing leaves until Uncork
            for (int i = 0; i < 5; i++) {
                Assert.True(batcher.Enqueue(target, new byte[] { (byte)i }, new byte[] { 0xAA, (byte)(i * 2) }));
            }
            batcher.Uncork();

            var datagram = capture.Next();
            Assert.Equal((byte)PacketType.Batch, datagram[0]);
            var packets = Unpack(datagram);
            Assert.Equal(5, packets.Count);
            for (int i = 0; i < 5; i++) {
                Assert.Equal(new byte[] { (byte)i, 0xAA, (byte)(i * 2) }, packets[i]);
            }
        }

        [Fact]
        public void A_Lone_Packet_Goes_Out_As_Plain_Data() {
            var capture = new Capture();
            using var batcher = new DatagramBatcher(capture.Send, NullNetLogger.Instance);
            var target = batcher.CreateTarget(null);

            foreach (int size in new[] { 3, 200, 20_000 }) { // 1-, 2- and 3-byte length headers
                var payload = Enumerable.Range(0, size).Select(i => (byte)i).ToArray();
                batcher.Enqueue(target, ReadOnlySpan<byte>.Empty, payload);
                var datagram = capture.Next();
                Assert.Equal((byte)PacketType.Data, datagram[0]);
                Assert.Equal(payload, datagram.AsSpan(1).ToArray());
            }
        }

        [Fact]
        public void Batches_Stay_Under_The_Size_Limit_And_Keep_Order() {
            var capture = new Capture { Gate = new ManualResetEventSlim(false) };
            using var batcher = new DatagramBatcher(capture.Send, NullNetLogger.Instance);
            var target = batcher.CreateTarget(null);

            // While the first send is stuck, everything else piles up: small, big, small.
            batcher.Enqueue(target, ReadOnlySpan<byte>.Empty, new byte[] { 0 });
            Assert.True(capture.Entered.Wait(5000), "the drain loop never started");
            int next = 1;
            var expected = new List<int> { 0 };
            void Add(int size) {
                var payload = new byte[size];
                BinaryPrimitives.WriteInt32LittleEndian(payload, next);
                expected.Add(next++);
                Assert.True(batcher.Enqueue(target, ReadOnlySpan<byte>.Empty, payload));
            }
            for (int i = 0; i < 100; i++) {
                Add(40);
            }
            Add(3000); // bigger than a batch: goes alone, in order
            for (int i = 0; i < 100; i++) {
                Add(40);
            }
            capture.Gate.Set();

            var order = new List<int>();
            while (order.Count < expected.Count) {
                var datagram = capture.Next();
                Assert.True(datagram.Length <= DatagramBatcher.MaxBatchSize || datagram[0] == (byte)PacketType.Data);
                var packets = datagram[0] == (byte)PacketType.Data ? new List<byte[]> { datagram.AsSpan(1).ToArray() } : Unpack(datagram);
                order.AddRange(packets.Select(p => p.Length >= 4 ? BinaryPrimitives.ReadInt32LittleEndian(p) : p[0]));
            }
            Assert.Equal(expected, order);
        }

        [Fact]
        public void Backlog_Is_Capped_Per_Destination() {
            var capture = new Capture { Gate = new ManualResetEventSlim(false) };
            using var batcher = new DatagramBatcher(capture.Send, NullNetLogger.Instance);
            var stuck = batcher.CreateTarget(null);
            var other = batcher.CreateTarget(null);

            batcher.Enqueue(stuck, ReadOnlySpan<byte>.Empty, new byte[100]); // blocks the drain loop
            Assert.True(capture.Entered.Wait(5000), "the drain loop never started");
            int accepted = 0;
            while (batcher.Enqueue(stuck, ReadOnlySpan<byte>.Empty, new byte[1000])) {
                accepted++;
                Assert.True(accepted < 1000, "backlog never filled");
            }
            Assert.InRange(accepted * 1002, DatagramBatcher.MaxQueuedBytes - 2000, DatagramBatcher.MaxQueuedBytes);
            Assert.True(batcher.Enqueue(other, ReadOnlySpan<byte>.Empty, new byte[1000])); // others unaffected

            capture.Gate.Set();
            batcher.Close(stuck); // sends what is queued, then refuses more
            Assert.False(batcher.Enqueue(stuck, ReadOnlySpan<byte>.Empty, new byte[1]));
        }

        [Fact]
        public async Task Closing_Sends_What_Is_Already_Queued() {
            var capture = new Capture { Gate = new ManualResetEventSlim(false) };
            var batcher = new DatagramBatcher(capture.Send, NullNetLogger.Instance);
            var busy = batcher.CreateTarget(null);
            var closing = batcher.CreateTarget(null);
            batcher.Enqueue(busy, ReadOnlySpan<byte>.Empty, new byte[] { 1 }); // the drain loop blocks on this one
            Assert.True(capture.Entered.Wait(5000), "the drain loop never started");
            batcher.Enqueue(closing, ReadOnlySpan<byte>.Empty, new byte[] { 2 }); // e.g. a Bye right before a kick
            batcher.Enqueue(busy, ReadOnlySpan<byte>.Empty, new byte[] { 3 });

            var close = Task.Run(() => batcher.Close(closing));
            var dispose = close.ContinueWith(_ => batcher.Dispose(), TaskScheduler.Default);
            capture.Gate.Set();
            Assert.Same(dispose, await Task.WhenAny(dispose, Task.Delay(5000)));
            // Next(), not a snapshot: the send released by the gate may still be finishing.
            var payloads = Enumerable.Range(0, 3).Select(_ => capture.Next()[1]).OrderBy(b => b).ToArray();
            Assert.Equal(new byte[] { 1, 2, 3 }, payloads);
        }

        private static NetConfig UdpOnlyConfig() {
            var config = new NetConfig { EventDelivery = EventDelivery.Immediate, PingTimeoutMs = 30_000 };
            config.Channels[0] = new ChannelConfig(TransportType.Udp, ReliabilityMode.ReliableOrdered);
            return config;
        }

        [Fact]
        public async Task Udp_Only_Kick_And_Disconnect_Arrive_At_Once() {
            using var server = new NetServer(UdpOnlyConfig());
            var closed = new TaskCompletionSource<DisconnectReason>(TaskCreationOptions.RunContinuationsAsynchronously);
            server.ConnectionClosed += (connection, reason) => closed.TrySetResult(reason);
            int port = await server.StartAsync(0);

            // Client leaves: the server learns it from the Bye, not from a 30 s ping timeout.
            using (var leaving = new NetClient(UdpOnlyConfig())) {
                await leaving.ConnectAsync("127.0.0.1", port);
                await WaitForAsync(() => server.ConnectionCount == 1);
                leaving.Disconnect();
            }
            Assert.Same(closed.Task, await Task.WhenAny(closed.Task, Task.Delay(3000)));

            // Server kicks: the client learns it from the Bye.
            using var kicked = new NetClient(UdpOnlyConfig());
            var reason = new TaskCompletionSource<DisconnectReason>(TaskCreationOptions.RunContinuationsAsynchronously);
            kicked.Disconnected += r => reason.TrySetResult(r);
            await kicked.ConnectAsync("127.0.0.1", port);
            await WaitForAsync(() => server.ConnectionCount == 1);
            await Assert.Single(server.Connections).DisconnectAsync();
            Assert.Same(reason.Task, await Task.WhenAny(reason.Task, Task.Delay(3000)));
            Assert.Equal(DisconnectReason.Kicked, await reason.Task);
        }

        private static async Task WaitForAsync(Func<bool> condition) {
            for (int i = 0; i < 500 && !condition(); i++) {
                await Task.Delay(10);
            }
            Assert.True(condition());
        }

        [Fact]
        public void Malformed_Batches_Are_Cut_At_The_First_Bad_Entry() {
            // [Batch][2]["ok"][0x80 0x80 0x80 = varint longer than 3 bytes]
            Assert.Single(Unpack(new byte[] { 0x12, 2, 1, 2, 0x80, 0x80, 0x80, 0x01 }));
            // length beyond the datagram
            Assert.Single(Unpack(new byte[] { 0x12, 1, 7, 50, 1 }));
            // truncated varint
            Assert.Single(Unpack(new byte[] { 0x12, 1, 7, 0x80 }));
            // packet cap
            Assert.Equal(2, Unpack(new byte[] { 0x12, 1, 1, 1, 2, 1, 3 }, maxPackets: 2).Count);
        }

        [Fact]
        public async Task Udp_Burst_Arrives_Complete_In_Fewer_Datagrams() {
            using var server = new UdpServerTransport();
            var received = new ConcurrentQueue<int>();
            var all = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            const int count = 300;
            server.DataReceived += (peer, data) => {
                received.Enqueue(BitConverter.ToInt32(data));
                if (received.Count == count) {
                    all.TrySetResult(true);
                }
            };
            int port = await server.StartAsync(0);

            using var client = new UdpTransport();
            await client.ConnectAsync(new IPEndPoint(IPAddress.Loopback, port));
            for (int i = 0; i < count; i++) {
                await client.SendAsync(BitConverter.GetBytes(i));
            }

            Assert.Same(all.Task, await Task.WhenAny(all.Task, Task.Delay(5000)));
            Assert.Equal(Enumerable.Range(0, count), received);
            Assert.True(client.Metrics!.PacketsSent < count, $"{client.Metrics.PacketsSent} datagrams for {count} packets");
            await server.StopAsync();
        }

        [Fact]
        public async Task Untrusted_Source_Gets_One_Packet_Per_Batch() {
            using var server = new UdpServerTransport();
            ((ITrustAwareServerTransport)server).RequireTrust = true;
            var received = new ConcurrentQueue<byte>();
            TransportPeer? peer = null;
            server.DataReceived += (from, data) => {
                peer = from;
                received.Enqueue(data[0]);
            };
            int port = await server.StartAsync(0);

            using var socket = new UdpClient();
            socket.Connect(IPAddress.Loopback, port);
            var batch = new byte[] { (byte)PacketType.Batch, 1, 1, 1, 2, 1, 3 };
            await SendUntil(socket, batch, () => received.Count >= 1);
            await Task.Delay(100);
            Assert.All(received, b => Assert.Equal(1, b)); // only the first entry of each batch

            ((ITrustAwareServerTransport)server).MarkTrusted(peer!);
            while (received.TryDequeue(out _)) { }
            await SendUntil(socket, batch, () => received.Count >= 3);
            Assert.Equal(new byte[] { 1, 2, 3 }, received.Take(3));
            await server.StopAsync();
        }

        private static async Task SendUntil(UdpClient socket, byte[] datagram, Func<bool> done) {
            for (int i = 0; i < 20 && !done(); i++) {
                await socket.SendAsync(datagram, datagram.Length);
                for (int wait = 0; wait < 10 && !done(); wait++) {
                    await Task.Delay(20);
                }
            }
            Assert.True(done(), "the server never delivered");
        }

        [Fact]
        public void Deferred_Acks_Collapse_To_One_Per_Channel_And_Cover_Every_Packet() {
            const byte channel = 3;
            var channels = new Dictionary<byte, ChannelConfig> {
                [channel] = new ChannelConfig(TransportType.Udp, ReliabilityMode.ReliableOrdered)
            };
            var toReceiver = new List<byte[]>();
            var toSender = new List<byte[]>();
            using var sender = new ReliableEndpoint(channels, m => { toReceiver.Add(m.ToArray()); return default; },
                                                    (_, _) => { }, () => 5, _ => { }, NullNetLogger.Instance);
            using var receiver = new ReliableEndpoint(channels, m => { toSender.Add(m.ToArray()); return default; },
                                                      (_, _) => { }, () => 5, _ => { }, NullNetLogger.Instance) { DeferAcks = true };

            for (int i = 0; i < 10; i++) {
                _ = sender.SendReliableAsync(channel, new byte[] { (byte)i });
            }
            // One "datagram" delivers packets 1..9 first and 0 last (reordered), then the boundary.
            foreach (int i in Enumerable.Range(1, 9).Append(0)) {
                receiver.HandleRelData(toReceiver[i], 0, toReceiver[i].Length);
            }
            Assert.Empty(toSender);
            receiver.FlushAcks();

            var ack = Assert.Single(toSender);
            Assert.Equal(PeerProtocol.MsgRelAck, ack[0]);
            Assert.Equal(9, BinaryPrimitives.ReadUInt16BigEndian(ack.AsSpan(2))); // anchored on the newest seq
            Assert.Equal(0x1FFu, BinaryPrimitives.ReadUInt32BigEndian(ack.AsSpan(4)) & 0x1FFu); // 8..0 all acked

            sender.HandleAck(ack, 0, ack.Length);
            receiver.FlushAcks();
            Assert.Single(toSender); // nothing pending: no extra ack
        }

        [Fact]
        public void Deferred_Acks_Send_Early_What_One_Mask_Cannot_Cover() {
            const byte channel = 3;
            var channels = new Dictionary<byte, ChannelConfig> {
                [channel] = new ChannelConfig(TransportType.Udp, ReliabilityMode.Reliable)
            };
            var toReceiver = new List<byte[]>();
            var acks = new List<byte[]>();
            using var sender = new ReliableEndpoint(channels, m => { toReceiver.Add(m.ToArray()); return default; },
                                                    (_, _) => { }, () => 5, _ => { }, NullNetLogger.Instance, congestionControl: false);
            using var receiver = new ReliableEndpoint(channels, m => { acks.Add(m.ToArray()); return default; },
                                                      (_, _) => { }, () => 5, _ => { }, NullNetLogger.Instance) { DeferAcks = true };
            for (int i = 0; i < 40; i++) {
                _ = sender.SendReliableAsync(channel, new byte[] { (byte)i });
            }
            receiver.HandleRelData(toReceiver[0], 0, toReceiver[0].Length);
            receiver.HandleRelData(toReceiver[39], 0, toReceiver[39].Length); // 39 apart: 0 falls outside 39's mask
            Assert.Single(acks); // the ack for 0 went out at once
            receiver.FlushAcks();
            Assert.Equal(2, acks.Count);
            Assert.Equal(0, BinaryPrimitives.ReadUInt16BigEndian(acks[0].AsSpan(2)));
            Assert.Equal(39, BinaryPrimitives.ReadUInt16BigEndian(acks[1].AsSpan(2)));
        }
    }
}
