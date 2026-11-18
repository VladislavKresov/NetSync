using System.Collections.Generic;
using System.Threading.Tasks;
using NetSync.Peers;
using NetSync.Transports;
using Xunit;

namespace NetSync.Tests {
    /// <summary>Polled delivery when the application stops polling: what may be dropped, and what may not.</summary>
    public class QueueOverflowTests {
        private static NetConfig Config(EventDelivery delivery, int maxPending = 100_000) {
            var config = new NetConfig { EventDelivery = delivery, MaxPendingEvents = maxPending };
            config.Channels[0] = new ChannelConfig(TransportType.Tcp);
            config.Channels[1] = new ChannelConfig(TransportType.Udp);
            return config;
        }

        [Fact]
        public async Task A_Reliable_Message_Is_Never_Lost_Silently() {
            using var server = new NetServer(Config(EventDelivery.Immediate));
            var opened = new TaskCompletionSource<NetConnection>(TaskCreationOptions.RunContinuationsAsynchronously);
            server.ConnectionOpened += connection => opened.TrySetResult(connection);
            int port = await server.StartAsync(0);

            using var client = new NetClient(Config(EventDelivery.Polled, maxPending: 8));
            var received = new List<byte>();
            DisconnectReason? reason = null;
            client.DataReceived += (channel, data) => received.Add(data[0]);
            client.Disconnected += r => reason = r;
            var connect = client.ConnectAsync("127.0.0.1", port);
            Assert.True(await Eventually.HoldsAsync(() => { client.PollEvents(); return connect.IsCompleted; }));
            await connect;
            var connection = await opened.Task;

            // The application stops polling while 30 reliable messages arrive. The client may close
            // the connection while some of them are still being sent: those sends say so.
            for (byte i = 0; i < 30; i++) {
                try {
                    await connection.SendAsync(0, new[] { i });
                }
                catch (System.InvalidOperationException) {
                    break; // "The connection closed while the message was waiting to be sent"
                }
            }
            Assert.True(await Eventually.HoldsAsync(() => !client.IsConnected), "the connection should close instead of dropping reliable data");

            // The Disconnected event is queued like any other: poll until it arrives.
            Assert.True(await Eventually.HoldsAsync(() => { client.PollEvents(); return reason != null; }));
            Assert.Equal(DisconnectReason.Overloaded, reason);
            // What was delivered is an unbroken prefix: nothing in the middle went missing.
            Assert.InRange(received.Count, 1, 29);
            for (int i = 0; i < received.Count; i++) {
                Assert.Equal(i, received[i]);
            }
        }

        [Fact]
        public async Task A_Full_Send_Queue_Slows_The_Sender_Down_Instead_Of_Failing() {
            var serverConfig = Config(EventDelivery.Immediate);
            serverConfig.PingTimeoutMs = 60_000;
            serverConfig.WriteTimeoutMs = 60_000;
            using var server = new NetServer(serverConfig);
            var opened = new TaskCompletionSource<NetConnection>(TaskCreationOptions.RunContinuationsAsynchronously);
            server.ConnectionOpened += connection => opened.TrySetResult(connection);
            int port = await server.StartAsync(0);

            // The client stops reading for a while, pongs included: give the keepalive room so the
            // server doesn't drop it as dead while a slow machine fills the queue.
            var clientConfig = Config(EventDelivery.Immediate);
            clientConfig.PingTimeoutMs = 60_000;
            using var client = new NetClient(clientConfig);
            using var resume = new System.Threading.ManualResetEventSlim();
            long received = 0;
            client.DataReceived += (channel, data) => {
                resume.Wait(System.TimeSpan.FromSeconds(30)); // the client stops reading
                System.Threading.Interlocked.Increment(ref received);
            };
            await client.ConnectAsync("127.0.0.1", port);
            var connection = await opened.Task;

            // Small messages until one has to wait: the queue and the socket buffers are full.
            var payload = new byte[1024];
            long sent = 0;
            Task? waiting = null;
            for (int i = 0; i < 500_000 && waiting == null; i++) {
                var send = connection.SendAsync(0, payload);
                sent++;
                if (!send.IsCompletedSuccessfully) {
                    var task = send.AsTask();
                    if (await Task.WhenAny(task, Task.Delay(200)) != task) {
                        waiting = task;
                    }
                    else {
                        await task; // would rethrow an overflow error
                    }
                }
            }
            Assert.NotNull(waiting);

            resume.Set();
            await waiting!; // completes once the client reads again: nothing was refused
            Assert.True(await Eventually.HoldsAsync(() => System.Threading.Interlocked.Read(ref received) == sent, 30_000),
                        $"received {System.Threading.Interlocked.Read(ref received)} of {sent}");
        }

        [Fact]
        public async Task Unreliable_Messages_Are_Dropped_And_The_Connection_Stays() {
            using var server = new NetServer(Config(EventDelivery.Immediate));
            var opened = new TaskCompletionSource<NetConnection>(TaskCreationOptions.RunContinuationsAsynchronously);
            server.ConnectionOpened += connection => opened.TrySetResult(connection);
            int port = await server.StartAsync(0);

            using var client = new NetClient(Config(EventDelivery.Polled, maxPending: 8));
            int received = 0;
            client.DataReceived += (channel, data) => received++;
            var connect = client.ConnectAsync("127.0.0.1", port);
            Assert.True(await Eventually.HoldsAsync(() => { client.PollEvents(); return connect.IsCompleted; }));
            await connect;
            var connection = await opened.Task;

            for (byte i = 0; i < 30; i++) {
                await connection.SendAsync(1, new[] { i });
            }
            Assert.True(await Eventually.HoldsAsync(() => client.Statistics.DroppedEvents > 0));

            client.PollEvents();
            Assert.True(client.IsConnected);
            Assert.InRange(received, 1, 29);
        }
    }
}
