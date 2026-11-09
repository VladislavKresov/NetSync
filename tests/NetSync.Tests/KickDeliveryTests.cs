using System;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using NetSync.Peers;
using NetSync.Relay;
using NetSync.Transports;
using Xunit;

namespace NetSync.Tests {
    public class KickDeliveryTests {
        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task Kick_Reaches_A_Client_While_Data_Is_Still_Queued(bool relayed) {
            // TCP only: on a UDP link the Bye travels separately and would hide a lost one.
            using var relay = new NetRelayServer();
            using var server = new NetServer(Config());
            var opened = new TaskCompletionSource<NetConnection>(TaskCreationOptions.RunContinuationsAsynchronously);
            server.ConnectionOpened += connection => opened.TrySetResult(connection);

            var clientConfig = Config();
            IPEndPoint target;
            if (relayed) {
                target = new IPEndPoint(IPAddress.Loopback, await relay.StartAsync(0));
                string roomId = RelayRoom.NewRoomCode();
                var hostRoom = new RelayRoom(roomId) { RelayEndPoint = target, AllowDirect = false };
                server.AddListener(TransportType.Tcp, hostRoom.Tcp, 0);
                await server.StartAsync(0);
                clientConfig.TransportFactories[TransportType.Tcp] = new RelayRoom(roomId) { AllowDirect = false }.Tcp;
            }
            else {
                target = new IPEndPoint(IPAddress.Loopback, await server.StartAsync(0));
            }

            using var client = new NetClient(clientConfig);
            var reason = new TaskCompletionSource<DisconnectReason>(TaskCreationOptions.RunContinuationsAsynchronously);
            client.Disconnected += r => reason.TrySetResult(r);
            using var resumeReading = new ManualResetEventSlim();
            client.DataReceived += (channel, data) => resumeReading.Wait(TimeSpan.FromSeconds(10));
            await client.ConnectAsync(target);
            var connected = await WithTimeout(opened.Task);

            // The client stops reading, so the server's queue is busy when the kick comes.
            var payload = new byte[64 * 1024];
            Task? blocked = null;
            int maxSends = relayed ? 64 : 1024; // a relay buffers without pushing back
            for (int i = 0; i < maxSends && blocked == null; i++) {
                var send = connected.SendAsync(0, payload).AsTask();
                if (await Task.WhenAny(send, Task.Delay(100)) != send) {
                    blocked = send;
                }
            }

            var kick = connected.DisconnectAsync();
            await Task.Delay(50);
            resumeReading.Set();
            await WithTimeout(kick.ContinueWith(_ => true, TaskScheduler.Default));

            Assert.Equal(DisconnectReason.Kicked, await WithTimeout(reason.Task));
        }

        private static NetConfig Config() {
            var config = new NetConfig { EventDelivery = EventDelivery.Immediate, ConnectTimeoutMs = 15000, PingIntervalMs = 200 };
            config.Channels[0] = new ChannelConfig(TransportType.Tcp);
            return config;
        }

        private static async Task<T> WithTimeout<T>(Task<T> task) {
            Assert.Same(task, await Task.WhenAny(task, Task.Delay(10_000)));
            return await task;
        }
    }
}
