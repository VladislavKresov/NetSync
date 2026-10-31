using System;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NetSync.Peers;
using NetSync.Pipeline.Security;
using Xunit;

namespace NetSync.Tests {
    /// <summary>Stage 13: every packet authenticated, replays rejected, links can't be hijacked.</summary>
    public class HeaderSecurityTests {
        private static readonly byte[] Psk = Encoding.ASCII.GetBytes("header-security-test-key-32bytes");

        // ------------------------------------------------------------ unit level

        public static TheoryData<CipherSuite> Suites() {
            var suites = new TheoryData<CipherSuite> { CipherSuite.AesCbcHmacSha256 };
            if (CipherSuites.GcmSupported) {
                suites.Add(CipherSuite.AesGcm);
            }
            return suites;
        }

        private static (LinkSecurity client, LinkSecurity server) Pair(TransportType slot = TransportType.Udp,
                                                                       CipherSuite suite = CipherSuite.AesCbcHmacSha256) {
            var clientCrypto = ConnectionCrypto.FromPresharedKey(Psk, 99, suite, isClient: true);
            var serverCrypto = ConnectionCrypto.FromPresharedKey(Psk, 99, suite, isClient: false);
            return (clientCrypto.CreateLinkSecurity(slot), serverCrypto.CreateLinkSecurity(slot));
        }

        private static byte[]? Open(LinkSecurity security, byte[] envelope) {
            return security.TryUnwrap(envelope, 0, envelope.Length, out int offset, out int count)
                ? envelope.AsSpan(offset, count).ToArray()
                : null;
        }

        [Theory]
        [MemberData(nameof(Suites))]
        public void Envelope_Roundtrips_And_Authenticates_The_Whole_Message(CipherSuite suite) {
            var (client, server) = Pair(TransportType.Udp, suite);
            var inner = new byte[] { PeerProtocol.MsgRelAck, 3, 0, 7, 0xFF, 0xFF, 0xFF, 0xFF };
            var envelope = client.Wrap(inner.AsSpan(0, 1), inner.AsSpan(1));
            Assert.Equal(inner, Open(server, envelope));

            var tampered = client.Wrap(inner.AsSpan(0, 1), inner.AsSpan(1));
            tampered[^1] ^= 0x01; // flip one bit of the ACK mask
            Assert.Null(Open(server, tampered));
        }

        [Theory]
        [MemberData(nameof(Suites))]
        public void Replays_Are_Rejected_But_Reordering_Is_Accepted(CipherSuite suite) {
            var (client, server) = Pair(TransportType.Udp, suite);
            var packets = Enumerable.Range(0, 10).Select(i => client.Wrap(ReadOnlySpan<byte>.Empty, new byte[] { 0x10, 1, (byte)i })).ToArray();

            // Out of order delivery (UDP) is fine…
            foreach (int i in new[] { 0, 2, 1, 5, 3, 4, 9, 6, 8, 7 }) {
                Assert.NotNull(Open(server, packets[i]));
            }
            // …but every replay is rejected.
            foreach (var packet in packets) {
                Assert.Null(Open(server, packet));
            }
        }

        [Fact]
        public void Packets_Older_Than_The_Window_Are_Rejected() {
            var (client, server) = Pair();
            var old = client.Wrap(ReadOnlySpan<byte>.Empty, new byte[] { 0x10, 1 });
            for (int i = 0; i < LinkSecurity.ReplayWindowSize + 10; i++) {
                Assert.NotNull(Open(server, client.Wrap(ReadOnlySpan<byte>.Empty, new byte[] { 0x10, 1 })));
            }
            Assert.Null(Open(server, old)); // never seen, but beyond the window: can't prove it isn't a replay
        }

        [Theory]
        [MemberData(nameof(Suites))]
        public void Envelopes_Cannot_Be_Moved_Between_Links_Or_Reflected(CipherSuite suite) {
            var (udpClient, udpServer) = Pair(TransportType.Udp, suite);
            var (_, tcpServer) = Pair(TransportType.Tcp, suite);
            var envelope = udpClient.Wrap(ReadOnlySpan<byte>.Empty, new byte[] { 0x10, 1, 2 });

            Assert.Null(Open(tcpServer, (byte[])envelope.Clone()));   // UDP packet replayed onto TCP
            var (_, otherClientReceiver) = Pair(TransportType.Udp, suite);
            var reflectedReceiver = ConnectionCrypto.FromPresharedKey(Psk, 99, suite, isClient: true).CreateLinkSecurity(TransportType.Udp);
            Assert.Null(Open(reflectedReceiver, (byte[])envelope.Clone())); // client's packet reflected back to a client
            Assert.NotNull(Open(udpServer, envelope));
            _ = otherClientReceiver;
        }

        // ------------------------------------------------------------ end to end

        private sealed class Harness : IDisposable {
            public readonly InMemoryNetwork StreamNet = new InMemoryNetwork();
            public readonly InMemoryNetwork DatagramNet;
            public NetServer Server = null!;
            public NetClient Client = null!;
            public readonly ConcurrentQueue<byte[]> UdpCaptured = new ConcurrentQueue<byte[]>();

            public Harness(double datagramLoss = 0) {
                DatagramNet = new InMemoryNetwork(datagramLoss, seed: 3);
                DatagramNet.ClientToServerTap = packet => UdpCaptured.Enqueue(packet);
            }

            public NetConfig Config(bool encrypted) {
                var config = new NetConfig { EventDelivery = EventDelivery.Immediate, ConnectTimeoutMs = 10_000 };
                config.TransportFactories[TransportType.Tcp] = StreamNet.Factory;
                config.TransportFactories[TransportType.Udp] = DatagramNet.Factory;
                config.Channels[0] = new ChannelConfig(TransportType.Tcp);
                config.Channels[1] = new ChannelConfig(TransportType.Udp);
                config.Channels[2] = new ChannelConfig(TransportType.Udp, ReliabilityMode.ReliableOrdered);
                if (encrypted) {
                    config.Encryption = new EncryptionConfig { Mode = KeyExchangeMode.PresharedKey, PresharedKey = Psk };
                }
                return config;
            }

            public InMemoryClientTransport ClientUdpLink => DatagramNet.Clients.First();

            public void Dispose() {
                Client?.Dispose();
                Server?.Dispose();
            }
        }

        [Fact]
        public async Task Replayed_Forged_And_Tampered_Packets_Never_Reach_The_Application() {
            using var h = new Harness();
            h.Server = new NetServer(h.Config(encrypted: true));
            int received = 0;
            h.Server.DataReceived += (_, channel, _) => {
                if (channel == 1) Interlocked.Increment(ref received);
            };
            int port = await h.Server.StartAsync(0);
            h.Client = new NetClient(h.Config(encrypted: true));
            await h.Client.ConnectAsync("127.0.0.1", port);

            while (h.UdpCaptured.TryDequeue(out _)) { } // forget the handshake
            await h.Client.SendAsync(1, new byte[] { 1, 2, 3 });
            await WaitUntilAsync(() => Volatile.Read(ref received) == 1);
            // The data packet (other enveloped traffic — time sync — may share the link).
            var genuine = h.UdpCaptured.Single(p => p[0] == PeerProtocol.MsgSecure &&
                                                    p.Length > LinkSecurity.HeaderSize + 1 &&
                                                    p[LinkSecurity.HeaderSize] == PeerProtocol.MsgData);

            var link = h.ClientUdpLink;
            for (int i = 0; i < 5; i++) {
                link.InjectToServer((byte[])genuine.Clone());                    // replay
            }
            link.InjectToServer(new byte[] { PeerProtocol.MsgData, 1, 9, 9, 9 }); // forged plaintext
            var tampered = (byte[])genuine.Clone();
            tampered[^1] ^= 0xFF;
            link.InjectToServer(tampered);                                        // tampered payload
            await Task.Delay(300);

            Assert.Equal(1, Volatile.Read(ref received));
        }

        [Fact]
        public async Task Forged_Acks_Cannot_Make_A_Reliable_Channel_Lose_Data() {
            // The attack the review found: ACKs travel outside the payload, so without
            // packet authentication anyone who can spoof the server's address can "ack"
            // packets that were actually lost — the sender forgets them, the ordered
            // channel stalls forever. With the envelope the forged ACKs are ignored.
            Assert.False(await RunTransferUnderAckForgeryAsync(encrypted: false), "sanity: unauthenticated links are vulnerable");
            Assert.True(await RunTransferUnderAckForgeryAsync(encrypted: true), "authenticated link must deliver despite forged ACKs");
        }

        private static async Task<bool> RunTransferUnderAckForgeryAsync(bool encrypted) {
            using var h = new Harness(datagramLoss: 0.10);
            // Attacker on the path: sees every packet, and the instant one is lost it "acks" it
            // on the server's behalf (headers are readable even when the payload isn't). The
            // forged ACK always reaches the client before any real one could trigger a resend,
            // so the outcome is deterministic, whatever the machine's load.
            int forged = 0;
            h.DatagramNet.ClientToServerDropped = (link, packet) => {
                int at = packet[0] == PeerProtocol.MsgSecure ? LinkSecurity.HeaderSize : 0;
                if (packet.Length < at + 5 || packet[at] != PeerProtocol.MsgRelData || packet[at + 2] != 2) {
                    return;
                }
                var ack = new byte[8];
                ack[0] = PeerProtocol.MsgRelAck;
                ack[1] = 2;
                ack[2] = packet[at + 3]; // the lost packet's seq
                ack[3] = packet[at + 4];
                link.InjectFromServer(ack);
                Interlocked.Increment(ref forged);
            };
            h.Server = new NetServer(h.Config(encrypted));
            var done = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var payload = new byte[100_000];
            new Random(4).NextBytes(payload);
            h.Server.DataReceived += (_, channel, data) => {
                if (channel == 2) done.TrySetResult(data.AsSpan().SequenceEqual(payload));
            };
            int port = await h.Server.StartAsync(0);
            h.Client = new NetClient(h.Config(encrypted));
            await h.Client.ConnectAsync("127.0.0.1", port);

            _ = h.Client.SendAsync(2, payload).AsTask();
            bool delivered = await Task.WhenAny(done.Task, Task.Delay(4000)) == done.Task && await done.Task;
            Assert.True(Volatile.Read(ref forged) > 0, "sanity: the lossy network must have dropped data packets");
            return delivered;
        }

        [Fact]
        public async Task Sniffed_Session_Token_Cannot_Hijack_A_Bound_Link() {
            using var h = new Harness();
            h.Server = new NetServer(h.Config(encrypted: false)); // even without encryption
            h.Server.DataReceived += (conn, channel, data) => _ = conn.SendAsync(channel, data);
            int port = await h.Server.StartAsync(0);
            h.Client = new NetClient(h.Config(encrypted: false));
            var echoes = 0;
            h.Client.DataReceived += (channel, _) => {
                if (channel == 1) Interlocked.Increment(ref echoes);
            };
            await h.Client.ConnectAsync("127.0.0.1", port);

            // Attacker sniffed the client's UDP Hello and replays it from its own endpoint.
            var hello = h.UdpCaptured.First(p => p[0] == PeerProtocol.MsgHello);
            var attacker = (InMemoryClientTransport)h.DatagramNet.Factory.CreateClient(new Transports.TransportSettings());
            int attackerGotData = 0;
            attacker.DataReceived += data => {
                if (data.Length > 0 && data[0] == PeerProtocol.MsgData) Interlocked.Increment(ref attackerGotData);
            };
            await attacker.ConnectAsync(new System.Net.IPEndPoint(System.Net.IPAddress.Loopback, port));
            await attacker.SendAsync(hello);
            await Task.Delay(200);

            await h.Client.SendAsync(1, new byte[] { 5 });
            await WaitUntilAsync(() => Volatile.Read(ref echoes) == 1);
            Assert.Equal(0, Volatile.Read(ref attackerGotData)); // traffic still goes to the real client
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
