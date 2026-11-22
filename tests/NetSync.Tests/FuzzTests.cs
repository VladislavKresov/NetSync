using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NetSync.Diagnostics;
using NetSync.Internal;
using NetSync.Discovery;
using NetSync.Peers;
using NetSync.Pipeline.Compression;
using NetSync.Pipeline.Fragmentation;
using NetSync.Pipeline.Reliability;
using NetSync.Pipeline.Security;
using NetSync.Relay;
using NetSync.Transports;
using NetSync.Transports.Tcp;
using NetSync.Transports.Udp;
using NetSync.Transports.WebSockets;
using Xunit;

namespace NetSync.Tests {
    /// <summary>
    /// Every parser that reads bytes from the network, fed mutated valid messages and random
    /// bytes. A malformed input must be refused — never an exception, a hang or an oversized
    /// allocation. Seeded: a failure names its input in hex and repeats on every run.
    /// </summary>
    public class FuzzTests {
        private const int Iterations = 20_000;

        /// <summary>Valid messages, bent in the ways corrupt or hostile input tends to be.</summary>
        private static IEnumerable<byte[]> Mutations(IReadOnlyList<byte[]> seeds, int seed, int count = Iterations) {
            var random = new Random(seed);
            for (int i = 0; i < count; i++) {
                var source = seeds[random.Next(seeds.Count)];
                var data = (byte[])source.Clone();
                switch (random.Next(7)) {
                    case 0: // flip a few bits
                        for (int k = random.Next(1, 9); k > 0 && data.Length > 0; k--) {
                            data[random.Next(data.Length)] ^= (byte)(1 << random.Next(8));
                        }
                        break;
                    case 1: // truncate
                        data = data.AsSpan(0, random.Next(data.Length + 1)).ToArray();
                        break;
                    case 2: // trailing garbage
                        data = data.Concat(Bytes(random, random.Next(1, 64))).ToArray();
                        break;
                    case 3: // boundary values at a random spot (lengths, counts, indices)
                        if (data.Length > 0) {
                            byte[] values = { 0x00, 0x01, 0x7F, 0x80, 0xFE, 0xFF };
                            int at = random.Next(data.Length);
                            for (int k = 0; k < random.Next(1, 5) && at + k < data.Length; k++) {
                                data[at + k] = values[random.Next(values.Length)];
                            }
                        }
                        break;
                    case 4: // a whole length-like region set huge
                        if (data.Length >= 4) {
                            int at = random.Next(data.Length - 3);
                            data[at] = 0x7F;
                            data[at + 1] = data[at + 2] = data[at + 3] = 0xFF;
                        }
                        break;
                    case 5: // the right first byte, random rest
                        if (data.Length > 0) {
                            var rest = Bytes(random, random.Next(0, 2 * data.Length + 2));
                            data = new[] { data[0] }.Concat(rest).ToArray();
                        }
                        break;
                    default: // nothing valid at all
                        data = Bytes(random, random.Next(0, 300));
                        break;
                }
                yield return data;
            }
        }

        private static byte[] Bytes(Random random, int length) {
            var bytes = new byte[length];
            random.NextBytes(bytes);
            return bytes;
        }

        private static void Fuzz(IReadOnlyList<byte[]> seeds, int seed, Action<byte[]> parse) {
            foreach (var input in Mutations(seeds, seed)) {
                try {
                    parse(input);
                }
                catch (Exception ex) {
                    throw new Xunit.Sdk.XunitException(
                        $"{ex.GetType().Name} on input {Convert.ToHexString(input)}: {ex.Message}{Environment.NewLine}{ex.StackTrace}");
                }
            }
        }

        [Fact]
        public void Handshake_Messages() {
            var key = Enumerable.Range(0, 32).Select(i => (byte)i).ToArray();
            var seeds = new[] {
                PeerProtocol.EncodeHello(1, 2, KeyExchangeMode.Ecdh, 3, key, null, 4, 5, 0),
                PeerProtocol.EncodeHello(1, 2, KeyExchangeMode.None, 0, Array.Empty<byte>(), new byte[16], 4, 5, 0),
                PeerProtocol.EncodeWelcome(9, CipherSuite.AesGcm, 0, key, key),
                PeerProtocol.EncodeWelcome(9, CipherSuite.None, 0, null, null)
            };
            Fuzz(seeds, 1, input => {
                if (PeerProtocol.TryParseHello(input, 0, input.Length, out var hello)) {
                    Assert.InRange(hello.PublicKeyOffset + hello.PublicKeyLength, 0, input.Length);
                    Assert.InRange(hello.CookieOffset + hello.CookieLength, 0, input.Length);
                }
                PeerProtocol.TryParseWelcome(input, 0, input.Length, out _);
                PeerProtocol.TryReadHelloVersion(input, out _);
            });
        }

        [Fact]
        public void Relay_Protocol() {
            var seeds = new[] {
                RelayProtocol.EncodeHello("ROOM-1", "access-key"),
                RelayProtocol.EncodeHello("R", ""),
                new byte[] { 4, (byte)'R', (byte)'O', (byte)'O', (byte)'M' },
                new byte[] { 4, 127, 0, 0, 1, 0x1F, 0x90 }
            };
            Fuzz(seeds, 2, input => {
                RelayProtocol.TryReadHello(input.AsSpan(Math.Min(1, input.Length)), out _, out _, out _);
                RelayProtocol.TryReadRoom(input, out _, out int consumed);
                Assert.InRange(consumed, 0, input.Length);
                if (RelayProtocol.TryReadEndPoint(input, out var endPoint, out consumed)) {
                    Assert.InRange(consumed, 1, input.Length);
                    Assert.True(endPoint != null || consumed == 1); // length 0 encodes "no address"
                }
            });
        }

        [Fact]
        public void Lan_Discovery() {
            var seeds = new[] { DiscoveryMessage.Encode("my-game", 7777), DiscoveryMessage.Encode("g", 1, new byte[] { 1, 2, 3 }) };
            Fuzz(seeds, 3, input => DiscoveryMessage.TryDecode(input, out _, out _, out _));
        }

        [Fact]
        public void Fragments_And_Their_Reassembly() {
            var seeds = PacketFragmenter.Fragment(new byte[5000], 77, 1000);
            var buffer = new FragmentBuffer { MaxPacketBytes = 64 * 1024, MaxBufferedBytes = 256 * 1024, MaxFragmentsPerPacket = 100 };
            Fuzz(seeds, 4, input => {
                if (PacketFragmenter.TryUnwrap(input, out uint sequenceId, out uint index, out uint total, out var payload)) {
                    if (buffer.TryAddFragment(sequenceId, index, total, payload.ToArray(), out var complete) && complete != null) {
                        Assert.True(complete.Length <= buffer.MaxPacketBytes); // an exact-size array of our own
                    }
                }
                Assert.True(buffer.BufferedBytes <= buffer.MaxBufferedBytes, $"{buffer.BufferedBytes} bytes buffered");
            });
        }

        [Fact]
        public void Typed_Message_Reader() {
            // Applications read their messages with NetReader: malformed input may only end in the
            // documented EndOfStreamException, never another exception or a huge allocation.
            var writer = new NetSync.Serialization.NetWriter(16);
            writer.WriteString("hello").WriteVarUInt(300).WriteInt32(-5).WriteBytes(new byte[40]).WriteString(new string('ж', 50));
            var seeds = new[] { writer.ToArray() };
            var random = new Random(13);
            Fuzz(seeds, 14, input => {
                var reader = new NetSync.Serialization.NetReader(input);
                try {
                    for (int i = 0; i < 12; i++) {
                        switch (random.Next(6)) {
                            case 0: reader.ReadString(); break;
                            case 1: reader.ReadVarUInt(); break;
                            case 2: reader.ReadInt32(); break;
                            case 3: Assert.True(reader.ReadBytes().Length <= input.Length); break;
                            case 4: reader.ReadVarInt(); break;
                            default: reader.ReadByte(); break;
                        }
                    }
                }
                catch (EndOfStreamException) {
                    // the documented way a malformed message ends
                }
            });
        }

        [Fact]
        public void Udp_Batches() {
            var seeds = new[] {
                new byte[] { 0x12, 2, 1, 2, 3, 4, 5, 6 },
                new byte[] { 0x12, 0x81, 0x01, 1 },
                new byte[] { 0x12, 0 }
            };
            Fuzz(seeds, 5, input => {
                int delivered = 0;
                DatagramBatcher.Unpack(input, input.Length, 64, input, (source, buffer, offset, count) => {
                    Assert.Same(source, buffer);
                    Assert.True(offset >= 1 && count >= 0 && offset + count <= source.Length, $"{offset}+{count} of {source.Length}");
                    delivered++;
                });
                Assert.True(delivered <= 64);
            });
        }

        [Fact]
        public void Tcp_Framing() {
            var seeds = new[] {
                new byte[] { 0x10, 0, 0, 0, 3, 1, 2, 3, 0x10, 0, 0, 0, 0 },
                new byte[] { 0x01, 0, 0, 0, 0, 0, 0, 0, 7 },
                new byte[] { 0x02, 0, 0, 0, 0, 0, 0, 0, 7, 0x10, 0, 0, 0, 1, 9 }
            };
            Fuzz(seeds, 6, input => {
                using var reader = new TcpFrameReader();
                int fed = 0;
                // Fed in uneven pieces, as a stream delivers them.
                while (fed < input.Length) {
                    var memory = reader.GetReadMemory();
                    int piece = Math.Min(Math.Min(memory.Length, input.Length - fed), 1 + fed % 7);
                    input.AsSpan(fed, piece).CopyTo(memory.Span);
                    reader.Advance(piece);
                    fed += piece;
                    TcpFrameReader.FrameKind kind;
                    while ((kind = reader.TryRead(4096, out int offset, out int length, out _)) != TcpFrameReader.FrameKind.None) {
                        if (kind == TcpFrameReader.FrameKind.Invalid) {
                            return; // the link would be dropped here
                        }
                        if (kind == TcpFrameReader.FrameKind.Data) {
                            Assert.True(length <= 4096 && offset + length <= reader.Buffer.Length);
                        }
                    }
                }
            });
        }

        [Theory]
        [InlineData(CipherSuite.AesCbcHmacSha256)]
        [InlineData(CipherSuite.AesGcm)]
        public void Authenticated_Envelopes(CipherSuite suite) {
            if (suite == CipherSuite.AesGcm && !CipherSuites.GcmSupported) {
                return;
            }
            var psk = Encoding.ASCII.GetBytes("fuzz-envelopes-preshared-key-32b");
            using var sender = ConnectionCrypto.FromSecret(TestKeys.Secret(psk, 1), suite, isClient: true);
            using var receiver = ConnectionCrypto.FromSecret(TestKeys.Secret(psk, 1), suite, isClient: false);
            var wrapping = sender.CreateLinkSecurity(TransportType.Udp);
            var unwrapping = receiver.CreateLinkSecurity(TransportType.Udp);
            var seeds = Enumerable.Range(0, 8).Select(i => wrapping.Wrap(new byte[] { 0x10, 1 }, new byte[i * 37])).ToArray();
            var originals = new HashSet<string>(seeds.Select(Convert.ToHexString));
            Fuzz(seeds, 7, input => {
                var result = unwrapping.Unwrap(input, 0, input.Length, out int offset, out int count);
                if (result == LinkSecurity.UnwrapResult.Ok) {
                    // Only an untouched envelope may verify (and then only once: replay protection).
                    Assert.Contains(Convert.ToHexString(input), originals);
                    Assert.True(offset >= 0 && offset + count <= input.Length);
                }
            });
        }

        [Fact]
        public void Channel_Decoding_Compressed_And_Encrypted() {
            var psk = Encoding.ASCII.GetBytes("fuzz-channel-transform-psk-32-by");
            using var crypto = ConnectionCrypto.FromSecret(TestKeys.Secret(psk, 2), CipherSuite.AesCbcHmacSha256, isClient: true);
            using var peer = ConnectionCrypto.FromSecret(TestKeys.Secret(psk, 2), CipherSuite.AesCbcHmacSha256, isClient: false);
            var channel = new ChannelConfig(TransportType.Tcp) { Compression = true, Encryption = true };
            var text = Encoding.UTF8.GetBytes(string.Concat(Enumerable.Repeat("compressible payload ", 200)));
            var seeds = new List<byte[]>();
            foreach (var size in new[] { 0, 10, 600, text.Length }) {
                var encoded = ChannelTransform.Encode(channel, crypto, text.AsSpan(0, size), out int length);
                seeds.Add(encoded.AsSpan(0, length).ToArray());
                Pooled.Return(encoded);
            }
            const int maxMessage = 64 * 1024;
            Fuzz(seeds, 8, input => {
                var decoded = ChannelTransform.Decode(channel, peer, input, 0, input.Length, maxMessage, NullNetLogger.Instance, out int length);
                if (decoded != null) {
                    Assert.True(length <= maxMessage);
                    Pooled.Return(decoded);
                }
                var inflated = Compressor.Decompress(input, 0, input.Length, maxMessage, NullNetLogger.Instance, out length);
                if (inflated != null) {
                    Assert.True(length <= maxMessage, $"inflated to {length} bytes");
                    Pooled.Return(inflated);
                }
            });
        }

        [Fact]
        public void Reliability_Packets_Leave_The_Endpoint_Working() {
            // Garbage reaches the reliability layer whenever no keys are configured (with keys the
            // envelope refuses it first — see Authenticated_Envelopes). Without authentication
            // injected packets can legitimately confuse a channel's state; what must hold is that
            // nothing throws, and that bogus acknowledgements leave the sender able to deliver.
            var network = new VirtualNetwork();
            var channels = new Dictionary<byte, ChannelConfig> {
                [1] = new ChannelConfig(TransportType.Udp, ReliabilityMode.ReliableOrdered),
                [2] = new ChannelConfig(TransportType.Udp, ReliabilityMode.Reliable),
                [3] = new ChannelConfig(TransportType.Udp, ReliabilityMode.UnreliableSequenced),
                [4] = new ChannelConfig(TransportType.Udp, ReliabilityMode.UnreliableFragmented)
            };
            var toReceiver = new VirtualPath(network, 1);
            var toSender = new VirtualPath(network, 2);
            var received = new List<byte[]>();
            var sender = network.Endpoint(channels, () => toReceiver, (_, _, _, _) => { });
            var receiver = network.Endpoint(channels, () => toSender, (channel, buffer, offset, count) => {
                if (channel == 1 && count == 5 && buffer[offset] == 0xAB) {
                    received.Add(buffer.AsSpan(offset, count).ToArray());
                }
            });
            toReceiver.Target = receiver;
            toSender.Target = sender;

            var seeds = new[] {
                new byte[] { 0x20, 0, 1, 0, 5, 9, 9 },        // RelData, ordered channel
                new byte[] { 0x20, 1, 4, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2, 7 }, // fragment flag
                new byte[] { 0x21, 1, 0, 9, 0xFF, 0xFF, 0xFF, 0xFF }, // RelAck
                new byte[] { 0x20, 0, 3, 0x80, 0, 1 }
            };
            Fuzz(seeds, 9, input => {
                if (input.Length > 0 && input[0] == 0x21) {
                    receiver.HandleAck(input, 0, input.Length);
                    sender.HandleAck(input, 0, input.Length);
                }
                else {
                    receiver.HandleRelData(input, 0, input.Length);
                }
            });

            // The sender, after all those bogus acks, delivers to a receiver that saw no garbage.
            var clean = network.Endpoint(channels, () => toSender, (channel, buffer, offset, count) => {
                if (channel == 1 && count == 5 && buffer[offset] == 0xAB) {
                    received.Add(buffer.AsSpan(offset, count).ToArray());
                }
            });
            toReceiver.Target = clean;
            byte next = 0;
            Assert.True(network.RunUntil(() => received.Count == 100, 30_000, () => {
                while (next < 100 && sender.TrySendReliable(1, new byte[] { 0xAB, next, 0, 0, 0 })) {
                    next++; // as fast as the congestion window admits
                }
            }, sender, clean), $"{received.Count} of 100 delivered");
            Assert.Equal(Enumerable.Range(0, 100), received.Select(message => (int)message[1]));
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task A_Server_Fed_Garbage_Keeps_Serving(bool encrypted) {
            NetConfig Config() {
                var config = new NetConfig { EventDelivery = EventDelivery.Immediate };
                config.Channels[0] = new ChannelConfig(TransportType.Tcp);
                config.Channels[1] = new ChannelConfig(TransportType.Udp, ReliabilityMode.ReliableOrdered);
                if (encrypted) {
                    config.Encryption = new EncryptionConfig {
                        Mode = KeyExchangeMode.PresharedKey,
                        PresharedKey = Encoding.ASCII.GetBytes("fuzz-server-garbage-psk-32-bytes")
                    };
                }
                return config;
            }
            using var server = new NetServer(Config());
            server.MessageReceived += (connection, channel, data) => _ = connection.SendAsync(channel, data);
            int port = await server.StartAsync(0);
            var target = new System.Net.IPEndPoint(System.Net.IPAddress.Loopback, port);

            // Datagrams: mutated Hellos and data packets, and noise.
            var hello = PeerProtocol.EncodeHello(5, 6, KeyExchangeMode.None, 0, Array.Empty<byte>(), null, 0, 0, 0);
            var seeds = new[] { new byte[] { 0x10 }.Concat(hello).ToArray(), new byte[] { 0x10, 0x20, 0, 1, 0, 0, 7 },
                                new byte[] { 0x12, 3, 0x10, 0x10, 1 }, new byte[] { 0x01, 0, 0, 0, 0, 0, 0, 0, 1 } };
            using (var udp = new System.Net.Sockets.UdpClient(System.Net.Sockets.AddressFamily.InterNetwork)) {
                foreach (var datagram in Mutations(seeds, 11, count: 5000)) {
                    udp.Send(datagram, datagram.Length, target);
                }
            }
            // Streams: a few connections that send garbage and go away.
            var random = new Random(12);
            for (int i = 0; i < 20; i++) {
                using var tcp = new System.Net.Sockets.TcpClient();
                await tcp.ConnectAsync(target.Address, target.Port);
                var junk = Bytes(random, random.Next(1, 4000));
                try {
                    await tcp.GetStream().WriteAsync(junk);
                }
                catch (IOException) {
                    // the server hung up on us: fine
                }
            }

            // An honest client is served as if nothing happened.
            using var client = new NetClient(Config());
            int echoes = 0;
            client.MessageReceived += (_, _) => Interlocked.Increment(ref echoes);
            await client.ConnectAsync("127.0.0.1", port);
            for (int i = 0; i < 50; i++) {
                await client.SendAsync((byte)(i % 2), new byte[32]);
            }
            Assert.True(await Eventually.HoldsAsync(() => Volatile.Read(ref echoes) == 50), $"{echoes} of 50 echoes");
        }

        [Fact]
        public async Task WebSocket_Upgrade_Requests() {
            var seeds = new[] {
                Encoding.ASCII.GetBytes("GET /netsync HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n" +
                                        "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n"),
                Encoding.ASCII.GetBytes("GET / HTTP/1.1\r\n\r\n")
            };
            foreach (var input in Mutations(seeds, 10, count: 2000)) {
                using var stream = new DuplexStream(input);
                using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
                try {
                    await WebSocketHandshake.AcceptAsync(stream, "/netsync", Array.Empty<string>(), timeout.Token);
                }
                catch (Exception ex) when (ex is IOException || ex is OperationCanceledException) {
                    // a refused or truncated request ends the link: fine
                }
                catch (Exception ex) {
                    throw new Xunit.Sdk.XunitException($"{ex.GetType().Name} on input {Convert.ToHexString(input)}: {ex.Message}");
                }
                Assert.False(timeout.IsCancellationRequested, $"handshake hung on {Convert.ToHexString(input)}");
            }
        }

        /// <summary>Reads a fixed request, swallows whatever is written back, then reports end of stream.</summary>
        private sealed class DuplexStream : Stream {
            private readonly MemoryStream _input;
            public DuplexStream(byte[] input) => _input = new MemoryStream(input);
            public override bool CanRead => true;
            public override bool CanSeek => false;
            public override bool CanWrite => true;
            public override long Length => throw new NotSupportedException();
            public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
            public override void Flush() { }
            public override int Read(byte[] buffer, int offset, int count) => _input.Read(buffer, offset, count);
            public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
            public override void SetLength(long value) => throw new NotSupportedException();
            public override void Write(byte[] buffer, int offset, int count) { }
        }
    }
}
