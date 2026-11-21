using System;
using System.Threading;
using System.Threading.Tasks;
using NetSync;
using NetSync.Peers;

/// <summary>
/// Server and client in one process, on an encrypted connection (the X25519 handshake), echoing
/// over a TCP channel and a reliable UDP channel. CI publishes Echo with NativeAOT and runs this:
/// it proves the trimmed native binary really connects, encrypts and delivers, not just that it builds.
/// </summary>
internal static class SelfTest {
    private const int PerChannel = 100;

    public static async Task<int> RunAsync() {
        static NetConfig Config() {
            var config = new NetConfig {
                EventDelivery = EventDelivery.Immediate,
                Encryption = new EncryptionConfig {
                    Mode = KeyExchangeMode.PresharedKey,
                    PresharedKey = System.Text.Encoding.ASCII.GetBytes("echo-selftest-preshared-key-32-b")
                }
            };
            config.Channels[0] = new ChannelConfig(TransportType.Tcp);
            config.Channels[1] = new ChannelConfig(TransportType.Udp, ReliabilityMode.ReliableOrdered);
            return config;
        }

        try {
            using var server = new NetServer(Config());
            server.MessageReceived += (connection, channel, data) => _ = connection.SendAsync(channel, data);
            int port = await server.StartAsync(0);

            using var client = new NetClient(Config());
            int tcpEchoes = 0, udpEchoes = 0;
            client.MessageReceived += (channel, _) => Interlocked.Increment(ref channel == 0 ? ref tcpEchoes : ref udpEchoes);
            await client.ConnectAsync("127.0.0.1", port);

            var payload = new byte[256];
            for (int i = 0; i < PerChannel; i++) {
                await client.SendAsync(0, payload);
                await client.SendAsync(1, payload);
            }
            var deadline = DateTime.UtcNow.AddSeconds(30);
            while ((Volatile.Read(ref tcpEchoes) < PerChannel || Volatile.Read(ref udpEchoes) < PerChannel) && DateTime.UtcNow < deadline) {
                await Task.Delay(10);
            }
            bool ok = tcpEchoes == PerChannel && udpEchoes == PerChannel && client.CipherSuite != CipherSuite.None;
            Console.WriteLine($"selftest: {tcpEchoes}/{PerChannel} TCP and {udpEchoes}/{PerChannel} UDP echoes, suite {client.CipherSuite}: " +
                              (ok ? "OK" : "FAILED"));
            client.Disconnect();
            await server.StopAsync();
            return ok ? 0 : 1;
        }
        catch (Exception ex) {
            Console.WriteLine($"selftest FAILED: {ex}");
            return 1;
        }
    }
}
