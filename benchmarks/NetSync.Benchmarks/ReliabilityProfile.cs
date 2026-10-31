using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using NetSync.Diagnostics;
using NetSync.Pipeline.Reliability;
using NetSync.Peers;

namespace NetSync.Benchmarks {
    /// <summary>
    /// CPU cost of the reliability engine alone: two endpoints wired back to back in one
    /// thread (no sockets), ReliableOrdered, acks processed inline.
    /// `dotnet run -c Release -- reliability`
    /// </summary>
    internal static class ReliabilityProfile {
        public static void Run() {
            var channels = new Dictionary<byte, ChannelConfig> {
                [0] = new ChannelConfig(TransportType.Udp, ReliabilityMode.ReliableOrdered)
            };
            ReliableEndpoint? a = null, b = null;
            long delivered = 0;
            a = new ReliableEndpoint(channels, data => { Dispatch(b!, data); return default; },
                                     (ch, buf, off, cnt) => { }, () => 1, _ => { }, NullNetLogger.Instance);
            b = new ReliableEndpoint(channels, data => { Dispatch(a!, data); return default; },
                                     (ch, buf, off, cnt) => delivered++, () => 1, _ => { }, NullNetLogger.Instance);
            var payload = new byte[64];
            for (int round = 0; round < 3; round++) {
                delivered = 0;
                var sw = Stopwatch.StartNew();
                const int n = 1_000_000;
                for (int i = 0; i < n; i++) {
                    var send = a.SendAsync(0, payload);
                    if (!send.IsCompletedSuccessfully) {
                        send.AsTask().GetAwaiter().GetResult();
                    }
                }
                sw.Stop();
                Console.WriteLine($"reliable-ordered, inline: {n / sw.Elapsed.TotalSeconds:N0} msg/s ({sw.Elapsed.TotalMilliseconds * 1000 / n:F3} us/msg), delivered {delivered}");
            }
        }

        private static void Dispatch(ReliableEndpoint to, ReadOnlyMemory<byte> data) {
            var array = data.ToArray();
            if (array[0] == NetSync.Peers.PeerProtocol.MsgRelData) {
                to.HandleRelData(array, 0, array.Length);
            }
            else {
                to.HandleAck(array, 0, array.Length);
            }
        }
    }
}
