using System;
using System.Net.Sockets;
using System.Threading.Tasks;
using Xunit;

namespace NetSync.Tests {
    internal static class Eventually {
        /// <summary>Waits for a condition instead of assuming how long it takes: a slow machine only waits longer.</summary>
        public static async Task<bool> HoldsAsync(Func<bool> condition, int timeoutMs = 15_000) {
            var patience = System.Diagnostics.Stopwatch.StartNew();
            while (!condition()) {
                if (patience.ElapsedMilliseconds > timeoutMs) {
                    return false;
                }
                await Task.Delay(10);
            }
            return true;
        }
    }

    internal static class TestUdp {
        /// <summary>
        /// Sends a raw datagram until a reply arrives. Loopback UDP does drop datagrams when a
        /// busy parallel test run fills socket buffers — a single unanswered send proves nothing.
        /// </summary>
        public static async Task<UdpReceiveResult> RequestAsync(UdpClient socket, byte[] datagram, int attempts = 10, int waitMs = 500) {
            Task<UdpReceiveResult>? receive = null;
            for (int i = 0; i < attempts; i++) {
                await socket.SendAsync(datagram, datagram.Length);
                receive ??= socket.ReceiveAsync();
                if (await Task.WhenAny(receive, Task.Delay(waitMs)) == receive) {
                    return await receive;
                }
            }
            throw new TimeoutException($"No reply after {attempts} datagrams");
        }
    }

    /// <summary>
    /// Many tests run at once, and Dispose()-style blocking calls park pool threads while the
    /// pool grows by only ~2 threads a second: start with enough threads that timeouts measure
    /// the library, not thread-pool starvation.
    /// </summary>
    internal static class TestEnvironment {
        [System.Runtime.CompilerServices.ModuleInitializer]
        internal static void RaiseThreadPoolMinimum() {
            System.Threading.ThreadPool.GetMinThreads(out int workers, out int io);
            System.Threading.ThreadPool.SetMinThreads(Math.Max(workers, 64), Math.Max(io, 64));
        }
    }
}
