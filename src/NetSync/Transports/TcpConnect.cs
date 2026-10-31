using System;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace NetSync.Transports {
    /// <summary>Outgoing TCP connects that honor cancellation on every target.</summary>
    internal static class TcpConnect {
        /// <summary>
        /// Connects <paramref name="client"/>. Without this an unanswered SYN (a firewall that
        /// drops instead of refusing) blocks for the OS default — about 21 s on Windows —
        /// whatever the caller's timeout. netstandard2.1 has no cancellable overload: there the
        /// socket is closed on cancellation, which aborts the attempt.
        /// </summary>
        public static async Task ConnectAsync(TcpClient client, IPEndPoint endpoint, CancellationToken ct) {
#if NET8_0_OR_GREATER
            await client.ConnectAsync(endpoint, ct).ConfigureAwait(false);
#else
            using (ct.Register(state => ((TcpClient)state!).Dispose(), client)) {
                try {
                    await client.ConnectAsync(endpoint.Address, endpoint.Port).ConfigureAwait(false);
                }
                catch (Exception) when (ct.IsCancellationRequested) {
                    throw new OperationCanceledException(ct);
                }
            }
#endif
        }
    }
}
