using System;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using NetSync.Diagnostics;

namespace NetSync.Transports {
    /// <summary>
    /// The accept loop of every TCP-based listener (TCP, WebSocket, relay). One misbehaving
    /// client never stops the listener: each is served on its own task, and a client whose
    /// setup throws (e.g. reset right after the accept) is closed and forgotten. Failing
    /// accepts (out of file descriptors…) back off instead of spinning.
    /// </summary>
    internal static class TcpAcceptLoop {
        private const int MaxBackoffMs = 1000;

        /// <param name="listener">A started listener; stopping it ends the loop.</param>
        /// <param name="serve">Runs for each accepted client, on its own task. Owns the client.</param>
        /// <param name="logger">Diagnostics.</param>
        /// <param name="owner">Name for log lines.</param>
        /// <param name="ct">Ends the loop.</param>
        public static async Task RunAsync(TcpListener listener, Func<TcpClient, Task> serve, INetLogger logger, string owner, CancellationToken ct) {
            int failures = 0;
            while (!ct.IsCancellationRequested) {
                TcpClient client;
                try {
                    client = await listener.AcceptTcpClientAsync().ConfigureAwait(false);
                    failures = 0;
                }
                catch (Exception) when (ct.IsCancellationRequested) {
                    return;
                }
                catch (ObjectDisposedException) {
                    return; // listener stopped
                }
                catch (Exception ex) {
                    logger.Error($"{owner} accept: {ex.Message}");
                    try {
                        await Task.Delay(Math.Min(MaxBackoffMs, 10 << Math.Min(failures++, 7)), ct).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException) {
                        return;
                    }
                    continue;
                }
                _ = Task.Run(() => ServeAsync(client, serve, logger, owner));
            }
        }

        private static async Task ServeAsync(TcpClient client, Func<TcpClient, Task> serve, INetLogger logger, string owner) {
            try {
                await serve(client).ConfigureAwait(false);
            }
            catch (Exception ex) {
                logger.Debug($"{owner}: dropping a client: {ex.Message}");
                client.Dispose();
            }
        }
    }
}
