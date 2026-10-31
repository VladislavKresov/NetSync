using System;
using System.Threading;
using System.Threading.Tasks;
using NetSync.Diagnostics;

namespace NetSync.Internal {
    /// <summary>Fire-and-forget helpers that never leave a failure unobserved.</summary>
    internal static class TaskExtensions {
        private static readonly Action<Task> ReadException = task => _ = task.Exception;

        /// <summary>
        /// For a task nobody awaits any more: its failure is marked observed, so it never
        /// surfaces as TaskScheduler.UnobservedTaskException. Runs on the default scheduler —
        /// never on a captured one (a game engine's main-thread scheduler, say).
        /// </summary>
        public static void ObserveFaults(this Task task) {
            task.ContinueWith(ReadException, CancellationToken.None,
                              TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
                              TaskScheduler.Default);
        }

        /// <summary>
        /// For a send nobody waits for (acks, retransmissions, control messages): free when it
        /// completed synchronously — the common case — otherwise a failure is logged at Debug.
        /// </summary>
        public static void Forget(this ValueTask task, INetLogger logger) {
            if (task.IsCompletedSuccessfully) {
                return;
            }
            _ = ObserveAsync(task, logger);
        }

        private static async Task ObserveAsync(ValueTask task, INetLogger logger) {
            try {
                await task.ConfigureAwait(false);
            }
            catch (Exception ex) {
                logger.Debug($"Background send failed: {ex.Message}");
            }
        }
    }
}
