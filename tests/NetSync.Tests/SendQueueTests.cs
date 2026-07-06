using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using NetSync.Diagnostics;
using NetSync.Sending;
using Xunit;

namespace NetSync.Tests {
    public class SendQueueTests {
        [Fact]
        public async Task Drains_In_Priority_Order() {
            var sent = new List<byte>();
            var gate = new SemaphoreSlim(0);
            var release = new SemaphoreSlim(0);

            using var queue = new SendQueue(async (data, length, ct) => {
                gate.Release();                       // signal: entered send
                await release.WaitAsync(ct);          // hold until test releases
                lock (sent) sent.Add(data[0]);
            }, NullNetLogger.Instance);

            queue.Start();

            // First job occupies the drain loop while we enqueue the rest.
            Assert.True(queue.TryEnqueue(new byte[] { 0 }, 1, false, SendPriority.Normal));
            await gate.WaitAsync();

            Assert.True(queue.TryEnqueue(new byte[] { 1 }, 1, false, SendPriority.Low));
            Assert.True(queue.TryEnqueue(new byte[] { 2 }, 1, false, SendPriority.Normal));
            Assert.True(queue.TryEnqueue(new byte[] { 3 }, 1, false, SendPriority.Critical));

            release.Release(4);
            await WaitUntilAsync(() => { lock (sent) return sent.Count == 4; });

            lock (sent) {
                Assert.Equal(new byte[] { 0, 3, 2, 1 }, sent.ToArray());
            }
        }

        [Fact]
        public void Rejects_When_Full() {
            using var queue = new SendQueue((_, _, _) => Task.Delay(-1), NullNetLogger.Instance, maxQueueSize: 2);
            // Not started: nothing drains.
            Assert.True(queue.TryEnqueue(new byte[1], 1, false, SendPriority.Normal));
            Assert.True(queue.TryEnqueue(new byte[1], 1, false, SendPriority.Normal));
            Assert.False(queue.TryEnqueue(new byte[1], 1, false, SendPriority.Normal));
        }

        [Fact]
        public async Task Send_Failure_Does_Not_Stop_Draining() {
            int calls = 0;
            using var queue = new SendQueue((data, length, _) => {
                Interlocked.Increment(ref calls);
                if (data[0] == 1) {
                    throw new InvalidOperationException("boom");
                }
                return Task.CompletedTask;
            }, NullNetLogger.Instance);

            queue.Start();
            queue.TryEnqueue(new byte[] { 1 }, 1, false, SendPriority.Normal); // throws
            queue.TryEnqueue(new byte[] { 2 }, 1, false, SendPriority.Normal); // must still be sent

            await WaitUntilAsync(() => Volatile.Read(ref calls) == 2);
            Assert.Equal(2, calls);
        }

        [Fact]
        public void Pooled_Buffers_Are_Returned_On_Dispose() {
            // Enqueue a pooled buffer into a queue that never starts draining, then
            // dispose. The queue must hand the buffer back to ArrayPool — observable
            // deterministically because Return/Rent happen on this same thread and
            // ArrayPool.Shared serves the per-thread cache first.
            var queue = new SendQueue((_, _, _) => Task.CompletedTask, NullNetLogger.Instance);
            var buffer = System.Buffers.ArrayPool<byte>.Shared.Rent(64);
            Assert.True(queue.TryEnqueue(buffer, 64, pooled: true, SendPriority.Normal));

            queue.Dispose();

            var again = System.Buffers.ArrayPool<byte>.Shared.Rent(64);
            Assert.Same(buffer, again);
            System.Buffers.ArrayPool<byte>.Shared.Return(again);
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
