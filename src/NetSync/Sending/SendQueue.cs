using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using NetSync.Diagnostics;
using NetSync.Transports;

namespace NetSync.Sending {
    /// <summary>
    /// Priority send queue with a dedicated drain loop.
    ///
    /// Blocks on a counting semaphore and wakes exactly when work arrives (the 1.x
    /// version polled with Task.Delay(1): ~1 ms extra latency plus constant CPU wakeups).
    ///
    /// Jobs are structs carrying (buffer, length); buffers rented from ArrayPool are
    /// returned after the write, so a steady send workload allocates nothing here.
    /// </summary>
    internal sealed class SendQueue : IDisposable {
        private readonly struct Job {
            public readonly byte[] Buffer;
            public readonly int Length;
            public readonly bool Pooled;

            public Job(byte[] buffer, int length, bool pooled) {
                Buffer = buffer;
                Length = length;
                Pooled = pooled;
            }
        }

        private readonly Queue<Job>[] _queues;
        private readonly object _lock = new object();
        private readonly SemaphoreSlim _signal = new SemaphoreSlim(0);
        private readonly Func<byte[], int, CancellationToken, Task> _sendFunc;
        private readonly INetLogger _logger;
        private readonly int _maxQueueSize;
        private CancellationTokenSource? _cts;
        private Task? _loop;
        private int _count;
        private bool _disposed;

        public int Count => Volatile.Read(ref _count);

        public SendQueue(Func<byte[], int, CancellationToken, Task> sendFunc, INetLogger logger, int maxQueueSize = 10_000) {
            _sendFunc = sendFunc;
            _logger = logger;
            _maxQueueSize = maxQueueSize;
            _queues = new Queue<Job>[4];
            for (int i = 0; i < _queues.Length; i++) {
                _queues[i] = new Queue<Job>();
            }
        }

        public void Start() {
            if (_loop != null) {
                return;
            }
            _cts = new CancellationTokenSource();
            var token = _cts.Token;
            _loop = Task.Run(() => DrainLoopAsync(token));
        }

        /// <summary>
        /// Returns false when the queue is full (caller decides how to react and owns
        /// the buffer in that case). On success the queue takes ownership: pooled
        /// buffers are returned to the pool after the write.
        /// </summary>
        public bool TryEnqueue(byte[] buffer, int length, bool pooled, SendPriority priority) {
            lock (_lock) {
                if (_count >= _maxQueueSize) {
                    return false;
                }
                _queues[(int)priority].Enqueue(new Job(buffer, length, pooled));
                _count++;
            }
            _signal.Release();
            return true;
        }

        private async Task DrainLoopAsync(CancellationToken ct) {
            try {
                while (!ct.IsCancellationRequested) {
                    await _signal.WaitAsync(ct).ConfigureAwait(false);

                    Job? dequeued = null;
                    lock (_lock) {
                        for (int i = 0; i < _queues.Length; i++) {
                            if (_queues[i].Count > 0) {
                                dequeued = _queues[i].Dequeue();
                                _count--;
                                break;
                            }
                        }
                    }
                    if (dequeued is not { } job) {
                        continue;
                    }

                    try {
                        await _sendFunc(job.Buffer, job.Length, ct).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException) {
                        return;
                    }
                    catch (Exception ex) {
                        // A failed write means the connection is going down; the receive
                        // loop notices and triggers disconnect. Log and keep draining so
                        // Stop() is never blocked.
                        _logger.Debug($"SendQueue write failed: {ex.Message}");
                    }
                    finally {
                        if (job.Pooled) {
                            PacketProtocol.Return(job.Buffer);
                        }
                    }
                }
            }
            catch (OperationCanceledException) { }
            finally {
                ReleaseQueuedBuffers();
            }
        }

        private void ReleaseQueuedBuffers() {
            lock (_lock) {
                foreach (var queue in _queues) {
                    while (queue.Count > 0) {
                        var job = queue.Dequeue();
                        if (job.Pooled) {
                            PacketProtocol.Return(job.Buffer);
                        }
                    }
                }
                _count = 0;
            }
        }

        public async Task StopAsync() {
            if (_loop == null) {
                return;
            }
            _cts!.Cancel();
            try {
                await _loop.ConfigureAwait(false);
            }
            catch (OperationCanceledException) { }
            _loop = null;
            _cts.Dispose();
            _cts = null;
        }

        public void Dispose() {
            if (_disposed) {
                return;
            }
            _disposed = true;
            try {
                StopAsync().Wait(TimeSpan.FromSeconds(5));
            }
            catch { }
            ReleaseQueuedBuffers();
            _signal.Dispose();
        }
    }
}
