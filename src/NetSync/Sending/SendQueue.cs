using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using NetSync.Diagnostics;
using NetSync.Internal;

namespace NetSync.Sending {
    /// <summary>
    /// Priority send queue with a dedicated drain loop.
    ///
    /// - Wakes exactly when work arrives, without allocating (<see cref="AsyncSignal"/>).
    ///   After an idle period the first write happens on the enqueuing thread itself.
    /// - Optional coalescing: consecutive small frames are copied into one pooled batch
    ///   and written with a single socket call — far fewer syscalls under load, while a
    ///   lone message still goes out immediately (no Nagle-style delay).
    /// - Bounded: <see cref="TryEnqueue"/> fails fast when full (real-time traffic),
    ///   <see cref="EnqueueAsync"/> waits for space (bulk transfers — backpressure
    ///   instead of failing halfway through a file). Bulk is additionally capped in
    ///   bytes, so streaming a huge file holds only a few MB in the queue.
    /// - Pooled buffers are returned after the write, or when the queue shuts down.
    /// - <see cref="WriteStartedAtMs"/> lets the owner detect a write that never
    ///   completes (remote stopped reading) on every runtime, including ones where
    ///   socket writes ignore cancellation.
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
        private readonly AsyncSignal _work = new AsyncSignal();
        private readonly SemaphoreSlim _slots;
        private readonly CancellationTokenSource _lifetime = new CancellationTokenSource();
        private readonly Func<byte[], int, CancellationToken, ValueTask> _write;
        private readonly INetLogger _logger;
        private readonly long _maxQueuedBytes;
        private readonly int _coalesceBytes;
        private TaskCompletionSource<bool>? _bytesFreed;
        private long _queuedBytes;
        private Task? _loop;
        private int _count;
        private long _writeStartedAtMs;
        private int _disposed;

        public int Count => Volatile.Read(ref _count);

        /// <summary>Monotonic start time of the write in progress, 0 when idle.</summary>
        public long WriteStartedAtMs => Interlocked.Read(ref _writeStartedAtMs);

        /// <summary>Bytes currently waiting in the queue.</summary>
        public long QueuedBytes {
            get {
                lock (_lock) {
                    return _queuedBytes;
                }
            }
        }

        /// <summary>One write per job (no coalescing) — the simple form, used by tests.</summary>
        public SendQueue(Func<byte[], int, CancellationToken, Task> sendFunc, INetLogger logger, int maxQueueSize = 10_000,
                         long maxQueuedBytes = 4 * 1024 * 1024)
            : this((buffer, length, ct) => new ValueTask(sendFunc(buffer, length, ct)), 0, logger, maxQueueSize, maxQueuedBytes) { }

        /// <param name="write">Writes buffer[0..length) to the wire.</param>
        /// <param name="coalesceBytes">Batch size for coalescing small jobs; 0 = one write per job.</param>
        /// <param name="logger">Diagnostics.</param>
        /// <param name="maxQueueSize">Job count cap.</param>
        /// <param name="maxQueuedBytes">Byte cap for <see cref="EnqueueAsync"/> (bulk).</param>
        public SendQueue(Func<byte[], int, CancellationToken, ValueTask> write, int coalesceBytes, INetLogger logger,
                         int maxQueueSize = 10_000, long maxQueuedBytes = 4 * 1024 * 1024) {
            _write = write;
            _coalesceBytes = coalesceBytes;
            _logger = logger;
            _maxQueuedBytes = maxQueuedBytes;
            _slots = new SemaphoreSlim(maxQueueSize, maxQueueSize);
            _queues = new Queue<Job>[4];
            for (int i = 0; i < _queues.Length; i++) {
                _queues[i] = new Queue<Job>();
            }
        }

        public void Start() {
            if (_loop != null) {
                return;
            }
            var token = _lifetime.Token;
            _loop = Task.Run(() => DrainLoopAsync(token));
        }

        /// <summary>
        /// Non-blocking enqueue. Returns false when the queue is full or shut down — the
        /// caller still owns the buffer in that case. On success the queue owns it.
        /// </summary>
        public bool TryEnqueue(byte[] buffer, int length, bool pooled, SendPriority priority) {
            if (Volatile.Read(ref _disposed) == 1 || !_slots.Wait(0)) {
                return false;
            }
            lock (_lock) {
                if (Volatile.Read(ref _disposed) == 1) {
                    return false; // shut down meanwhile: caller still owns the buffer
                }
                PushLocked(buffer, length, pooled, priority);
            }
            _work.Set();
            return true;
        }

        /// <summary>
        /// Waits for queue space. On success the queue owns the buffer; if this throws
        /// (cancelled or queue shut down) the caller still owns it.
        /// </summary>
        public async ValueTask EnqueueAsync(byte[] buffer, int length, bool pooled, SendPriority priority, CancellationToken ct) {
            if (Volatile.Read(ref _disposed) == 1) {
                throw new ObjectDisposedException(nameof(SendQueue));
            }
            if (!_slots.Wait(0)) {
                using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, _lifetime.Token);
                try {
                    await _slots.WaitAsync(linked.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (!ct.IsCancellationRequested) {
                    throw new ObjectDisposedException(nameof(SendQueue));
                }
            }
            // Byte budget: wait until the queue drains below the cap (an oversized single
            // job is still admitted into an empty queue, so nothing can deadlock).
            while (true) {
                Task freed;
                lock (_lock) {
                    if (Volatile.Read(ref _disposed) == 1) {
                        _slots.Release();
                        throw new ObjectDisposedException(nameof(SendQueue)); // not queued: caller still owns the buffer
                    }
                    if (_queuedBytes == 0 || _queuedBytes + length <= _maxQueuedBytes) {
                        PushLocked(buffer, length, pooled, priority);
                        break;
                    }
                    freed = (_bytesFreed ??= new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously)).Task;
                }
                using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, _lifetime.Token);
                var cancelled = Task.Delay(Timeout.Infinite, linked.Token);
                if (await Task.WhenAny(freed, cancelled).ConfigureAwait(false) == cancelled) {
                    _slots.Release();
                    ct.ThrowIfCancellationRequested();
                    throw new ObjectDisposedException(nameof(SendQueue));
                }
            }
            // From here on the queue owns the buffer — even if it shuts down right now,
            // it returns pooled buffers itself. Never throw past this point: the caller
            // would return the same buffer to the pool a second time.
            _work.Set();
        }

        private void PushLocked(byte[] buffer, int length, bool pooled, SendPriority priority) {
            _queues[(int)priority].Enqueue(new Job(buffer, length, pooled));
            _count++;
            _queuedBytes += length;
        }

        private async Task DrainLoopAsync(CancellationToken ct) {
            try {
                while (!ct.IsCancellationRequested) {
                    await _work.WaitAsync().ConfigureAwait(false);

                    while (!ct.IsCancellationRequested && TryTakeWrite(out var job)) {
                        try {
                            await _write(job.Buffer, job.Length, ct).ConfigureAwait(false);
                        }
                        catch (OperationCanceledException) {
                            return;
                        }
                        catch (Exception ex) {
                            // A failed write means the connection is going down; the receive
                            // loop notices and triggers disconnect. Keep draining so shutdown
                            // is never blocked.
                            _logger.Debug($"SendQueue write failed: {ex.Message}");
                        }
                        finally {
                            Interlocked.Exchange(ref _writeStartedAtMs, 0);
                            if (job.Pooled) {
                                Pooled.Return(job.Buffer);
                            }
                        }
                    }
                }
            }
            catch (OperationCanceledException) { }
            finally {
                ReleaseQueuedBuffers();
            }
        }

        /// <summary>
        /// Takes the next write: the highest-priority job, plus — when coalescing — the
        /// jobs that follow it in priority order while they fit into one batch.
        /// </summary>
        private bool TryTakeWrite(out Job write) {
            TaskCompletionSource<bool>? freed;
            int taken = 0;
            lock (_lock) {
                if (!TryDequeueLocked(out var first)) {
                    write = default;
                    return false;
                }
                taken = 1;
                write = first;
                // Under the lock: a job leaving the queue is never invisible to WaitUntilIdleAsync.
                Interlocked.Exchange(ref _writeStartedAtMs, NetTime.NowMs);
                if (_coalesceBytes > 0 && first.Length < _coalesceBytes && PeekLocked(out var next) &&
                    first.Length + next.Length <= _coalesceBytes) {
                    var batch = Pooled.Rent(_coalesceBytes);
                    int length = Append(batch, 0, first);
                    while (PeekLocked(out next) && length + next.Length <= _coalesceBytes) {
                        TryDequeueLocked(out next);
                        length = Append(batch, length, next);
                        taken++;
                    }
                    write = new Job(batch, length, pooled: true);
                }
                freed = _bytesFreed;
                _bytesFreed = null;
            }
            _slots.Release(taken);
            freed?.TrySetResult(true);
            return true;
        }

        private static int Append(byte[] batch, int offset, Job job) {
            Buffer.BlockCopy(job.Buffer, 0, batch, offset, job.Length);
            if (job.Pooled) {
                Pooled.Return(job.Buffer);
            }
            return offset + job.Length;
        }

        private bool PeekLocked(out Job job) {
            foreach (var queue in _queues) {
                if (queue.Count > 0) {
                    job = queue.Peek();
                    return true;
                }
            }
            job = default;
            return false;
        }

        private bool TryDequeueLocked(out Job job) {
            foreach (var queue in _queues) {
                if (queue.Count > 0) {
                    job = queue.Dequeue();
                    _count--;
                    _queuedBytes -= job.Length;
                    return true;
                }
            }
            job = default;
            return false;
        }

        private void ReleaseQueuedBuffers() {
            lock (_lock) {
                foreach (var queue in _queues) {
                    while (queue.Count > 0) {
                        var job = queue.Dequeue();
                        if (job.Pooled) {
                            Pooled.Return(job.Buffer);
                        }
                    }
                }
                _count = 0;
                _queuedBytes = 0;
            }
        }

        /// <summary>
        /// Waits until everything queued so far has been written. False on timeout or shutdown.
        /// For closing a connection without discarding its last messages; not for the data path.
        /// </summary>
        public async Task<bool> WaitUntilIdleAsync(int timeoutMs) {
            long deadline = NetTime.NowMs + timeoutMs;
            while (Volatile.Read(ref _disposed) == 0) {
                lock (_lock) {
                    if (_count == 0 && Interlocked.Read(ref _writeStartedAtMs) == 0) {
                        return true;
                    }
                }
                if (NetTime.NowMs >= deadline) {
                    return false;
                }
                await Task.Delay(1).ConfigureAwait(false);
            }
            return false;
        }

        public void Dispose() {
            lock (_lock) {
                // Under the lock: after this, no push can slip in behind the final release.
                if (Interlocked.Exchange(ref _disposed, 1) == 1) {
                    return;
                }
            }
            _lifetime.Cancel(); // wakes EnqueueAsync waiters
            _work.Set();        // wakes the drain loop so it observes the cancellation
            var loop = _loop;
            if (loop != null) {
                try {
                    // The drain loop only blocks inside a socket write; the owner closes
                    // the socket right after Dispose, which unblocks it. Don't wait long.
                    loop.Wait(TimeSpan.FromSeconds(1));
                }
                catch { }
            }
            ReleaseQueuedBuffers();
        }
    }
}
