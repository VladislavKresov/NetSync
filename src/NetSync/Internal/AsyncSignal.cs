using System;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;

namespace NetSync.Internal {
    /// <summary>
    /// Allocation-free auto-reset signal for exactly one async waiter. A Set() that
    /// arrives while nobody waits is remembered (one pending wake-up); a Set() during a
    /// wait completes it.
    ///
    /// Replaces SemaphoreSlim.WaitAsync in drain loops: the semaphore allocates a task
    /// node for every wait that suspends — once per message in request/response traffic.
    ///
    /// The waiter resumes on the thread pool, never inline in Set(): a producer only
    /// pays for the enqueue, and while the consumer is busy, producers keep adding work it
    /// can batch. When the waiter is an async method (the drain loops), .NET queues its
    /// state machine to the pool directly — no allocation for the hop either.
    /// </summary>
    internal sealed class AsyncSignal : IValueTaskSource {
        private ManualResetValueTaskSourceCore<bool> _core;
        private readonly object _lock = new object();
        private bool _signaled;
        private bool _waiting;

        public AsyncSignal() {
            _core.RunContinuationsAsynchronously = true;
        }

        public void Set() {
            lock (_lock) {
                if (!_waiting) {
                    _signaled = true;
                    return;
                }
                _waiting = false;
            }
            _core.SetResult(true);
        }

        /// <summary>Only one caller may wait at a time, and it must await before waiting again.</summary>
        public ValueTask WaitAsync() {
            lock (_lock) {
                if (_signaled) {
                    _signaled = false;
                    return default;
                }
                _core.Reset();
                _waiting = true;
                return new ValueTask(this, _core.Version);
            }
        }

        void IValueTaskSource.GetResult(short token) => _core.GetResult(token);

        ValueTaskSourceStatus IValueTaskSource.GetStatus(short token) => _core.GetStatus(token);

        void IValueTaskSource.OnCompleted(Action<object?> continuation, object? state, short token,
                                          ValueTaskSourceOnCompletedFlags flags) {
            _core.OnCompleted(continuation, state, token, flags);
        }
    }
}
