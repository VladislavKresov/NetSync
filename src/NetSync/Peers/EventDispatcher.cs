using System;
using System.Collections.Concurrent;
using NetSync.Diagnostics;

namespace NetSync.Peers {
    /// <summary>
    /// Routes events either straight through (Immediate) or into a queue drained by
    /// PollEvents (Polled). Handler exceptions are logged, never propagated into
    /// network threads.
    /// </summary>
    internal sealed class EventDispatcher {
        private readonly ConcurrentQueue<Action>? _queue;
        private readonly INetLogger _logger;

        /// <summary>True in Immediate mode: callers can invoke handlers directly and skip
        /// the closure allocation that Dispatch would need.</summary>
        public bool IsImmediate => _queue == null;

        public EventDispatcher(EventDelivery mode, INetLogger logger) {
            _logger = logger;
            if (mode == EventDelivery.Polled) {
                _queue = new ConcurrentQueue<Action>();
            }
        }

        public void Dispatch(Action raise) {
            if (_queue == null) {
                Run(raise);
            }
            else {
                _queue.Enqueue(raise);
            }
        }

        /// <summary>Drains queued events on the calling thread. No-op in Immediate mode.</summary>
        public int Poll(int maxEvents) {
            if (_queue == null) {
                return 0;
            }
            int handled = 0;
            while (handled < maxEvents && _queue.TryDequeue(out var raise)) {
                Run(raise);
                handled++;
            }
            return handled;
        }

        private void Run(Action raise) {
            try {
                raise();
            }
            catch (Exception ex) {
                _logger.Error($"Unhandled exception in event handler: {ex}");
            }
        }
    }
}
