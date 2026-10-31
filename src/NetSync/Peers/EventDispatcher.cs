using System;
using System.Collections.Generic;
using System.Threading;
using NetSync.Diagnostics;
using NetSync.Internal;

namespace NetSync.Peers {
    /// <summary>Raises a queued data event; implemented by NetClient / NetServer.</summary>
    internal interface IDataEventSink {
        /// <param name="source">The NetConnection on servers, null on clients.</param>
        /// <param name="channel">Channel id.</param>
        /// <param name="buffer">Pooled copy; valid only during the call.</param>
        /// <param name="length">Valid bytes in <paramref name="buffer"/>.</param>
        void RaiseQueuedData(object? source, byte channel, byte[] buffer, int length);
    }

    /// <summary>
    /// Routes events either straight through (Immediate) or into a queue drained by
    /// PollEvents (Polled). Handler exceptions are logged, never propagated into
    /// network threads.
    ///
    /// Polled data events cost no allocation: the payload is copied into a pooled
    /// buffer and the queue holds plain structs in a reused ring (lifecycle events,
    /// which are rare, keep their closures).
    ///
    /// The Polled queue is bounded: if the application stops polling (paused game,
    /// forgotten call) data events beyond the cap are dropped with a warning instead of
    /// growing memory forever. Lifecycle events (connect/disconnect) are never dropped.
    /// </summary>
    internal sealed class EventDispatcher {
        private struct QueuedEvent {
            public Action? Lifecycle;   // set for lifecycle events
            public Action? Legacy;      // data event queued as a closure (tests, custom callers)
            public object? Source;
            public byte[]? Buffer;      // pooled payload copy
            public int Length;
            public byte Channel;
        }

        private readonly Queue<QueuedEvent>? _queue;
        private readonly object _queueLock = new object();
        private readonly object _pollLock = new object();
        private readonly INetLogger _logger;
        private readonly IDataEventSink? _sink;
        private readonly int _maxPendingEvents;
        private QueuedEvent[] _batch = new QueuedEvent[64];
        private int _pending;
        private long _dropped;
        private int _overflowLogged;

        /// <summary>True in Immediate mode: callers raise data events directly.</summary>
        public bool IsImmediate => _queue == null;

        /// <summary>Data events dropped because the Polled queue was full.</summary>
        public long DroppedEvents => Interlocked.Read(ref _dropped);

        public EventDispatcher(EventDelivery mode, INetLogger logger, int maxPendingEvents = 100_000, IDataEventSink? sink = null) {
            _logger = logger;
            _maxPendingEvents = maxPendingEvents;
            _sink = sink;
            if (mode == EventDelivery.Polled) {
                _queue = new Queue<QueuedEvent>();
            }
        }

        /// <summary>Lifecycle event: always delivered.</summary>
        public void Dispatch(Action raise) {
            if (_queue == null) {
                Run(raise);
                return;
            }
            Enqueue(new QueuedEvent { Lifecycle = raise });
        }

        /// <summary>Data event as a closure: may be dropped when the Polled queue is full.</summary>
        public void DispatchData(Action raise) {
            if (_queue == null) {
                Run(raise);
                return;
            }
            if (TryReserveDataSlot()) {
                Enqueue(new QueuedEvent { Legacy = raise });
            }
        }

        /// <summary>
        /// Polled mode: queues a data event, copying the payload into a pooled buffer
        /// (the caller's buffer is only valid during this call). May drop when full.
        /// </summary>
        public void QueueData(object? source, byte channel, byte[] buffer, int offset, int count) {
            if (_queue == null) {
                throw new InvalidOperationException("QueueData is for Polled mode");
            }
            if (!TryReserveDataSlot()) {
                return;
            }
            var copy = Pooled.Rent(count);
            Buffer.BlockCopy(buffer, offset, copy, 0, count);
            Enqueue(new QueuedEvent { Source = source, Channel = channel, Buffer = copy, Length = count });
        }

        private bool TryReserveDataSlot() {
            if (Volatile.Read(ref _pending) < _maxPendingEvents) {
                return true;
            }
            Interlocked.Increment(ref _dropped);
            if (Interlocked.Exchange(ref _overflowLogged, 1) == 0) {
                _logger.Warning($"Event queue full ({_maxPendingEvents} pending): dropping data events. " +
                                "Is PollEvents() being called?");
            }
            return false;
        }

        private void Enqueue(QueuedEvent item) {
            Interlocked.Increment(ref _pending);
            lock (_queueLock) {
                _queue!.Enqueue(item);
            }
        }

        /// <summary>Drains queued events on the calling thread. No-op in Immediate mode.</summary>
        public int Poll(int maxEvents) {
            if (_queue == null) {
                return 0;
            }
            if (Monitor.IsEntered(_pollLock)) {
                return 0; // PollEvents called from inside a handler: the outer poll continues
            }
            int handled = 0;
            lock (_pollLock) { // one poller at a time keeps events in order
                while (handled < maxEvents) {
                    int taken = 0;
                    lock (_queueLock) {
                        int want = Math.Min(_batch.Length, maxEvents - handled);
                        while (taken < want && _queue.Count > 0) {
                            _batch[taken++] = _queue.Dequeue();
                        }
                    }
                    if (taken == 0) {
                        break;
                    }
                    Interlocked.Add(ref _pending, -taken);
                    // Handlers run outside the queue lock: network threads keep enqueuing.
                    for (int i = 0; i < taken; i++) {
                        var item = _batch[i];
                        _batch[i] = default;
                        Raise(item);
                    }
                    handled += taken;
                }
            }
            if (handled > 0) {
                Volatile.Write(ref _overflowLogged, 0); // warn again on the next overflow episode
            }
            return handled;
        }

        private void Raise(in QueuedEvent item) {
            if (item.Lifecycle != null) {
                Run(item.Lifecycle);
                return;
            }
            if (item.Legacy != null) {
                Run(item.Legacy);
                return;
            }
            var buffer = item.Buffer!;
            try {
                _sink?.RaiseQueuedData(item.Source, item.Channel, buffer, item.Length);
            }
            catch (Exception ex) {
                _logger.Error($"Unhandled exception in event handler: {ex}");
            }
            finally {
                Pooled.Return(buffer);
            }
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
