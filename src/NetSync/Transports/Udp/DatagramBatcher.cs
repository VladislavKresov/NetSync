using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using NetSync.Diagnostics;
using NetSync.Internal;

namespace NetSync.Transports.Udp {
    /// <summary>
    /// Packs small UDP packets into shared datagrams — one socket call carries many messages.
    ///
    /// Callers only copy their packet into the destination's open batch; one drain loop per
    /// transport sends the batches. While that loop is inside a socket call, new packets keep
    /// accumulating, so batching grows with load on its own and never delays a lone packet
    /// (no timer, no Nagle-style wait). A batch holding a single packet goes out as a plain
    /// Data datagram — the receiver sees exactly the pre-batching wire format.
    ///
    /// Wire format of a batch: [Batch][len][packet][len][packet]… where len is a 1–3 byte
    /// little-endian base-128 varint and each packet is the payload of a Data datagram. A
    /// batch never exceeds <see cref="MaxBatchSize"/> (below the IPv6 minimum MTU, so it is
    /// never IP-fragmented); larger packets go out alone, in order with the batched ones.
    ///
    /// The receive thread corks the batcher while it handles one datagram
    /// (<see cref="Cork"/>/<see cref="Uncork"/>): everything it sends in response — acks,
    /// replies — leaves together once the datagram is done, instead of the drain loop
    /// waking halfway and splitting it. Other threads are never held back by the cork.
    ///
    /// Bounded: a destination holds at most <see cref="MaxQueuedBytes"/>; beyond that new
    /// packets are dropped, exactly like a full socket buffer drops them (the reliability
    /// layer retransmits what matters).
    /// </summary>
    internal sealed class DatagramBatcher : IDisposable {
        /// <summary>Largest batch: 1280 (IPv6 minimum MTU) − 40 (IPv6) − 8 (UDP) − margin.</summary>
        public const int MaxBatchSize = 1200;

        /// <summary>Per-destination backlog cap.</summary>
        public const int MaxQueuedBytes = 256 * 1024;

        private const int MaxVarIntSize = 3;

        /// <summary>One destination's queue. The owner keeps it alongside the peer.</summary>
        internal sealed class Target {
            internal readonly object? Destination;
            internal readonly Queue<Batch> Sealed = new Queue<Batch>();
            internal Batch Open;
            internal int QueuedBytes;
            internal bool Scheduled;
            internal bool Closed;

            internal Target(object? destination) {
                Destination = destination;
            }
        }

        internal struct Batch {
            public byte[]? Buffer;
            public int Length;
            public int Count;
            public int FirstHeader; // varint size of the first entry's length
        }

        /// <summary>Sends buffer[offset..offset+length) to the target's destination. Must not block for long.</summary>
        internal delegate void SendDatagram(object? destination, byte[] buffer, int offset, int length);

        private readonly SendDatagram _send;
        private readonly INetLogger _logger;
        private readonly object _lock = new object();
        private readonly Queue<Target> _scheduled = new Queue<Target>();
        private readonly List<Batch> _drained = new List<Batch>(); // drain loop only
        private readonly AsyncSignal _work = new AsyncSignal();
        private readonly CancellationTokenSource _lifetime = new CancellationTokenSource();
        private int _disposed;
        private int _corkThread;     // managed thread id of the corking receive thread, 0 = none
        private bool _wakeDeferred;  // the corking thread queued work and did not wake the loop

        public DatagramBatcher(SendDatagram send, INetLogger logger) {
            _send = send;
            _logger = logger;
            var token = _lifetime.Token;
            _ = Task.Run(() => DrainLoopAsync(token));
        }

        public Target CreateTarget(object? destination) => new Target(destination);

        /// <summary>
        /// Queues one Data packet (prefix‖payload). Returns false when the target is closed or
        /// over its backlog cap — the packet is dropped, as the network would drop it.
        /// </summary>
        public bool Enqueue(Target target, ReadOnlySpan<byte> prefix, ReadOnlySpan<byte> payload) {
            int length = prefix.Length + payload.Length;
            int header = VarIntSize(length);
            int entrySize = header + length;
            bool wake = false;
            lock (_lock) {
                if (target.Closed || Volatile.Read(ref _disposed) == 1 || target.QueuedBytes + entrySize > MaxQueuedBytes) {
                    return false;
                }
                ref var open = ref target.Open;
                if (open.Buffer != null && open.Length + entrySize > MaxBatchSize) {
                    target.Sealed.Enqueue(open);
                    open = default;
                }
                if (open.Buffer == null) {
                    open.Buffer = Pooled.Rent(Math.Max(MaxBatchSize, 1 + entrySize));
                    open.Buffer[0] = (byte)PacketType.Batch;
                    open.Length = 1;
                    open.FirstHeader = header;
                }
                var buffer = open.Buffer;
                int offset = WriteVarInt(buffer, open.Length, length);
                prefix.CopyTo(buffer.AsSpan(offset));
                payload.CopyTo(buffer.AsSpan(offset + prefix.Length));
                open.Length = offset + length;
                open.Count++;
                target.QueuedBytes += entrySize;

                if (!target.Scheduled) {
                    target.Scheduled = true;
                    _scheduled.Enqueue(target);
                }
                // The loop needs a wake-up only when it may be idle: the queue just became
                // non-empty, or the corking thread left it asleep with work queued.
                if (_scheduled.Count == 1 || _wakeDeferred) {
                    if (_corkThread != 0 && _corkThread == Environment.CurrentManagedThreadId) {
                        _wakeDeferred = true;
                    }
                    else {
                        _wakeDeferred = false;
                        wake = true;
                    }
                }
            }
            if (wake) {
                _work.Set();
            }
            return true;
        }

        /// <summary>
        /// Holds back the wake-up for packets the calling thread queues until <see cref="Uncork"/>.
        /// One corking thread at a time (the transport's receive thread).
        /// </summary>
        public void Cork() {
            lock (_lock) {
                _corkThread = Environment.CurrentManagedThreadId;
            }
        }

        public void Uncork() {
            bool wake;
            lock (_lock) {
                _corkThread = 0;
                wake = _wakeDeferred;
                _wakeDeferred = false;
            }
            if (wake) {
                _work.Set();
            }
        }

        /// <summary>
        /// Refuses further packets for the target and sends what is already queued for it right
        /// away, on the calling thread — a Bye queued just before a kick or disconnect still leaves.
        /// </summary>
        public void Close(Target target) {
            List<Batch> pending;
            lock (_lock) {
                if (target.Closed) {
                    return;
                }
                target.Closed = true;
                pending = TakeLocked(target);
            }
            SendAll(target, pending);
        }

        private static List<Batch> TakeLocked(Target target) {
            var batches = new List<Batch>(target.Sealed.Count + 1);
            while (target.Sealed.Count > 0) {
                batches.Add(target.Sealed.Dequeue());
            }
            if (target.Open.Buffer != null) {
                batches.Add(target.Open);
                target.Open = default;
            }
            target.QueuedBytes = 0;
            return batches;
        }

        /// <summary>Best-effort send outside the drain loop (closing): errors only mean the datagram is lost.</summary>
        private void SendAll(Target target, List<Batch> batches) {
            foreach (var batch in batches) {
                try {
                    SendBatch(target, batch);
                }
                catch (Exception ex) {
                    _logger.Debug($"UDP send while closing failed: {ex.Message}");
                }
                finally {
                    Pooled.Return(batch.Buffer!);
                }
            }
        }

        private async Task DrainLoopAsync(CancellationToken ct) {
            try {
                while (!ct.IsCancellationRequested) {
                    await _work.WaitAsync().ConfigureAwait(false);
                    while (!ct.IsCancellationRequested && TryTake(out var target)) {
                        foreach (var batch in _drained) {
                            try {
                                SendBatch(target, batch);
                            }
                            catch (Exception ex) {
                                _logger.Debug($"UDP send failed: {ex.Message}");
                            }
                            finally {
                                Pooled.Return(batch.Buffer!);
                            }
                        }
                        _drained.Clear();
                    }
                }
            }
            catch (Exception ex) {
                _logger.Error($"DatagramBatcher drain loop: {ex}");
            }
        }

        /// <summary>Takes every batch of the next scheduled target into <see cref="_drained"/>.</summary>
        private bool TryTake(out Target target) {
            lock (_lock) {
                while (_scheduled.Count > 0) {
                    target = _scheduled.Dequeue();
                    target.Scheduled = false;
                    if (target.Closed) {
                        continue;
                    }
                    while (target.Sealed.Count > 0) {
                        _drained.Add(target.Sealed.Dequeue());
                    }
                    if (target.Open.Buffer != null) {
                        _drained.Add(target.Open);
                        target.Open = default;
                    }
                    target.QueuedBytes = 0;
                    if (_drained.Count > 0) {
                        return true;
                    }
                }
            }
            target = null!;
            return false;
        }

        private void SendBatch(Target target, Batch batch) {
            var buffer = batch.Buffer!;
            if (batch.Count == 1) {
                // [Batch][len][packet] → [Data][packet]: overwrite the last header byte.
                int start = batch.FirstHeader;
                buffer[start] = (byte)PacketType.Data;
                _send(target.Destination, buffer, start, batch.Length - start);
            }
            else {
                _send(target.Destination, buffer, 0, batch.Length);
            }
        }

        /// <summary>
        /// Calls <paramref name="deliver"/> for each packet of a batch datagram (buffer[0] is the
        /// Batch type). Stops at the first malformed entry, or after <paramref name="maxPackets"/>.
        /// </summary>
        public static void Unpack<TState>(byte[] buffer, int received, int maxPackets, TState state,
                                          Action<TState, byte[], int, int> deliver) {
            int offset = 1;
            int delivered = 0;
            while (offset < received && delivered < maxPackets) {
                if (!TryReadVarInt(buffer, ref offset, received, out int length) || length > received - offset) {
                    return;
                }
                deliver(state, buffer, offset, length);
                offset += length;
                delivered++;
            }
        }

        internal static int VarIntSize(int value) => value < 0x80 ? 1 : value < 0x4000 ? 2 : 3;

        private static int WriteVarInt(byte[] buffer, int offset, int value) {
            while (value >= 0x80) {
                buffer[offset++] = (byte)(value | 0x80);
                value >>= 7;
            }
            buffer[offset++] = (byte)value;
            return offset;
        }

        internal static bool TryReadVarInt(byte[] buffer, ref int offset, int end, out int value) {
            value = 0;
            for (int i = 0; i < MaxVarIntSize; i++) {
                if (offset >= end) {
                    return false;
                }
                byte b = buffer[offset++];
                value |= (b & 0x7F) << (7 * i);
                if ((b & 0x80) == 0) {
                    return true;
                }
            }
            return false;
        }

        /// <summary>Stops the drain loop; what is still queued is sent on the calling thread first.</summary>
        public void Dispose() {
            var pending = new List<(Target, List<Batch>)>();
            lock (_lock) {
                if (Interlocked.Exchange(ref _disposed, 1) == 1) {
                    return;
                }
                while (_scheduled.Count > 0) {
                    var target = _scheduled.Dequeue();
                    target.Scheduled = false;
                    pending.Add((target, TakeLocked(target)));
                }
            }
            _lifetime.Cancel();
            _work.Set();
            foreach (var (target, batches) in pending) {
                SendAll(target, batches);
            }
        }
    }
}
