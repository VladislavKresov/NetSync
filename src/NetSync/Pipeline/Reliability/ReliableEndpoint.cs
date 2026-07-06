using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using NetSync.Diagnostics;
using NetSync.Internal;
using NetSync.Pipeline.Fragmentation;
using NetSync.Peers;
using NetSync.Transports;

namespace NetSync.Pipeline.Reliability {
    /// <summary>
    /// Reliability engine for one unreliable datagram link (ENet-style ARQ).
    /// One instance per UDP link; transport-agnostic — it only needs a send delegate.
    ///
    /// Per channel, depending on <see cref="ReliabilityMode"/>:
    /// - sequence numbers (16-bit, wrap-aware) with a 256-packet send window,
    /// - immediate ACKs carrying a 32-packet history bitmask,
    /// - RTO-based retransmission with exponential backoff, RTO derived from link RTT,
    /// - duplicate elimination and (for ReliableOrdered) reorder buffering,
    /// - automatic fragmentation of payloads above <see cref="FragmentThreshold"/>.
    ///
    /// Threading: HandleRelData/HandleAck are called from the transport receive thread
    /// (one per link); Send* may be called from any thread; the retransmit loop runs on
    /// its own task. Sender state is guarded by a per-channel lock, receiver state is
    /// only touched by the receive thread.
    /// </summary>
    internal sealed class ReliableEndpoint : IDisposable {
        public const int WindowSize = 256;
        /// <summary>Payloads above this size are split into reliable fragments (fits UDP MTU with headers).</summary>
        public const int FragmentThreshold = 1100;
        public const int RelDataHeaderSize = 5;   // [type][flags][channel][seq:2]
        public const int RelAckSize = 8;          // [type][channel][anchorSeq:2][mask:4]
        private const byte FlagFragment = 0x01;

        private sealed class PendingPacket {
            public byte[] Message = Array.Empty<byte>();
            public long LastSentAtMs;
            public int Attempts;
        }

        private sealed class SenderState {
            public ushort NextSeq;
            /// <summary>Oldest unacked seq; equals NextSeq when nothing is pending.
            /// The send window is a RANGE gate: NextSeq may never run more than
            /// WindowSize ahead of this, otherwise the receiver's dedup ring (also
            /// WindowSize) could not distinguish new packets from ancient ones.</summary>
            public ushort LowestUnacked;
            public readonly Dictionary<ushort, PendingPacket> Pending = new Dictionary<ushort, PendingPacket>();
            public TaskCompletionSource<bool>? WindowWaiter;
            public bool Closed;
            public readonly object Lock = new object();
        }

        private sealed class HeldPacket {
            public byte Flags;
            public byte[] Payload = Array.Empty<byte>();
        }

        private sealed class ReceiverState {
            public ReliabilityMode Mode;
            public ushort Highest = 0xFFFF;                       // unordered + sequenced ("-1")
            public readonly bool[] SeenRing;                      // unordered dedup ring
            public ushort Expected;                               // ordered: next seq to deliver
            public readonly Dictionary<ushort, HeldPacket>? Held; // ordered reorder buffer
            public readonly FragmentBuffer? Fragments;

            public ReceiverState(ReliabilityMode mode) {
                Mode = mode;
                SeenRing = mode == ReliabilityMode.Reliable ? new bool[WindowSize] : Array.Empty<bool>();
                if (mode == ReliabilityMode.ReliableOrdered) {
                    Held = new Dictionary<ushort, HeldPacket>();
                }
                if (mode == ReliabilityMode.Reliable || mode == ReliabilityMode.ReliableOrdered) {
                    // Under reliability fragments cannot be lost, only delayed by
                    // retransmission — the default 5 s reassembly timeout would drop
                    // slow transfers mid-flight.
                    Fragments = new FragmentBuffer { FragmentTimeoutMs = 600_000 };
                }
            }
        }

        private readonly Dictionary<byte, SenderState> _senders = new Dictionary<byte, SenderState>();
        private readonly Dictionary<byte, ReceiverState> _receivers = new Dictionary<byte, ReceiverState>();
        private readonly Func<ReadOnlyMemory<byte>, ValueTask> _sendAsync;
        private readonly Action<byte, byte[]> _deliver;
        private readonly Func<int> _getPingMs;
        private readonly Action<DisconnectReason> _onDead;
        private readonly INetLogger _logger;
        private readonly int _maxRetransmits;
        private readonly int _retransmitScanMs;
        private readonly int _rtoFloorMs;
        private readonly int _rtoCeilingMs;
        private CancellationTokenSource? _cts;
        private int _disposed;
        private int _deadSignaled;

        public ReliableEndpoint(
            Dictionary<byte, ChannelConfig> channels,
            Func<ReadOnlyMemory<byte>, ValueTask> sendAsync,
            Action<byte, byte[]> deliver,
            Func<int> getPingMs,
            Action<DisconnectReason> onDead,
            INetLogger logger,
            int maxRetransmits = 12,
            int retransmitScanMs = 20,
            int rtoFloorMs = 50,
            int rtoCeilingMs = 3000) {
            _sendAsync = sendAsync;
            _deliver = deliver;
            _getPingMs = getPingMs;
            _onDead = onDead;
            _logger = logger;
            _maxRetransmits = maxRetransmits;
            _retransmitScanMs = retransmitScanMs;
            _rtoFloorMs = rtoFloorMs;
            _rtoCeilingMs = rtoCeilingMs;

            foreach (var kvp in channels) {
                if (kvp.Value.Transport != TransportType.Udp || kvp.Value.Reliability == ReliabilityMode.Unreliable) {
                    continue;
                }
                _senders[kvp.Key] = new SenderState();
                _receivers[kvp.Key] = new ReceiverState(kvp.Value.Reliability);
            }
        }

        public void Start() {
            if (_cts != null) {
                return;
            }
            _cts = new CancellationTokenSource();
            var token = _cts.Token;
            _ = Task.Run(() => RetransmitLoopAsync(token));
        }

        // ---------------------------------------------------------------- send

        public async ValueTask SendReliableAsync(byte channel, ReadOnlyMemory<byte> data, CancellationToken ct = default) {
            var sender = GetSender(channel);
            if (data.Length <= FragmentThreshold) {
                await SendReliablePacketAsync(sender, channel, flags: 0, data.Span, default, ct).ConfigureAwait(false);
                return;
            }

            // Fragmented: each chunk is its own reliable packet; the window semaphore
            // paces the stream so a 100 MB file cannot flood the link.
            uint fragmentId = PacketFragmenter.NextSequenceId();
            int totalFragments = (data.Length + FragmentThreshold - 1) / FragmentThreshold;
            for (int i = 0; i < totalFragments; i++) {
                int offset = i * FragmentThreshold;
                int chunkLength = Math.Min(FragmentThreshold, data.Length - offset);
                var fragmentInfo = new FragmentInfo(fragmentId, (uint)i, (uint)totalFragments);
                await SendReliablePacketAsync(sender, channel, FlagFragment, data.Span.Slice(offset, chunkLength), fragmentInfo, ct).ConfigureAwait(false);
            }
        }

        private readonly struct FragmentInfo {
            public readonly uint Id;
            public readonly uint Index;
            public readonly uint Total;
            public FragmentInfo(uint id, uint index, uint total) {
                Id = id;
                Index = index;
                Total = total;
            }
        }

        private ValueTask SendReliablePacketAsync(SenderState sender, byte channel, byte flags, ReadOnlySpan<byte> payload, FragmentInfo fragment, CancellationToken ct) {
            // The message is built synchronously (spans cannot cross an await); the seq
            // is stamped in later, under the window slot.
            var message = BuildRelData(channel, flags, seq: 0, payload, fragment);
            return EnqueueAndSendAsync(sender, message, ct);
        }

        private async ValueTask EnqueueAndSendAsync(SenderState sender, byte[] message, CancellationToken ct) {
            // Backpressure: block while the seq range in flight is a full window.
            while (true) {
                TaskCompletionSource<bool>? waiter = null;
                lock (sender.Lock) {
                    if (sender.Closed) {
                        throw new InvalidOperationException("Reliable endpoint is disposed");
                    }
                    if ((ushort)(sender.NextSeq - sender.LowestUnacked) < WindowSize) {
                        ushort seq = sender.NextSeq++;
                        BinaryPrimitives.WriteUInt16BigEndian(message.AsSpan(3, 2), seq);
                        sender.Pending[seq] = new PendingPacket { Message = message, LastSentAtMs = NetTime.NowMs };
                    }
                    else {
                        waiter = sender.WindowWaiter ??= new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
                    }
                }
                if (waiter == null) {
                    break;
                }
                if (ct.CanBeCanceled) {
                    var cancelled = Task.Delay(Timeout.Infinite, ct);
                    if (await Task.WhenAny(waiter.Task, cancelled).ConfigureAwait(false) == cancelled) {
                        ct.ThrowIfCancellationRequested();
                    }
                }
                else {
                    await waiter.Task.ConfigureAwait(false);
                }
            }

            try {
                await _sendAsync(message).ConfigureAwait(false);
            }
            catch (Exception ex) {
                _logger.Debug($"Reliable send failed (will retransmit): {ex.Message}");
            }
        }

        public async ValueTask SendSequencedAsync(byte channel, ReadOnlyMemory<byte> data) {
            var sender = GetSender(channel);
            if (data.Length > FragmentThreshold) {
                throw new ArgumentException(
                    $"UnreliableSequenced payload of {data.Length} bytes exceeds {FragmentThreshold} bytes. " +
                    "Sequenced packets are not fragmented (a lost fragment would kill the whole packet); " +
                    "use a Reliable/ReliableOrdered channel for large payloads.", nameof(data));
            }

            var message = BuildRelData(channel, flags: 0, seq: 0, data.Span, default);
            lock (sender.Lock) {
                BinaryPrimitives.WriteUInt16BigEndian(message.AsSpan(3, 2), sender.NextSeq++);
            }
            await _sendAsync(message).ConfigureAwait(false);
        }

        private static byte[] BuildRelData(byte channel, byte flags, ushort seq, ReadOnlySpan<byte> payload, FragmentInfo fragment) {
            bool fragmented = (flags & FlagFragment) != 0;
            int fragmentHeader = fragmented ? PacketFragmenter.HeaderSize : 0;
            var message = new byte[RelDataHeaderSize + fragmentHeader + payload.Length];
            message[0] = PeerProtocol.MsgRelData;
            message[1] = flags;
            message[2] = channel;
            BinaryPrimitives.WriteUInt16BigEndian(message.AsSpan(3, 2), seq);
            if (fragmented) {
                PacketFragmenter.WriteFragmentHeader(message.AsSpan(RelDataHeaderSize), fragment.Id, fragment.Index, fragment.Total);
            }
            payload.CopyTo(message.AsSpan(RelDataHeaderSize + fragmentHeader));
            return message;
        }

        // ------------------------------------------------------------- receive

        /// <summary>Handles an incoming MsgRelData. Buffer valid only during the call.</summary>
        public void HandleRelData(byte[] buffer, int offset, int count) {
            if (count < RelDataHeaderSize) {
                return;
            }
            byte flags = buffer[offset + 1];
            byte channel = buffer[offset + 2];
            ushort seq = BinaryPrimitives.ReadUInt16BigEndian(buffer.AsSpan(offset + 3, 2));
            if (!_receivers.TryGetValue(channel, out var receiver)) {
                return; // channel not configured for reliability — protocol mismatch, drop
            }

            int payloadOffset = offset + RelDataHeaderSize;
            int payloadCount = count - RelDataHeaderSize;

            switch (receiver.Mode) {
                case ReliabilityMode.UnreliableSequenced:
                    if (SeqDistance(seq, receiver.Highest) > 0) {
                        receiver.Highest = seq;
                        DeliverPayload(receiver, channel, flags, buffer, payloadOffset, payloadCount);
                    }
                    break; // sequenced mode never acks

                case ReliabilityMode.Reliable:
                    HandleUnordered(receiver, channel, flags, seq, buffer, payloadOffset, payloadCount);
                    break;

                case ReliabilityMode.ReliableOrdered:
                    HandleOrdered(receiver, channel, flags, seq, buffer, payloadOffset, payloadCount);
                    break;
            }
        }

        private void HandleUnordered(ReceiverState receiver, byte channel, byte flags, ushort seq, byte[] buffer, int payloadOffset, int payloadCount) {
            int distance = SeqDistance(seq, receiver.Highest);
            bool isNew;
            if (distance > 0) {
                // Advancing: clear ring slots the window slides over.
                int steps = Math.Min(distance, WindowSize);
                for (int i = 1; i <= steps; i++) {
                    receiver.SeenRing[(ushort)(receiver.Highest + i) % WindowSize] = false;
                }
                receiver.Highest = seq;
                receiver.SeenRing[seq % WindowSize] = true;
                isNew = true;
            }
            else if (distance <= -WindowSize) {
                isNew = false; // ancient retransmit — just re-ack so the sender stops
            }
            else if (receiver.SeenRing[seq % WindowSize]) {
                isNew = false; // duplicate
            }
            else {
                receiver.SeenRing[seq % WindowSize] = true;
                isNew = true;
            }

            if (isNew) {
                DeliverPayload(receiver, channel, flags, buffer, payloadOffset, payloadCount);
            }
            SendAck(channel, seq, receiver);
        }

        private void HandleOrdered(ReceiverState receiver, byte channel, byte flags, ushort seq, byte[] buffer, int payloadOffset, int payloadCount) {
            int distance = SeqDistance(seq, receiver.Expected);
            if (distance >= WindowSize) {
                // Out of window and NOT stored: acking it would make the sender drop a
                // packet the receiver never kept. Stay silent; it will be retransmitted
                // once the window slides forward.
                return;
            }
            if (distance < 0 || (distance > 0 && receiver.Held!.ContainsKey(seq))) {
                SendAck(channel, seq, receiver); // duplicate — re-ack so the sender stops
                return;
            }

            if (distance == 0) {
                DeliverPayload(receiver, channel, flags, buffer, payloadOffset, payloadCount);
                receiver.Expected++;
                // Drain any consecutive packets buffered while this one was missing.
                while (receiver.Held!.TryGetValue(receiver.Expected, out var held)) {
                    receiver.Held.Remove(receiver.Expected);
                    DeliverPayload(receiver, channel, held.Flags, held.Payload, 0, held.Payload.Length);
                    receiver.Expected++;
                }
            }
            else {
                var payload = new byte[payloadCount];
                Array.Copy(buffer, payloadOffset, payload, 0, payloadCount);
                receiver.Held![seq] = new HeldPacket { Flags = flags, Payload = payload };
            }
            SendAck(channel, seq, receiver);
        }

        private void DeliverPayload(ReceiverState receiver, byte channel, byte flags, byte[] buffer, int offset, int count) {
            if ((flags & FlagFragment) != 0) {
                if (receiver.Fragments == null ||
                    !PacketFragmenter.TryUnwrap(buffer.AsSpan(offset, count), out uint fragId, out uint index, out uint total, out var chunk)) {
                    return; // malformed fragment
                }
                var complete = receiver.Fragments.AddFragment(fragId, index, total, chunk);
                if (complete != null) {
                    _deliver(channel, complete);
                }
                return;
            }

            var payload = new byte[count];
            Array.Copy(buffer, offset, payload, 0, count);
            _deliver(channel, payload);
        }

        // ---------------------------------------------------------------- acks

        private void SendAck(byte channel, ushort anchorSeq, ReceiverState receiver) {
            var ack = new byte[RelAckSize];
            ack[0] = PeerProtocol.MsgRelAck;
            ack[1] = channel;
            BinaryPrimitives.WriteUInt16BigEndian(ack.AsSpan(2, 2), anchorSeq);
            BinaryPrimitives.WriteUInt32BigEndian(ack.AsSpan(4, 4), BuildAckMask(receiver, anchorSeq));
            FireAndForget(_sendAsync(ack));
        }

        /// <summary>Bit i set = seq (anchor - 1 - i) has been received.</summary>
        private static uint BuildAckMask(ReceiverState receiver, ushort anchorSeq) {
            uint mask = 0;
            for (int i = 0; i < 32; i++) {
                var seq = (ushort)(anchorSeq - 1 - i);
                bool received = receiver.Mode switch {
                    ReliabilityMode.Reliable =>
                        SeqDistance(seq, receiver.Highest) <= 0 &&
                        SeqDistance(seq, receiver.Highest) > -WindowSize &&
                        receiver.SeenRing[seq % WindowSize],
                    ReliabilityMode.ReliableOrdered =>
                        SeqDistance(seq, receiver.Expected) < 0 || receiver.Held!.ContainsKey(seq),
                    _ => false
                };
                if (received) {
                    mask |= 1u << i;
                }
            }
            return mask;
        }

        /// <summary>Handles an incoming MsgRelAck. Buffer valid only during the call.</summary>
        public void HandleAck(byte[] buffer, int offset, int count) {
            if (count < RelAckSize) {
                return;
            }
            byte channel = buffer[offset + 1];
            ushort anchorSeq = BinaryPrimitives.ReadUInt16BigEndian(buffer.AsSpan(offset + 2, 2));
            uint mask = BinaryPrimitives.ReadUInt32BigEndian(buffer.AsSpan(offset + 4, 4));
            if (!_senders.TryGetValue(channel, out var sender)) {
                return;
            }

            TaskCompletionSource<bool>? waiter = null;
            lock (sender.Lock) {
                int removed = 0;
                if (sender.Pending.Remove(anchorSeq)) {
                    removed++;
                }
                for (int i = 0; i < 32; i++) {
                    if ((mask & (1u << i)) != 0 && sender.Pending.Remove((ushort)(anchorSeq - 1 - i))) {
                        removed++;
                    }
                }
                if (removed > 0 && AdvanceLowestUnacked(sender)) {
                    waiter = sender.WindowWaiter;
                    sender.WindowWaiter = null;
                }
            }
            waiter?.TrySetResult(true); // wake blocked senders: the window slid forward
        }

        /// <summary>Recomputes LowestUnacked after acks. Returns true when it advanced. Caller holds the lock.</summary>
        private static bool AdvanceLowestUnacked(SenderState sender) {
            ushort previous = sender.LowestUnacked;
            if (sender.Pending.Count == 0) {
                sender.LowestUnacked = sender.NextSeq;
            }
            else {
                int minDistance = int.MaxValue;
                foreach (var seq in sender.Pending.Keys) {
                    int distance = (ushort)(seq - previous);
                    if (distance < minDistance) {
                        minDistance = distance;
                    }
                }
                sender.LowestUnacked = (ushort)(previous + minDistance);
            }
            return sender.LowestUnacked != previous;
        }

        // ---------------------------------------------------------- retransmit

        private async Task RetransmitLoopAsync(CancellationToken ct) {
            var due = new List<byte[]>();
            try {
                while (!ct.IsCancellationRequested) {
                    await Task.Delay(_retransmitScanMs, ct).ConfigureAwait(false);

                    long now = NetTime.NowMs;
                    int ping = _getPingMs();
                    int baseRto = Math.Clamp(ping <= 0 ? 200 : ping * 2 + 30, _rtoFloorMs, _rtoCeilingMs);

                    foreach (var sender in _senders.Values) {
                        due.Clear();
                        bool dead = false;
                        lock (sender.Lock) {
                            foreach (var pending in sender.Pending.Values) {
                                int rto = Math.Min(baseRto << Math.Min(pending.Attempts, 4), _rtoCeilingMs);
                                if (now - pending.LastSentAtMs < rto) {
                                    continue;
                                }
                                if (pending.Attempts >= _maxRetransmits) {
                                    dead = true;
                                    break;
                                }
                                pending.Attempts++;
                                pending.LastSentAtMs = now;
                                due.Add(pending.Message);
                            }
                        }

                        if (dead) {
                            _logger.Warning($"Reliable link dead: packet unacked after {_maxRetransmits} retransmits");
                            SignalDead();
                            return;
                        }
                        foreach (var message in due) {
                            try {
                                await _sendAsync(message).ConfigureAwait(false);
                            }
                            catch (Exception ex) {
                                _logger.Debug($"Retransmit failed: {ex.Message}");
                            }
                        }
                    }
                }
            }
            catch (OperationCanceledException) { }
        }

        private void SignalDead() {
            if (Interlocked.Exchange(ref _deadSignaled, 1) == 0) {
                _onDead(DisconnectReason.Timeout);
            }
        }

        // -------------------------------------------------------------- helpers

        private SenderState GetSender(byte channel) {
            return _senders.TryGetValue(channel, out var sender)
                ? sender
                : throw new ArgumentException($"Channel {channel} has no reliability configured", nameof(channel));
        }

        /// <summary>Wrap-aware distance: positive when <paramref name="a"/> is newer than <paramref name="b"/>.</summary>
        private static int SeqDistance(ushort a, ushort b) => (short)(ushort)(a - b);

        private void FireAndForget(ValueTask task) {
            if (task.IsCompletedSuccessfully) {
                return;
            }
            _ = Observe(task);

            async Task Observe(ValueTask t) {
                try {
                    await t.ConfigureAwait(false);
                }
                catch (Exception ex) {
                    _logger.Debug($"Reliability send failed: {ex.Message}");
                }
            }
        }

        public void Dispose() {
            if (Interlocked.Exchange(ref _disposed, 1) == 1) {
                return;
            }
            _cts?.Cancel();
            _cts?.Dispose();
            foreach (var sender in _senders.Values) {
                TaskCompletionSource<bool>? waiter;
                lock (sender.Lock) {
                    sender.Closed = true;
                    sender.Pending.Clear();
                    waiter = sender.WindowWaiter;
                    sender.WindowWaiter = null;
                }
                waiter?.TrySetResult(true); // blocked senders wake up and observe Closed
            }
        }
    }
}
