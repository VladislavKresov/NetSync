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
    /// Reliability engine for one unreliable datagram link (ENet-style ARQ with
    /// TCP-grade congestion control). One instance per datagram link; transport-agnostic
    /// — it only needs a send delegate.
    ///
    /// Per channel, depending on <see cref="ReliabilityMode"/>:
    /// - 16-bit wrap-aware sequence numbers and a 256-packet range window,
    /// - ACKs carrying a 32-packet history bitmask: immediate, or — on links that report
    ///   datagram boundaries (<see cref="DeferAcks"/>) — one per channel per received
    ///   datagram, sent together with the replies to it,
    /// - RTO from ACK-sampled SRTT/RTTVAR (Karn's rule), exponential backoff,
    /// - fast retransmit after 3 later packets were acked (loss recovered in ~1 RTT),
    /// - congestion window per channel: Reno-style, or delay-based scavenger for
    ///   <see cref="ChannelPriority.Low"/> channels,
    /// - duplicate elimination and (ReliableOrdered) reorder buffering,
    /// - fragmentation above <see cref="FragmentSize"/> (1100 B, raised to fit the path MTU
    ///   once measured), reliable or unreliable.
    ///
    /// Threading: HandleRelData/HandleAck are called from the link's receive thread;
    /// Send* may be called from any thread; retransmission timers run on the shared
    /// <see cref="ReliabilityClock"/> thread (<see cref="Tick"/>).
    /// Sender state is guarded by a per-channel lock (then the RTT lock — always in
    /// that order); receiver state is only touched by the receive thread.
    ///
    /// Memory: every packet lives in a pooled buffer. A pending packet's buffer can be
    /// read by a (re)transmission outside the lock while an ACK removes it, so it goes
    /// back to the pool only when no send is reading it (PendingPacket.InFlight).
    /// The send delegate must consume the bytes before it returns (all NetSync links do).
    /// Delivered payloads are valid only during the deliver callback.
    /// </summary>
    internal sealed class ReliableEndpoint : IDisposable {
        /// <summary>Receives a payload; buffer[offset..offset+count) is valid only during the call.</summary>
        public delegate void SegmentHandler(byte channel, byte[] buffer, int offset, int count);

        public const int WindowSize = 256;
        /// <summary>Payloads above this size are split into fragments (fits UDP MTU with headers).</summary>
        public const int FragmentThreshold = 1100;
        /// <summary>Largest fragment the path-MTU logic may set (1472-byte IPv4 datagram minus headers).</summary>
        public const int MaxFragmentSize = 1472 - 1 - RelDataHeaderSize - PacketFragmenter.HeaderSize;

        private int _fragmentSize = FragmentThreshold;

        /// <summary>Fragment payload that fits a datagram of <paramref name="datagramSize"/> bytes with every header.</summary>
        public static int FragmentSizeForDatagram(int datagramSize, bool enveloped) {
            return datagramSize - 1 /* transport type */ - (enveloped ? Security.LinkSecurity.HeaderSize : 0)
                   - RelDataHeaderSize - PacketFragmenter.HeaderSize;
        }

        /// <summary>
        /// Payload bytes per datagram for reliable and fragmented sends. Starts at the
        /// always-safe <see cref="FragmentThreshold"/>; raised when the path MTU has been
        /// measured (fewer packets and ACKs for bulk data). Receivers accept any size.
        /// </summary>
        public int FragmentSize {
            get => Volatile.Read(ref _fragmentSize);
            set => Volatile.Write(ref _fragmentSize, Math.Max(FragmentThreshold, Math.Min(MaxFragmentSize, value)));
        }
        public const int RelDataHeaderSize = 5;   // [type][flags][channel][seq:2]
        public const int RelAckSize = 8;          // [type][channel][anchorSeq:2][mask:4]
        private const byte FlagFragment = 0x01;
        private const int FastRetransmitThreshold = 3;

        private const int MaxSparePendingPackets = 512;

        private sealed class PendingPacket {
            public byte[] Buffer = Array.Empty<byte>();
            public int Length;
            public long FirstSentAtMs;
            public long LastSentAtMs;
            public int Attempts;
            /// <summary>Sends currently reading <see cref="Buffer"/> outside the lock.</summary>
            public int InFlight;
            /// <summary>Acked or disposed while in flight: the last sender recycles it.</summary>
            public bool Released;
        }

        private sealed class SenderState {
            public readonly ReliabilityMode Mode;
            public readonly CongestionWindow? Congestion;
            public ushort NextSeq;
            /// <summary>Oldest unacked seq; equals NextSeq when nothing is pending.
            /// The send window is a RANGE gate: NextSeq may never run more than
            /// WindowSize ahead of this, otherwise the receiver's dedup ring (also
            /// WindowSize) could not distinguish new packets from ancient ones.</summary>
            public ushort LowestUnacked;
            public readonly Dictionary<ushort, PendingPacket> Pending = new Dictionary<ushort, PendingPacket>();
            public readonly Stack<PendingPacket> Spare = new Stack<PendingPacket>();
            public TaskCompletionSource<bool>? WindowWaiter;
            public bool Closed;
            public readonly object Lock = new object();

            public SenderState(ReliabilityMode mode, CongestionWindow? congestion) {
                Mode = mode;
                Congestion = congestion;
            }

            public bool IsReliable => Mode == ReliabilityMode.Reliable || Mode == ReliabilityMode.ReliableOrdered;
        }

        private readonly struct HeldPacket {
            public readonly byte Flags;
            public readonly byte[] Buffer; // pooled copy
            public readonly int Length;

            public HeldPacket(byte flags, byte[] buffer, int length) {
                Flags = flags;
                Buffer = buffer;
                Length = length;
            }
        }

        private sealed class ReceiverState {
            public readonly ReliabilityMode Mode;
            public ushort Highest = 0xFFFF;                       // unordered + sequenced ("-1")
            public readonly bool[] SeenRing;                      // unordered dedup ring
            public ushort Expected;                               // ordered: next seq to deliver
            public readonly Dictionary<ushort, HeldPacket>? Held; // ordered reorder buffer
            public readonly FragmentBuffer? Fragments;
            public uint LastDeliveredMessageId;                   // unreliable-fragmented staleness
            public bool HasDeliveredMessage;
            public bool AckPending;                               // deferred ack waiting for FlushAcks
            public ushort AckAnchor;

            public ReceiverState(ReliabilityMode mode, int maxMessageSize) {
                Mode = mode;
                SeenRing = mode == ReliabilityMode.Reliable ? new bool[WindowSize] : Array.Empty<bool>();
                if (mode == ReliabilityMode.ReliableOrdered) {
                    Held = new Dictionary<ushort, HeldPacket>();
                }
                if (mode == ReliabilityMode.Reliable || mode == ReliabilityMode.ReliableOrdered) {
                    // Under reliability fragments cannot be lost, only delayed by
                    // retransmission — the default 5 s reassembly timeout would drop
                    // slow transfers mid-flight.
                    Fragments = new FragmentBuffer {
                        FragmentTimeoutMs = 600_000,
                        MaxPacketBytes = maxMessageSize,
                        MaxBufferedBytes = 2L * maxMessageSize,
                        MaxFragmentsPerPacket = maxMessageSize / FragmentThreshold + 2
                    };
                }
                else if (mode == ReliabilityMode.UnreliableFragmented) {
                    // Real-time data: an incomplete message is worthless after a moment,
                    // and a handful in flight is plenty (older ones are stale anyway).
                    Fragments = new FragmentBuffer {
                        FragmentTimeoutMs = 2_000,
                        MaxConcurrentPackets = 8,
                        MaxPacketBytes = maxMessageSize,
                        MaxBufferedBytes = 2L * maxMessageSize,
                        MaxFragmentsPerPacket = maxMessageSize / FragmentThreshold + 2
                    };
                }
            }
        }

        private readonly Dictionary<byte, SenderState> _senders = new Dictionary<byte, SenderState>();
        private readonly Dictionary<byte, ReceiverState> _receivers = new Dictionary<byte, ReceiverState>();
        private readonly RttEstimator _rtt = new RttEstimator();
        private readonly object _rttLock = new object();
        private readonly Func<ReadOnlyMemory<byte>, ValueTask> _sendAsync;
        private readonly SegmentHandler _deliver;
        private readonly List<PendingPacket> _fastResend = new List<PendingPacket>(); // receive thread only
        private readonly List<byte> _ackChannels = new List<byte>();                // receive thread only
        private readonly Func<int> _getPingMs;
        private readonly Action<DisconnectReason> _onDead;
        private readonly INetLogger _logger;
        private readonly int _maxRetransmits;
        private readonly int _retransmitScanMs;
        private readonly int _rtoFloorMs;
        private readonly int _rtoCeilingMs;
        private readonly List<PendingPacket> _due = new List<PendingPacket>(); // clock thread only
        private long _lastScanMs;
        private int _started;
        private int _disposed;
        private int _deadSignaled;
        private long _packetsSent;
        private long _retransmissions;
        private long _fastRetransmissions;

        /// <summary>Convenience overload: each delivered payload is copied into its own array.</summary>
        public ReliableEndpoint(
            Dictionary<byte, ChannelConfig> channels,
            Func<ReadOnlyMemory<byte>, ValueTask> sendAsync,
            Action<byte, byte[]> deliver,
            Func<int> getPingMs,
            Action<DisconnectReason> onDead,
            INetLogger logger,
            int maxRetransmits = 12,
            int retransmitScanMs = 10,
            int maxMessageSize = NetConfig.DefaultMaxMessageSize,
            int rtoFloorMs = 30,
            int rtoCeilingMs = 3000,
            bool congestionControl = true)
            : this(channels, sendAsync, (channel, buffer, offset, count) => deliver(channel, Pooled.ToArray(buffer, offset, count)),
                   getPingMs, onDead, logger, maxRetransmits, retransmitScanMs, maxMessageSize, rtoFloorMs, rtoCeilingMs, congestionControl) { }

        public ReliableEndpoint(
            Dictionary<byte, ChannelConfig> channels,
            Func<ReadOnlyMemory<byte>, ValueTask> sendAsync,
            SegmentHandler deliver,
            Func<int> getPingMs,
            Action<DisconnectReason> onDead,
            INetLogger logger,
            int maxRetransmits = 12,
            int retransmitScanMs = 10,
            int maxMessageSize = NetConfig.DefaultMaxMessageSize,
            int rtoFloorMs = 30,
            int rtoCeilingMs = 3000,
            bool congestionControl = true) {
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
                var channel = kvp.Value;
                if (channel.Transport != TransportType.Udp || channel.Reliability == ReliabilityMode.Unreliable) {
                    continue;
                }
                bool reliable = channel.Reliability == ReliabilityMode.Reliable || channel.Reliability == ReliabilityMode.ReliableOrdered;
                var congestion = reliable && congestionControl
                    ? new CongestionWindow(background: channel.Priority == ChannelPriority.Low, maxWindow: WindowSize)
                    : null;
                _senders[kvp.Key] = new SenderState(channel.Reliability, congestion);
                _receivers[kvp.Key] = new ReceiverState(channel.Reliability, maxMessageSize);
            }
        }

        /// <summary>
        /// Collapse the acks for one received datagram into one per channel, sent by
        /// <see cref="FlushAcks"/>. Only for links that call FlushAcks after every datagram
        /// — otherwise acks would never leave. Set before traffic starts.
        /// </summary>
        public bool DeferAcks { get; set; }

        public void Start() {
            if (Interlocked.Exchange(ref _started, 1) == 0) {
                ReliabilityClock.Register(this);
            }
        }

        /// <summary>First transmissions of reliable packets.</summary>
        public long PacketsSent => Interlocked.Read(ref _packetsSent);
        /// <summary>Timeout-driven retransmissions.</summary>
        public long Retransmissions => Interlocked.Read(ref _retransmissions);
        /// <summary>Retransmissions triggered by ACK gaps (before any timeout).</summary>
        public long FastRetransmissions => Interlocked.Read(ref _fastRetransmissions);

        public double SmoothedRttMs {
            get {
                lock (_rttLock) {
                    return _rtt.HasSample ? _rtt.SmoothedRtt : -1;
                }
            }
        }

        /// <summary>Current congestion window of a reliable channel, in packets (-1 if none).</summary>
        public double GetCongestionWindow(byte channel) {
            if (!_senders.TryGetValue(channel, out var sender) || sender.Congestion == null) {
                return -1;
            }
            lock (sender.Lock) {
                return sender.Congestion.Window;
            }
        }

        // ---------------------------------------------------------------- send

        /// <summary>
        /// Sends on a channel according to its configured mode. <paramref name="data"/> is
        /// read only before this returns; the caller may reuse it right away.
        /// </summary>
        public ValueTask SendAsync(byte channel, ReadOnlyMemory<byte> data, CancellationToken ct = default) {
            var sender = GetSender(channel);
            return sender.Mode switch {
                ReliabilityMode.UnreliableSequenced => SendSequencedAsync(channel, data),
                ReliabilityMode.UnreliableFragmented => SendUnreliableFragmentedAsync(channel, data),
                _ => SendReliableAsync(channel, data, ct)
            };
        }

        public ValueTask SendReliableAsync(byte channel, ReadOnlyMemory<byte> data, CancellationToken ct = default) {
            var sender = GetSender(channel);
            if (data.Length <= FragmentSize) {
                var message = BuildRelData(channel, flags: 0, data.Span, default, out int length);
                return EnqueueAndSendAsync(sender, message, length, ct);
            }
            // Fragmented: each chunk is its own reliable packet; the window and the
            // congestion controller pace the stream so a big message cannot flood the link.
            return SendFragmentsAsync(sender, channel, data, reliable: true, ct);
        }

        public ValueTask SendSequencedAsync(byte channel, ReadOnlyMemory<byte> data) {
            var sender = GetSender(channel);
            if (data.Length > FragmentThreshold) {
                throw new ArgumentException(
                    $"UnreliableSequenced payload of {data.Length} bytes exceeds {FragmentThreshold} bytes. " +
                    "Use UnreliableFragmented for large real-time data, or a Reliable/ReliableOrdered channel.", nameof(data));
            }
            var message = BuildRelData(channel, flags: 0, data.Span, default, out int length);
            return SendUnreliable(sender, message, length);
        }

        /// <summary>
        /// Splits a message of any size into datagram-sized fragments, no retransmission.
        /// Always fragment-framed (even single-fragment messages) so the receiver can
        /// order whole messages by their message id.
        /// </summary>
        public ValueTask SendUnreliableFragmentedAsync(byte channel, ReadOnlyMemory<byte> data) {
            return SendFragmentsAsync(GetSender(channel), channel, data, reliable: false, CancellationToken.None);
        }

        /// <summary>Stamps the next seq and sends once; the pooled message is returned right after.</summary>
        private ValueTask SendUnreliable(SenderState sender, byte[] message, int length) {
            lock (sender.Lock) {
                BinaryPrimitives.WriteUInt16BigEndian(message.AsSpan(3, 2), sender.NextSeq++);
            }
            try {
                return _sendAsync(new ReadOnlyMemory<byte>(message, 0, length));
            }
            finally {
                Pooled.Return(message); // the link consumed it before returning
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

        /// <summary>
        /// Sends fragments straight from the caller's memory while that completes
        /// synchronously. As soon as one fragment has to wait (window, congestion), the
        /// rest of the message is copied once into a pooled buffer, so the caller's memory
        /// is never read after this method returns.
        /// </summary>
        private ValueTask SendFragmentsAsync(SenderState sender, byte channel, ReadOnlyMemory<byte> data, bool reliable, CancellationToken ct) {
            uint id = PacketFragmenter.NextSequenceId();
            int size = FragmentSize; // one size for the whole message
            int total = Math.Max(1, (data.Length + size - 1) / size);
            for (int i = 0; i < total; i++) {
                int offset = i * size;
                int chunkLength = Math.Min(size, data.Length - offset);
                var pending = SendFragment(sender, channel, data.Slice(offset, chunkLength), new FragmentInfo(id, (uint)i, (uint)total), reliable, ct);
                if (!pending.IsCompletedSuccessfully) {
                    int restStart = offset + chunkLength;
                    byte[]? rest = restStart < data.Length ? Pooled.Copy(data.Span.Slice(restStart)) : null;
                    return ContinueFragmentsAsync(pending, sender, channel, rest, restStart, data.Length, size, i + 1, id, total, reliable, ct);
                }
            }
            return default;
        }

        private async ValueTask ContinueFragmentsAsync(ValueTask pending, SenderState sender, byte channel, byte[]? rest, int restStart,
                                                       int dataLength, int size, int next, uint id, int total, bool reliable, CancellationToken ct) {
            try {
                await pending.ConfigureAwait(false);
                for (int i = next; i < total; i++) {
                    int offset = i * size;
                    int chunkLength = Math.Min(size, dataLength - offset);
                    var chunk = new ReadOnlyMemory<byte>(rest, offset - restStart, chunkLength);
                    await SendFragment(sender, channel, chunk, new FragmentInfo(id, (uint)i, (uint)total), reliable, ct).ConfigureAwait(false);
                }
            }
            finally {
                if (rest != null) {
                    Pooled.Return(rest);
                }
            }
        }

        private ValueTask SendFragment(SenderState sender, byte channel, ReadOnlyMemory<byte> chunk, FragmentInfo fragment, bool reliable, CancellationToken ct) {
            var message = BuildRelData(channel, FlagFragment, chunk.Span, fragment, out int length);
            return reliable ? EnqueueAndSendAsync(sender, message, length, ct) : SendUnreliable(sender, message, length);
        }

        /// <summary>
        /// Admits a reliable packet into the window and transmits it. The fast path (window
        /// open) completes synchronously without an async state machine; otherwise waits.
        /// Takes ownership of the pooled <paramref name="message"/> in every outcome.
        /// </summary>
        private ValueTask EnqueueAndSendAsync(SenderState sender, byte[] message, int length, CancellationToken ct) {
            PendingPacket? pending;
            lock (sender.Lock) {
                if (sender.Closed) {
                    Pooled.Return(message);
                    return new ValueTask(Task.FromException(new InvalidOperationException("Reliable endpoint is disposed")));
                }
                pending = TryAdmitLocked(sender, message, length);
            }
            if (pending != null) {
                return TransmitFirst(sender, pending);
            }
            return EnqueueSlowAsync(sender, message, length, ct);
        }

        private async ValueTask EnqueueSlowAsync(SenderState sender, byte[] message, int length, CancellationToken ct) {
            // Backpressure: wait while the seq range is a full window or the congestion
            // window says the link has enough in flight.
            PendingPacket? pending = null;
            try {
                while (pending == null) {
                    TaskCompletionSource<bool> waiter;
                    lock (sender.Lock) {
                        if (sender.Closed) {
                            throw new InvalidOperationException("Reliable endpoint is disposed");
                        }
                        pending = TryAdmitLocked(sender, message, length);
                        if (pending != null) {
                            break;
                        }
                        waiter = sender.WindowWaiter ??= new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
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
            }
            catch {
                if (pending == null) {
                    Pooled.Return(message); // never admitted: still ours
                }
                throw;
            }
            await TransmitFirst(sender, pending).ConfigureAwait(false);
        }

        /// <summary>Window check + bookkeeping for a new reliable packet. Caller holds the lock.</summary>
        private static PendingPacket? TryAdmitLocked(SenderState sender, byte[] message, int length) {
            bool rangeOpen = (ushort)(sender.NextSeq - sender.LowestUnacked) < WindowSize;
            bool congestionOpen = sender.Congestion == null || sender.Pending.Count < sender.Congestion.AllowedInFlight;
            if (!rangeOpen || !congestionOpen) {
                return null;
            }
            ushort seq = sender.NextSeq++;
            BinaryPrimitives.WriteUInt16BigEndian(message.AsSpan(3, 2), seq);
            long now = NetTime.NowMs;
            var pending = sender.Spare.Count > 0 ? sender.Spare.Pop() : new PendingPacket();
            pending.Buffer = message;
            pending.Length = length;
            pending.FirstSentAtMs = now;
            pending.LastSentAtMs = now;
            pending.Attempts = 0;
            pending.InFlight = 1; // the first transmission is about to read it
            pending.Released = false;
            sender.Pending[seq] = pending;
            return pending;
        }

        /// <summary>First transmission: a failure is only logged — the retransmit timer covers it.</summary>
        private ValueTask TransmitFirst(SenderState sender, PendingPacket pending) {
            Interlocked.Increment(ref _packetsSent);
            ValueTask send;
            try {
                send = Transmit(sender, pending);
            }
            catch (Exception ex) {
                _logger.Debug($"Reliable send failed (will retransmit): {ex.Message}");
                return default;
            }
            return send.IsCompletedSuccessfully ? default : ObserveAsync(send);
        }

        private async ValueTask ObserveAsync(ValueTask send) {
            try {
                await send.ConfigureAwait(false);
            }
            catch (Exception ex) {
                _logger.Debug($"Reliable send failed (will retransmit): {ex.Message}");
            }
        }

        /// <summary>Sends a pending packet whose InFlight the caller already incremented.</summary>
        private ValueTask Transmit(SenderState sender, PendingPacket pending) {
            try {
                return _sendAsync(new ReadOnlyMemory<byte>(pending.Buffer, 0, pending.Length));
            }
            finally {
                lock (sender.Lock) {
                    if (--pending.InFlight == 0 && pending.Released) {
                        Recycle(sender, pending);
                    }
                }
            }
        }

        /// <summary>The packet left the window (acked / disposed). Caller holds the lock.</summary>
        private static void ReleaseLocked(SenderState sender, PendingPacket pending) {
            if (pending.InFlight > 0) {
                pending.Released = true; // a send is still reading it; Transmit recycles
                return;
            }
            Recycle(sender, pending);
        }

        private static void Recycle(SenderState sender, PendingPacket pending) {
            Pooled.Return(pending.Buffer);
            pending.Buffer = Array.Empty<byte>();
            pending.Released = false;
            if (sender.Spare.Count < MaxSparePendingPackets) {
                sender.Spare.Push(pending);
            }
        }

        /// <summary>Builds [RelData][flags][channel][seq=0][fragment header?][payload] in a pooled buffer.</summary>
        private static byte[] BuildRelData(byte channel, byte flags, ReadOnlySpan<byte> payload, FragmentInfo fragment, out int length) {
            bool fragmented = (flags & FlagFragment) != 0;
            int fragmentHeader = fragmented ? PacketFragmenter.HeaderSize : 0;
            length = RelDataHeaderSize + fragmentHeader + payload.Length;
            var message = Pooled.Rent(length);
            message[0] = PeerProtocol.MsgRelData;
            message[1] = flags;
            message[2] = channel;
            message[3] = 0;
            message[4] = 0;
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

                case ReliabilityMode.UnreliableFragmented:
                    HandleUnreliableFragment(receiver, channel, flags, buffer, payloadOffset, payloadCount);
                    break; // never acks

                case ReliabilityMode.Reliable:
                    HandleUnordered(receiver, channel, flags, seq, buffer, payloadOffset, payloadCount);
                    break;

                case ReliabilityMode.ReliableOrdered:
                    HandleOrdered(receiver, channel, flags, seq, buffer, payloadOffset, payloadCount);
                    break;
            }
        }

        private void HandleUnreliableFragment(ReceiverState receiver, byte channel, byte flags, byte[] buffer, int offset, int count) {
            if ((flags & FlagFragment) == 0 ||
                !PacketFragmenter.TryParse(buffer.AsSpan(offset, count), out uint messageId, out uint index, out uint total, out var chunk)) {
                return; // this mode is always fragment-framed
            }
            if (receiver.HasDeliveredMessage && MessageIdDistance(messageId, receiver.LastDeliveredMessageId) <= 0) {
                return; // a newer message was already delivered: this one is stale, don't even buffer it
            }
            if (!receiver.Fragments!.TryAddFragmentPooled(messageId, index, total, chunk, out var complete, out int completeLength)) {
                _logger.Warning("Unreliable-fragmented channel: remote violated fragment limits; closing");
                SignalDead(DisconnectReason.Error);
                return;
            }
            if (complete == null) {
                return;
            }
            try {
                if (receiver.HasDeliveredMessage && MessageIdDistance(messageId, receiver.LastDeliveredMessageId) <= 0) {
                    return; // completed after a newer one: stale
                }
                receiver.LastDeliveredMessageId = messageId;
                receiver.HasDeliveredMessage = true;
                _deliver(channel, complete, 0, completeLength);
            }
            finally {
                Pooled.Return(complete);
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
                    try {
                        DeliverPayload(receiver, channel, held.Flags, held.Buffer, 0, held.Length);
                    }
                    finally {
                        Pooled.Return(held.Buffer);
                    }
                    receiver.Expected++;
                }
            }
            else {
                var copy = Pooled.Copy(new ReadOnlySpan<byte>(buffer, payloadOffset, payloadCount));
                receiver.Held![seq] = new HeldPacket(flags, copy, payloadCount);
            }
            SendAck(channel, seq, receiver);
        }

        private void DeliverPayload(ReceiverState receiver, byte channel, byte flags, byte[] buffer, int offset, int count) {
            if ((flags & FlagFragment) != 0) {
                if (receiver.Fragments == null ||
                    !PacketFragmenter.TryParse(buffer.AsSpan(offset, count), out uint fragId, out uint index, out uint total, out var chunk)) {
                    return; // malformed fragment
                }
                if (!receiver.Fragments.TryAddFragmentPooled(fragId, index, total, chunk, out var complete, out int completeLength)) {
                    _logger.Warning("Reliable channel: remote violated fragment limits (message too large?); closing");
                    SignalDead(DisconnectReason.Error);
                    return;
                }
                if (complete != null) {
                    try {
                        _deliver(channel, complete, 0, completeLength);
                    }
                    finally {
                        Pooled.Return(complete);
                    }
                }
                return;
            }

            _deliver(channel, buffer, offset, count); // zero-copy: valid during the call
        }

        // ---------------------------------------------------------------- acks

        private void SendAck(byte channel, ushort seq, ReceiverState receiver) {
            if (!DeferAcks) {
                SendAckNow(channel, seq, receiver);
                return;
            }
            if (!receiver.AckPending) {
                receiver.AckPending = true;
                receiver.AckAnchor = seq;
                _ackChannels.Add(channel);
                return;
            }
            // One ack covers its anchor and the 32 seqs below it: anchor on the newest seq,
            // and send right away whatever that mask could not cover.
            int distance = SeqDistance(seq, receiver.AckAnchor);
            if (distance > 0) {
                if (distance >= 32) {
                    SendAckNow(channel, receiver.AckAnchor, receiver);
                }
                receiver.AckAnchor = seq;
            }
            else if (distance <= -32) {
                SendAckNow(channel, seq, receiver);
            }
        }

        /// <summary>Sends the acks deferred since the last call. Receive thread only.</summary>
        public void FlushAcks() {
            var channels = _ackChannels;
            for (int i = 0; i < channels.Count; i++) {
                var receiver = _receivers[channels[i]];
                if (receiver.AckPending) {
                    receiver.AckPending = false;
                    SendAckNow(channels[i], receiver.AckAnchor, receiver);
                }
            }
            channels.Clear();
        }

        private void SendAckNow(byte channel, ushort anchorSeq, ReceiverState receiver) {
            var ack = Pooled.Rent(RelAckSize);
            ack[0] = PeerProtocol.MsgRelAck;
            ack[1] = channel;
            BinaryPrimitives.WriteUInt16BigEndian(ack.AsSpan(2, 2), anchorSeq);
            BinaryPrimitives.WriteUInt32BigEndian(ack.AsSpan(4, 4), BuildAckMask(receiver, anchorSeq));
            try {
                _sendAsync(new ReadOnlyMemory<byte>(ack, 0, RelAckSize)).Forget(_logger);
            }
            catch (Exception ex) {
                _logger.Debug($"Ack send failed: {ex.Message}");
            }
            finally {
                Pooled.Return(ack);
            }
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
            if (!_senders.TryGetValue(channel, out var sender) || !sender.IsReliable) {
                return;
            }

            long now = NetTime.NowMs;
            TaskCompletionSource<bool>? waiter = null;
            var fastResend = _fastResend;
            fastResend.Clear();
            lock (sender.Lock) {
                int removed = 0;
                if (sender.Pending.TryGetValue(anchorSeq, out var anchorPacket)) {
                    sender.Pending.Remove(anchorSeq);
                    removed++;
                    if (anchorPacket.Attempts == 0) {
                        // Karn's rule: only never-retransmitted packets give an unambiguous RTT.
                        lock (_rttLock) {
                            _rtt.AddSample(now - anchorPacket.FirstSentAtMs, now);
                        }
                    }
                    ReleaseLocked(sender, anchorPacket);
                }
                for (int i = 0; i < 32; i++) {
                    var acked = (ushort)(anchorSeq - 1 - i);
                    if ((mask & (1u << i)) != 0 && sender.Pending.TryGetValue(acked, out var ackedPacket)) {
                        sender.Pending.Remove(acked);
                        ReleaseLocked(sender, ackedPacket);
                        removed++;
                    }
                }
                if (removed == 0) {
                    return;
                }

                AdvanceLowestUnacked(sender);
                if (sender.Congestion != null) {
                    lock (_rttLock) {
                        sender.Congestion.OnAcked(removed, _rtt, now);
                    }
                }

                // Fast retransmit: packets this far behind an acked one are almost surely lost.
                bool lossDetected = false;
                for (ushort seq = sender.LowestUnacked; SeqDistance(anchorSeq, seq) >= FastRetransmitThreshold; seq++) {
                    if (sender.Pending.TryGetValue(seq, out var lost) && lost.Attempts == 0) {
                        lost.Attempts = 1;
                        lost.LastSentAtMs = now;
                        lost.InFlight++;
                        fastResend.Add(lost);
                        lossDetected = true;
                    }
                }
                if (lossDetected && sender.Congestion != null) {
                    lock (_rttLock) {
                        sender.Congestion.OnLoss(_rtt, now);
                    }
                }

                // Any removal may free in-flight capacity: wake blocked senders.
                waiter = sender.WindowWaiter;
                sender.WindowWaiter = null;
            }
            waiter?.TrySetResult(true);

            if (fastResend.Count > 0) {
                Interlocked.Add(ref _fastRetransmissions, fastResend.Count);
                foreach (var lost in fastResend) {
                    try {
                        Transmit(sender, lost).Forget(_logger);
                    }
                    catch (Exception ex) {
                        _logger.Debug($"Fast retransmit failed: {ex.Message}");
                    }
                }
                fastResend.Clear();
            }
        }

        /// <summary>Recomputes LowestUnacked after acks. Caller holds the lock.</summary>
        private static void AdvanceLowestUnacked(SenderState sender) {
            ushort previous = sender.LowestUnacked;
            if (sender.Pending.Count == 0) {
                sender.LowestUnacked = sender.NextSeq;
                return;
            }
            int minDistance = int.MaxValue;
            foreach (var seq in sender.Pending.Keys) {
                int distance = (ushort)(seq - previous);
                if (distance < minDistance) {
                    minDistance = distance;
                }
            }
            sender.LowestUnacked = (ushort)(previous + minDistance);
        }

        // ---------------------------------------------------------- retransmit

        /// <summary>
        /// One timer pass (called by <see cref="ReliabilityClock"/>): resends what timed out,
        /// declares the link dead after too many attempts. Never blocks — sends are handed to
        /// the link, which consumes them synchronously.
        /// </summary>
        internal void Tick(long now) {
            if (Volatile.Read(ref _disposed) == 1 || Volatile.Read(ref _deadSignaled) == 1 || now - _lastScanMs < _retransmitScanMs) {
                return;
            }
            _lastScanMs = now;
            try {
                int ping = _getPingMs();
                int baseRto;
                lock (_rttLock) {
                    baseRto = _rtt.RetransmitTimeoutMs(_rtoFloorMs, _rtoCeilingMs, fallbackMs: ping <= 0 ? 200 : ping * 2 + 30);
                }

                var due = _due;
                foreach (var sender in _senders.Values) {
                    if (!sender.IsReliable) {
                        continue;
                    }
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
                            pending.InFlight++;
                            due.Add(pending);
                        }
                        if (due.Count > 0 && sender.Congestion != null) {
                            lock (_rttLock) {
                                sender.Congestion.OnLoss(_rtt, now);
                            }
                        }
                    }

                    Interlocked.Add(ref _retransmissions, due.Count);
                    foreach (var pending in due) {
                        try {
                            Transmit(sender, pending).Forget(_logger);
                        }
                        catch (Exception ex) {
                            _logger.Debug($"Retransmit failed: {ex.Message}");
                        }
                    }
                    due.Clear();
                    if (dead) {
                        _logger.Warning($"Reliable link dead: packet unacked after {_maxRetransmits} retransmits");
                        SignalDead(DisconnectReason.Timeout);
                        return;
                    }
                }
            }
            catch (Exception ex) {
                _logger.Error($"Reliability timer: {ex}"); // never let one endpoint stop the shared clock
            }
        }

        private void SignalDead(DisconnectReason reason) {
            if (Interlocked.Exchange(ref _deadSignaled, 1) == 0) {
                _onDead(reason);
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

        /// <summary>Wrap-aware distance for 32-bit message ids.</summary>
        private static int MessageIdDistance(uint a, uint b) => (int)(a - b);

        public void Dispose() {
            if (Interlocked.Exchange(ref _disposed, 1) == 1) {
                return;
            }
            ReliabilityClock.Unregister(this);
            foreach (var sender in _senders.Values) {
                TaskCompletionSource<bool>? waiter;
                lock (sender.Lock) {
                    sender.Closed = true;
                    foreach (var pending in sender.Pending.Values) {
                        ReleaseLocked(sender, pending);
                    }
                    sender.Pending.Clear();
                    waiter = sender.WindowWaiter;
                    sender.WindowWaiter = null;
                }
                waiter?.TrySetResult(true); // blocked senders wake up and observe Closed
            }
        }
    }
}
