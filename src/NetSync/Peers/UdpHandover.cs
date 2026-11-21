using System;
using System.Threading;
using System.Threading.Tasks;
using NetSync.Internal;
using NetSync.Pipeline.Reliability;
using NetSync.Transports;

namespace NetSync.Peers {
    /// <summary>
    /// Moves a connection's UDP channels to TCP when UDP is lost mid-session, without losing,
    /// duplicating or reordering a message (feature <see cref="PeerProtocol.FeatureLiveDegrade"/>).
    ///
    /// The reliability layer keeps every unacknowledged packet and the receiver drops
    /// duplicates and restores order by sequence number, so the switch is:
    ///   1. stop admitting new messages on reliable UDP channels (they wait here);
    ///   2. point the reliability layer at TCP and resend everything unacknowledged;
    ///   3. once all of it is acknowledged — over TCP that means received and delivered in
    ///      order — let the waiting and all later messages go as plain data over TCP.
    /// Both sides switch: the one that notices sends Degrade, the other follows on receipt,
    /// so acknowledgements travel over TCP too. A connection never switches back.
    /// </summary>
    internal sealed class UdpHandover {
        private const int Normal = 0;
        private const int Switching = 1;
        private const int Switched = 2;

        private int _state;
        private int _routes;   // sends currently inside the reliability layer
        private int _slot = (int)TransportType.Udp;
        private readonly TaskCompletionSource<bool> _done =
            new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>The link the reliability layer sends on.</summary>
        public TransportType ReliableSlot => (TransportType)Volatile.Read(ref _slot);

        public bool HasStarted => Volatile.Read(ref _state) != Normal;

        /// <summary>
        /// A send on a reliable UDP channel starts. True: go through the reliability layer, then
        /// call <see cref="Exit"/>. False: a switch is under way or done — see <see cref="SendAfterSwitchAsync"/>.
        /// </summary>
        public bool TryEnter() {
            Interlocked.Increment(ref _routes);
            if (Volatile.Read(ref _state) == Normal) {
                return true;
            }
            Interlocked.Decrement(ref _routes);
            return false;
        }

        public void Exit() => Interlocked.Decrement(ref _routes);

        /// <summary>
        /// Sends <paramref name="data"/> once the switch is complete. While it is under way the
        /// message waits, so it can't overtake packets still being handed over. The data is
        /// copied first: the caller's buffer is only valid until the send call returns.
        /// </summary>
        public ValueTask SendAfterSwitchAsync(ReadOnlyMemory<byte> data, Func<ReadOnlyMemory<byte>, ValueTask> send) {
            if (_done.Task.IsCompleted) {
                return _done.Task.Result ? send(data) : new ValueTask(Task.FromException(Closed()));
            }
            var copy = Pooled.Copy(data.Span);
            return WaitAndSendAsync(copy, data.Length, send);
        }

        private async ValueTask WaitAndSendAsync(byte[] copy, int length, Func<ReadOnlyMemory<byte>, ValueTask> send) {
            try {
                if (!await _done.Task.ConfigureAwait(false)) {
                    throw Closed();
                }
                await send(new ReadOnlyMemory<byte>(copy, 0, length)).ConfigureAwait(false);
            }
            finally {
                Pooled.Return(copy);
            }
        }

        private static InvalidOperationException Closed() =>
            new InvalidOperationException("The connection closed while moving its UDP channels to TCP");

        /// <summary>Claims the switch; false when it has already begun (the other side's Degrade crossed ours).</summary>
        public bool TryBegin() => Interlocked.CompareExchange(ref _state, Switching, Normal) == Normal;

        /// <summary>Step 2: the reliability layer sends over TCP from now on and resends what is unacknowledged.</summary>
        public void MoveReliableToTcp(ReliableEndpoint? reliable) {
            Volatile.Write(ref _slot, (int)TransportType.Tcp);
            if (reliable == null) {
                return;
            }
            reliable.DeferAcks = false; // acks were flushed at UDP datagram boundaries; TCP has none
            reliable.FlushAcks();
            reliable.ResendAllOnNewPath(NetTime.NowMs);
        }

        /// <summary>Step 3: waits until nothing is in flight, then releases the waiting sends. False on timeout.</summary>
        public async Task<bool> FinishAsync(ReliableEndpoint? reliable, int timeoutMs, CancellationToken ct) {
            long deadline = NetTime.NowMs + timeoutMs;
            while (Volatile.Read(ref _routes) != 0 || (reliable != null && !reliable.IsIdle)) {
                if (NetTime.NowMs > deadline) {
                    return false;
                }
                await Task.Delay(5, ct).ConfigureAwait(false);
            }
            Volatile.Write(ref _state, Switched);
            _done.TrySetResult(true);
            return true;
        }

        /// <summary>The connection closed: waiting sends fail instead of hanging.</summary>
        public void Abort() => _done.TrySetResult(false);
    }
}
