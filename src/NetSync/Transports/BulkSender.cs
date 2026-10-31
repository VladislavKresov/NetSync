using System;
using System.Threading;
using System.Threading.Tasks;
using NetSync.Diagnostics;
using NetSync.Internal;
using NetSync.Pipeline.Fragmentation;
using NetSync.Sending;

namespace NetSync.Transports {
    /// <summary>
    /// Sends a large message as fragment frames through a send queue, with backpressure —
    /// for stream links (TCP frames: [type][length][body]) and message links (WebSocket
    /// messages: [type][body]) alike.
    ///
    /// Contract kept for callers: the message memory is only read before SendAsync
    /// returns. Fragments are cut straight from prefix‖payload while the queue has room;
    /// the moment the queue makes us wait, the rest of the message is copied into one
    /// pooled buffer and the transfer continues from there. The common case copies
    /// nothing extra, and a caller can always reuse (or pool-return) its buffer at once.
    /// </summary>
    internal static class BulkSender {
        /// <param name="queue">The link's send queue.</param>
        /// <param name="prefix">First part of the message.</param>
        /// <param name="payload">Rest of the message.</param>
        /// <param name="priority">Queue priority for the fragments.</param>
        /// <param name="metrics">Optional counters.</param>
        /// <param name="lengthPrefixed">True for stream framing (TCP), false for message framing (WebSocket).</param>
        /// <param name="ct">Cancels waiting for queue space.</param>
        public static ValueTask SendAsync(SendQueue queue, ReadOnlyMemory<byte> prefix, ReadOnlyMemory<byte> payload,
                                          SendPriority priority, NetMetrics? metrics, bool lengthPrefixed, CancellationToken ct) {
            int totalLength = prefix.Length + payload.Length;
            uint seqId = PacketFragmenter.NextSequenceId();
            uint count = (uint)((totalLength + PacketFragmenter.FragmentSizeTcp - 1) / PacketFragmenter.FragmentSizeTcp);
            for (uint i = 0; i < count; i++) {
                int offset = (int)(i * PacketFragmenter.FragmentSizeTcp);
                int chunkLength = Math.Min(PacketFragmenter.FragmentSizeTcp, totalLength - offset);
                var frame = lengthPrefixed
                    ? PacketProtocol.RentTcpFragmentFrame(seqId, i, count, prefix.Span, payload.Span, offset, chunkLength, out int frameLength)
                    : PacketProtocol.RentMessageFragment(seqId, i, count, prefix.Span, payload.Span, offset, chunkLength, out frameLength);
                var enqueue = queue.EnqueueAsync(frame, frameLength, pooled: true, priority, ct);
                if (!enqueue.IsCompletedSuccessfully) {
                    int restStart = offset + chunkLength;
                    int restLength = totalLength - restStart;
                    byte[]? rest = null;
                    if (restLength > 0) {
                        rest = Pooled.Rent(restLength);
                        PacketProtocol.CopyRange(prefix.Span, payload.Span, restStart, rest.AsSpan(0, restLength));
                    }
                    return ContinueAsync(queue, enqueue, frame, seqId, i + 1, count, rest, restStart, totalLength, priority, metrics, lengthPrefixed, ct);
                }
                metrics?.IncrementFragmentsSent();
            }
            return default;
        }

        private static async ValueTask ContinueAsync(SendQueue queue, ValueTask pending, byte[] pendingFrame, uint seqId,
                                                     uint nextIndex, uint count, byte[]? rest, int restStart, int totalLength,
                                                     SendPriority priority, NetMetrics? metrics, bool lengthPrefixed, CancellationToken ct) {
            try {
                await AwaitEnqueueAsync(pending, pendingFrame).ConfigureAwait(false);
                metrics?.IncrementFragmentsSent();
                for (uint i = nextIndex; i < count; i++) {
                    int offset = (int)(i * PacketFragmenter.FragmentSizeTcp);
                    int chunkLength = Math.Min(PacketFragmenter.FragmentSizeTcp, totalLength - offset);
                    var chunk = rest.AsSpan(offset - restStart, chunkLength);
                    var frame = lengthPrefixed
                        ? PacketProtocol.RentTcpFragmentFrame(seqId, i, count, chunk, out int frameLength)
                        : PacketProtocol.RentMessageFragment(seqId, i, count, ReadOnlySpan<byte>.Empty, chunk, 0, chunkLength, out frameLength);
                    await AwaitEnqueueAsync(queue.EnqueueAsync(frame, frameLength, pooled: true, priority, ct), frame).ConfigureAwait(false);
                    metrics?.IncrementFragmentsSent();
                }
            }
            finally {
                if (rest != null) {
                    Pooled.Return(rest);
                }
            }
        }

        /// <summary>A failed enqueue leaves the frame with us: return it, report a closed link.</summary>
        private static async ValueTask AwaitEnqueueAsync(ValueTask enqueue, byte[] frame) {
            try {
                await enqueue.ConfigureAwait(false);
            }
            catch (ObjectDisposedException) {
                Pooled.Return(frame);
                throw new InvalidOperationException("Connection closed during send");
            }
            catch {
                Pooled.Return(frame);
                throw;
            }
        }
    }
}
