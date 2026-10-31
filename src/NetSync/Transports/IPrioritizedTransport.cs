using System;
using System.Threading;
using System.Threading.Tasks;
using NetSync.Sending;

namespace NetSync.Transports {
    /// <summary>
    /// Optional capability: a client transport with an internal send queue can honor
    /// per-channel priority (<see cref="ChannelPriority"/>). The peer layer uses it when
    /// present and falls back to plain SendAsync otherwise.
    /// </summary>
    public interface IPrioritizedTransport {
        ValueTask SendAsync(ReadOnlyMemory<byte> prefix, ReadOnlyMemory<byte> payload, SendPriority priority, CancellationToken ct = default);
    }

    /// <summary>Server-side counterpart of <see cref="IPrioritizedTransport"/>.</summary>
    public interface IPrioritizedServerTransport {
        ValueTask SendAsync(TransportPeer peer, ReadOnlyMemory<byte> prefix, ReadOnlyMemory<byte> payload, SendPriority priority, CancellationToken ct = default);
    }

    internal static class PriorityMapping {
        public static SendPriority ToSendPriority(ChannelPriority priority) {
            return priority switch {
                ChannelPriority.High => SendPriority.High,
                ChannelPriority.Low => SendPriority.Low,
                _ => SendPriority.Normal
            };
        }

        /// <summary>
        /// Priority for the fragments of a large message: bulk never outranks small
        /// real-time messages unless the channel is explicitly High.
        /// </summary>
        public static SendPriority ForFragments(SendPriority priority) {
            return priority == SendPriority.High ? SendPriority.High : SendPriority.Low;
        }
    }
}
