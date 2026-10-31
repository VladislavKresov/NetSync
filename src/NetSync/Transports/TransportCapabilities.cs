using System;
using NetSync.Internal;

namespace NetSync.Transports {
    /// <summary>
    /// Zero-copy delivery for a client transport: the peer layer receives the transport's
    /// own (pooled) buffer, valid only during the call. When set, DataReceived is not raised.
    /// Built-in transports implement it; custom ones simply raise DataReceived.
    /// </summary>
    internal interface IPooledReceiveTransport {
        Action<byte[], int, int>? PooledReceive { get; set; }
    }

    /// <summary>Server-side counterpart of <see cref="IPooledReceiveTransport"/>.</summary>
    internal interface IPooledReceiveServerTransport {
        Action<TransportPeer, byte[], int, int>? PooledReceive { get; set; }
    }

    /// <summary>
    /// A datagram transport that reports when it has handed over every packet of one
    /// received datagram (a batch carries several). The reliability layer acknowledges
    /// them with one ack per channel at that point instead of one ack per packet.
    /// </summary>
    internal interface IDatagramBoundaryTransport {
        Action? DatagramCompleted { get; set; }
    }

    /// <summary>Server-side counterpart of <see cref="IDatagramBoundaryTransport"/>.</summary>
    internal interface IDatagramBoundaryServerTransport {
        Action<TransportPeer>? DatagramCompleted { get; set; }
    }

    /// <summary>
    /// A server transport that can treat peers as potential attackers until the peer layer
    /// vouches for them: tiny message limit, no keepalive, bounded population.
    /// </summary>
    internal interface ITrustAwareServerTransport {
        bool RequireTrust { get; set; }

        /// <summary>The handshake bound this peer to a connection.</summary>
        void MarkTrusted(TransportPeer peer);
    }

    /// <summary>How every built-in transport hands a received packet to the peer layer.</summary>
    internal static class ReceivedPacket {
        /// <summary>Zero-copy through the pooled hook when one is set (buffer valid only during the call), else an owned copy.</summary>
        public static void Deliver(Action<byte[], int, int>? pooled, Action<byte[]>? copying, byte[] buffer, int offset, int count) {
            if (pooled != null) {
                pooled(buffer, offset, count);
            }
            else {
                copying?.Invoke(Pooled.ToArray(buffer, offset, count));
            }
        }

        /// <summary>Server-side form of <see cref="Deliver(Action{byte[], int, int}, Action{byte[]}, byte[], int, int)"/>.</summary>
        public static void Deliver(Action<TransportPeer, byte[], int, int>? pooled, Action<TransportPeer, byte[]>? copying,
                                   TransportPeer peer, byte[] buffer, int offset, int count) {
            if (pooled != null) {
                pooled(peer, buffer, offset, count);
            }
            else {
                copying?.Invoke(peer, Pooled.ToArray(buffer, offset, count));
            }
        }
    }
}
