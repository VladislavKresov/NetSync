using System;
using System.Threading;
using System.Threading.Tasks;
using NetSync.Diagnostics;
using NetSync.Internal;
using NetSync.Pipeline.Fragmentation;
using NetSync.Sending;

namespace NetSync.Transports {
    /// <summary>
    /// NetSync packets over any reliable, message-preserving pipe (a WebSocket, a relay
    /// tunnel): [type:1][payload] per message — Ping, Pong, Data — with a priority send
    /// queue, keepalive bookkeeping, fragmentation of large messages and bounded reassembly.
    /// The pipe only has to write one message at a time and hand complete messages in.
    /// </summary>
    internal sealed class MessageLink : IDisposable {
        /// <summary>Largest message a link may receive: type byte plus a full fragment frame.</summary>
        public const int MaxMessage = 1 + PacketProtocol.MaxTcpFrameBody + 64;

        public readonly SendQueue SendQueue;
        public readonly NetMetrics Metrics = new NetMetrics();
        private readonly FragmentBuffer _fragments;
        private long _lastPongAtMs = NetTime.NowMs;
        private int _pingMs = -1;

        /// <param name="writeMessage">Writes one complete message to the pipe (called by one drain loop at a time).</param>
        /// <param name="settings">Limits and logging.</param>
        public MessageLink(Func<byte[], int, CancellationToken, ValueTask> writeMessage, TransportSettings settings) {
            SendQueue = new SendQueue((buffer, length, ct) => {
                Metrics.AddBytesSent(length);
                return writeMessage(buffer, length, ct);
            }, 0, settings.Logger); // no coalescing: every packet is its own message
            _fragments = Tcp.TcpTransport.CreateFragmentBuffer(settings.MaxMessageSize);
        }

        public int PingMs => Volatile.Read(ref _pingMs);
        public long LastPongAtMs => Volatile.Read(ref _lastPongAtMs);

        public void Start() => SendQueue.Start();

        public ValueTask SendAsync(ReadOnlyMemory<byte> prefix, ReadOnlyMemory<byte> payload, SendPriority priority, CancellationToken ct) {
            if (priority == SendPriority.Critical) {
                priority = SendPriority.High; // Critical is reserved for keepalive
            }
            if (prefix.Length + payload.Length >= PacketFragmenter.MaxUnfragmentedSize) {
                return BulkSender.SendAsync(SendQueue, prefix, payload, PriorityMapping.ForFragments(priority), Metrics, lengthPrefixed: false, ct);
            }
            var message = PacketProtocol.RentUdpDataPacket(prefix.Span, payload.Span, out int length); // [Data][prefix][payload]
            if (!SendQueue.TryEnqueue(message, length, pooled: true, priority)) {
                PacketProtocol.Return(message);
                throw new InvalidOperationException("Send queue overflow (the remote is not keeping up)");
            }
            Metrics.IncrementPacketsSent();
            return default;
        }

        public void SendPing() {
            var ping = PacketProtocol.RentPingPacket(NetTime.NowMs);
            if (!SendQueue.TryEnqueue(ping, PacketProtocol.PingPacketSize, pooled: true, SendPriority.Critical)) {
                PacketProtocol.Return(ping);
            }
        }

        /// <summary>
        /// One received message. Data is handed to <paramref name="deliver"/> (buffer valid only
        /// during that call). False = protocol violation: drop the link.
        /// </summary>
        public bool HandleMessage(byte[] buffer, int offset, int length, Action<byte[], int, int> deliver) {
            if (length == 0) {
                return true;
            }
            switch ((PacketType)buffer[offset]) {
                case PacketType.Ping:
                    if (length >= PacketProtocol.PingPacketSize) {
                        var pong = PacketProtocol.RentPongPacket(PacketProtocol.ReadTimestamp(buffer.AsSpan(offset)));
                        if (!SendQueue.TryEnqueue(pong, PacketProtocol.PingPacketSize, pooled: true, SendPriority.Critical)) {
                            PacketProtocol.Return(pong);
                        }
                    }
                    return true;

                case PacketType.Pong:
                    if (length >= PacketProtocol.PingPacketSize) {
                        long now = NetTime.NowMs;
                        Volatile.Write(ref _pingMs, (int)Math.Max(0, now - PacketProtocol.ReadTimestamp(buffer.AsSpan(offset))));
                        Volatile.Write(ref _lastPongAtMs, now);
                    }
                    return true;

                case PacketType.Data: {
                    Metrics.IncrementPacketsReceived();
                    Metrics.AddBytesReceived(length);
                    var body = new ReadOnlySpan<byte>(buffer, offset + 1, length - 1);
                    if (!PacketFragmenter.TryParse(body, out uint seqId, out uint index, out uint total, out var chunk)) {
                        deliver(buffer, offset + 1, length - 1);
                        return true;
                    }
                    if (!_fragments.TryAddFragmentPooled(seqId, index, total, chunk, out var whole, out int wholeLength)) {
                        return false;
                    }
                    if (whole != null) {
                        try {
                            deliver(whole, 0, wholeLength);
                        }
                        finally {
                            Pooled.Return(whole);
                        }
                    }
                    return true;
                }

                default:
                    return false;
            }
        }

        /// <summary>Liveness check for a keepalive loop: null = fine, otherwise why the link is dead.</summary>
        public string? CheckHealth(TransportSettings settings, bool expectPongs) {
            long now = NetTime.NowMs;
            if (expectPongs && settings.PingTimeoutMs > 0 && now - LastPongAtMs > settings.PingTimeoutMs) {
                Metrics.IncrementPingTimeouts();
                return "no pong within the ping timeout";
            }
            long writeStarted = SendQueue.WriteStartedAtMs;
            if (settings.WriteTimeoutMs > 0 && writeStarted != 0 && now - writeStarted > settings.WriteTimeoutMs) {
                return "write stalled: the remote stopped reading";
            }
            return null;
        }

        public void Dispose() {
            SendQueue.Dispose();
            _fragments.Clear();
        }
    }
}
