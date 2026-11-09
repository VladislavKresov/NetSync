using System;
using System.Buffers.Binary;
using System.IO;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using NetSync.Diagnostics;
using NetSync.Internal;
using NetSync.Sending;

namespace NetSync.Relay {
    /// <summary>
    /// A TCP connection carrying relay frames ([kind][length][body]) in both directions:
    /// coalescing priority send queue, one pooled receive buffer, bounded frame size,
    /// write watchdog. Used by the relay server, relay hosts and relay clients alike.
    /// </summary>
    internal sealed class FramedConnection : IDisposable {
        /// <summary>Handles one frame; buffer valid only during the call. False = protocol violation.</summary>
        public delegate bool FrameHandler(byte kind, byte[] buffer, int offset, int length);

        private readonly TcpClient _client;
        private readonly NetworkStream _stream;
        private readonly INetLogger _logger;
        private int _closed;

        public SendQueue Queue { get; }

        public FramedConnection(TcpClient client, INetLogger logger, int maxQueuedJobs = 10_000) {
            _client = client;
            _client.NoDelay = true;
            _stream = client.GetStream();
            _logger = logger;
            Queue = new SendQueue((buffer, length, ct) => _stream.WriteAsync(buffer.AsMemory(0, length), ct),
                                  Transports.Tcp.TcpTransport.CoalesceBytes, logger, maxQueuedJobs);
            Queue.Start();
        }

        public bool IsClosed => Volatile.Read(ref _closed) == 1;

        /// <summary>Builds [kind][len][header][body] in a pooled buffer.</summary>
        private static byte[] BuildFrame(byte kind, ReadOnlySpan<byte> header, ReadOnlySpan<byte> body, out int length) {
            length = RelayProtocol.FrameHeaderSize + header.Length + body.Length;
            var frame = Pooled.Rent(length);
            frame[0] = kind;
            BinaryPrimitives.WriteInt32BigEndian(frame.AsSpan(1), header.Length + body.Length);
            header.CopyTo(frame.AsSpan(RelayProtocol.FrameHeaderSize));
            body.CopyTo(frame.AsSpan(RelayProtocol.FrameHeaderSize + header.Length));
            return frame;
        }

        /// <summary>Queues a frame without waiting; false when the queue is full or closed.</summary>
        public bool TrySend(byte kind, ReadOnlySpan<byte> header, ReadOnlySpan<byte> body, SendPriority priority = SendPriority.Normal) {
            var frame = BuildFrame(kind, header, body, out int length);
            if (!Queue.TryEnqueue(frame, length, pooled: true, priority)) {
                Pooled.Return(frame);
                return false;
            }
            return true;
        }

        /// <summary>Queues a frame, waiting for room (backpressure). The bodies are copied before this returns.</summary>
        public ValueTask SendAsync(byte kind, ReadOnlySpan<byte> header, ReadOnlySpan<byte> body, SendPriority priority, CancellationToken ct) {
            var frame = BuildFrame(kind, header, body, out int length);
            var enqueue = Queue.EnqueueAsync(frame, length, pooled: true, priority, ct);
            return enqueue.IsCompletedSuccessfully ? default : AwaitAsync(enqueue, frame);

            static async ValueTask AwaitAsync(ValueTask pending, byte[] frame) {
                try {
                    await pending.ConfigureAwait(false);
                }
                catch {
                    Pooled.Return(frame); // never queued: still ours
                    throw;
                }
            }
        }

        /// <summary>Reads exactly one frame (for handshakes). Returns false on close or a bad frame.</summary>
        public async Task<(bool Ok, byte Kind, byte[] Body)> ReadOneAsync(int maxBody, CancellationToken ct) {
            var header = new byte[RelayProtocol.FrameHeaderSize];
            if (!await ReadExactAsync(header, ct).ConfigureAwait(false)) {
                return (false, 0, Array.Empty<byte>());
            }
            int length = BinaryPrimitives.ReadInt32BigEndian(header.AsSpan(1));
            if (length < 0 || length > maxBody) {
                return (false, 0, Array.Empty<byte>());
            }
            var body = new byte[length];
            if (!await ReadExactAsync(body, ct).ConfigureAwait(false)) {
                return (false, 0, Array.Empty<byte>());
            }
            return (true, header[0], body);
        }

        private async Task<bool> ReadExactAsync(byte[] buffer, CancellationToken ct) {
            int total = 0;
            while (total < buffer.Length) {
                int read = await _stream.ReadAsync(buffer.AsMemory(total), ct).ConfigureAwait(false);
                if (read == 0) {
                    return false;
                }
                total += read;
            }
            return true;
        }

        /// <summary>
        /// Delivers frames until the remote closes (true) or breaks the protocol (false).
        /// Many frames per socket read; frames are handled straight from the buffer.
        /// </summary>
        public async Task<bool> RunReceiveLoopAsync(FrameHandler onFrame, CancellationToken ct) {
            var buffer = new Transports.ReceiveBuffer(); // small until a big frame actually arrives
            int pendingFrameSize = 0;
            try {
                while (!ct.IsCancellationRequested) {
                    int read = await _stream.ReadAsync(buffer.GetReadMemory(pendingFrameSize), ct).ConfigureAwait(false);
                    if (read == 0) {
                        return true;
                    }
                    buffer.Advance(read);
                    pendingFrameSize = 0;
                    while (buffer.Available >= RelayProtocol.FrameHeaderSize) {
                        int length = BinaryPrimitives.ReadInt32BigEndian(buffer.Array.AsSpan(buffer.Start + 1, 4));
                        if (length < 0 || length > RelayProtocol.MaxFrameBody) {
                            return false;
                        }
                        if (buffer.Available < RelayProtocol.FrameHeaderSize + length) {
                            pendingFrameSize = RelayProtocol.FrameHeaderSize + length;
                            break;
                        }
                        if (!onFrame(buffer.Array[buffer.Start], buffer.Array, buffer.Start + RelayProtocol.FrameHeaderSize, length)) {
                            return false;
                        }
                        buffer.Consume(RelayProtocol.FrameHeaderSize + length);
                    }
                }
                return true;
            }
            catch (Exception ex) when (ex is IOException || ex is SocketException || ex is ObjectDisposedException || ex is OperationCanceledException) {
                return true; // connection gone
            }
            finally {
                buffer.Dispose();
            }
        }

        /// <summary>A write stuck this long means the remote stopped reading.</summary>
        public bool IsWriteStalled(int timeoutMs) {
            long started = Queue.WriteStartedAtMs;
            return timeoutMs > 0 && started != 0 && NetTime.NowMs - started > timeoutMs;
        }

        /// <summary>Writes what is already queued (bounded wait), then closes.</summary>
        public async Task DisposeAfterFlushAsync(int timeoutMs) {
            await Queue.WaitUntilIdleAsync(timeoutMs).ConfigureAwait(false);
            Dispose();
        }

        public void Dispose() {
            if (Interlocked.Exchange(ref _closed, 1) == 1) {
                return;
            }
            try {
                _client.Close(); // socket first: unblocks a stuck write
            }
            catch (Exception ex) {
                _logger.Debug($"Relay connection close: {ex.Message}");
            }
            Queue.Dispose();
        }
    }
}
