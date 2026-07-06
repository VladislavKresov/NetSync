using System;
using System.Buffers;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using NetSync.Diagnostics;
using NetSync.Internal;
using NetSync.Pipeline.Fragmentation;
using NetSync.Sending;

namespace NetSync.Transports.Tcp {
    /// <summary>
    /// Client-side TCP transport. Frames packets, keeps the link alive with ping/pong,
    /// fragments large payloads so bulk data never blocks small real-time packets.
    ///
    /// Hot paths are allocation-free: frames are built in ArrayPool buffers, queued as
    /// structs and returned to the pool after the socket write; receive uses pooled
    /// buffers plus a single exact-size copy for delivery.
    ///
    /// Reusable: each ConnectAsync builds a fresh connection state.
    /// </summary>
    public sealed class TcpTransport : ITransport {
        // Everything that belongs to one physical connection lives here, so a stale
        // receive loop from a previous connection can never touch the current one.
        private sealed class Connection {
            public readonly TcpClient Client;
            public readonly NetworkStream Stream;
            public readonly CancellationTokenSource Cts = new CancellationTokenSource();
            public readonly SendQueue SendQueue;
            public readonly FragmentBuffer FragmentBuffer = new FragmentBuffer();
            public readonly SemaphoreSlim WriteLock = new SemaphoreSlim(1, 1);
            public readonly NetMetrics Metrics = new NetMetrics();
            public long LastPongAtMs = NetTime.NowMs;
            public int DisconnectSignaled; // 0 = live, 1 = disconnect already handled

            public Connection(TcpClient client, INetLogger logger) {
                Client = client;
                Stream = client.GetStream();
                SendQueue = new SendQueue(WriteRawAsync, logger);
            }

            private async Task WriteRawAsync(byte[] buffer, int length, CancellationToken ct) {
                await WriteLock.WaitAsync(ct).ConfigureAwait(false);
                try {
                    await Stream.WriteAsync(buffer, 0, length, ct).ConfigureAwait(false);
                    Metrics.AddBytesSent(length);
                }
                finally {
                    WriteLock.Release();
                }
            }
        }

        private readonly int _pingIntervalMs;
        private readonly int _pingTimeoutMs;
        private readonly INetLogger _logger;
        private Connection? _connection;
        private int _pingMs = -1;

        public bool IsConnected => _connection?.Client.Connected ?? false;
        public int PingMs => Volatile.Read(ref _pingMs);
        public NetMetrics? Metrics => _connection?.Metrics;

        public event Action? Connected;
        public event Action<DisconnectReason>? Disconnected;
        public event Action<byte[]>? DataReceived;

        /// <summary>
        /// Zero-copy delivery hook for the peer layer: receives the pooled buffer,
        /// valid ONLY for the duration of the call. When set, <see cref="DataReceived"/>
        /// is not raised and no per-packet array is allocated by the transport.
        /// </summary>
        internal Action<byte[], int, int>? PooledReceive;

        /// <param name="pingIntervalMs">How often to ping the server.</param>
        /// <param name="pingTimeoutMs">Disconnect when no pong arrives for this long; 0 disables the check.</param>
        /// <param name="logger">Optional logger; defaults to <see cref="NullNetLogger"/>.</param>
        public TcpTransport(int pingIntervalMs = 1000, int pingTimeoutMs = 5000, INetLogger? logger = null) {
            _pingIntervalMs = pingIntervalMs;
            _pingTimeoutMs = pingTimeoutMs;
            _logger = logger ?? NullNetLogger.Instance;
        }

        public async Task ConnectAsync(IPEndPoint endpoint, CancellationToken ct = default) {
            Disconnect();

            var client = new TcpClient { NoDelay = true };
            try {
                await client.ConnectAsync(endpoint.Address, endpoint.Port).ConfigureAwait(false);
            }
            catch {
                client.Dispose();
                throw;
            }

            var connection = new Connection(client, _logger);
            _connection = connection;
            _pingMs = -1;
            connection.SendQueue.Start();

            _ = Task.Run(() => ReceiveLoopAsync(connection));
            _ = Task.Run(() => PingLoopAsync(connection));
            Connected?.Invoke();
        }

        public ValueTask SendAsync(ReadOnlyMemory<byte> data, CancellationToken ct = default) {
            return SendAsync(ReadOnlyMemory<byte>.Empty, data, ct);
        }

        public ValueTask SendAsync(ReadOnlyMemory<byte> prefix, ReadOnlyMemory<byte> payload, CancellationToken ct = default) {
            var connection = _connection ?? throw new InvalidOperationException("Not connected");
            int totalLength = prefix.Length + payload.Length;

            if (totalLength >= PacketFragmenter.MaxUnfragmentedSize) {
                EnqueueFragmented(connection, prefix.Span, payload.Span, totalLength);
            }
            else {
                var frame = PacketProtocol.RentTcpDataFrame(prefix.Span, payload.Span, out int length);
                if (!connection.SendQueue.TryEnqueue(frame, length, pooled: true, SendPriority.Normal)) {
                    PacketProtocol.Return(frame);
                    throw new InvalidOperationException("Send queue overflow");
                }
                connection.Metrics.IncrementPacketsSent();
            }
            connection.Metrics.UpdateSendQueueSize(connection.SendQueue.Count);
            return default;
        }

        private void EnqueueFragmented(Connection connection, ReadOnlySpan<byte> prefix, ReadOnlySpan<byte> payload, int totalLength) {
            // Bulk path: gather prefix+payload once into a rented scratch buffer, then
            // cut fragments straight from it into rented frame buffers.
            var whole = ArrayPool<byte>.Shared.Rent(totalLength);
            try {
                prefix.CopyTo(whole);
                payload.CopyTo(whole.AsSpan(prefix.Length));

                uint seqId = PacketFragmenter.NextSequenceId();
                uint totalFragments = (uint)((totalLength + PacketFragmenter.FragmentSizeTcp - 1) / PacketFragmenter.FragmentSizeTcp);
                for (uint i = 0; i < totalFragments; i++) {
                    int offset = (int)(i * PacketFragmenter.FragmentSizeTcp);
                    int chunkLength = Math.Min(PacketFragmenter.FragmentSizeTcp, totalLength - offset);
                    var frame = PacketProtocol.RentTcpFragmentFrame(seqId, i, totalFragments, whole.AsSpan(offset, chunkLength), out int frameLength);
                    if (!connection.SendQueue.TryEnqueue(frame, frameLength, pooled: true, SendPriority.Low)) {
                        PacketProtocol.Return(frame);
                        throw new InvalidOperationException("Send queue overflow");
                    }
                    connection.Metrics.IncrementFragmentsSent();
                }
            }
            finally {
                ArrayPool<byte>.Shared.Return(whole);
            }
        }

        private async Task ReceiveLoopAsync(Connection connection) {
            var ct = connection.Cts.Token;
            var reason = DisconnectReason.Remote;
            try {
                var header = new byte[PacketProtocol.PingPacketSize]; // fits both frame header and ping/pong
                while (!ct.IsCancellationRequested) {
                    if (!await ReadExactAsync(connection.Stream, header, 0, 1, ct).ConfigureAwait(false)) {
                        break; // remote closed
                    }

                    var type = (PacketType)header[0];
                    switch (type) {
                        case PacketType.Ping:
                        case PacketType.Pong: {
                            if (!await ReadExactAsync(connection.Stream, header, 1, 8, ct).ConfigureAwait(false)) {
                                goto done;
                            }
                            long timestamp = PacketProtocol.ReadTimestamp(header);
                            if (type == PacketType.Ping) {
                                var pong = PacketProtocol.RentPongPacket(timestamp);
                                if (!connection.SendQueue.TryEnqueue(pong, PacketProtocol.PingPacketSize, pooled: true, SendPriority.Critical)) {
                                    PacketProtocol.Return(pong);
                                }
                            }
                            else {
                                Volatile.Write(ref _pingMs, (int)Math.Max(0, NetTime.NowMs - timestamp));
                                Volatile.Write(ref connection.LastPongAtMs, NetTime.NowMs);
                            }
                            break;
                        }

                        case PacketType.Data: {
                            if (!await ReadExactAsync(connection.Stream, header, 1, 4, ct).ConfigureAwait(false)) {
                                goto done;
                            }
                            int length = PacketProtocol.ReadTcpFrameLength(header);
                            if (length < 0) {
                                _logger.Error($"TcpTransport: invalid frame length {length}");
                                reason = DisconnectReason.Error;
                                goto done;
                            }
                            if (length == 0) {
                                DeliverPayload(connection, Array.Empty<byte>(), 0);
                                break;
                            }

                            var payload = ArrayPool<byte>.Shared.Rent(length);
                            try {
                                if (!await ReadExactAsync(connection.Stream, payload, 0, length, ct).ConfigureAwait(false)) {
                                    goto done;
                                }
                                DeliverPayload(connection, payload, length);
                            }
                            finally {
                                ArrayPool<byte>.Shared.Return(payload);
                            }
                            break;
                        }

                        default:
                            _logger.Error($"TcpTransport: unknown packet type 0x{header[0]:X2}");
                            reason = DisconnectReason.Error;
                            goto done;
                    }
                }
                done: ;
            }
            catch (OperationCanceledException) {
                reason = DisconnectReason.Local;
            }
            catch (Exception ex) when (ex is IOException || ex is SocketException || ex is ObjectDisposedException) {
                reason = ct.IsCancellationRequested ? DisconnectReason.Local : DisconnectReason.Remote;
            }
            catch (Exception ex) {
                _logger.Error($"TcpTransport receive loop: {ex}");
                reason = DisconnectReason.Error;
            }
            finally {
                CloseConnection(connection, reason);
            }
        }

        // buffer is pooled and only valid during this call.
        private void DeliverPayload(Connection connection, byte[] buffer, int length) {
            var span = buffer.AsSpan(0, length);
            if (PacketFragmenter.TryUnwrap(span, out uint seqId, out uint index, out uint total, out var fragmentData)) {
                connection.Metrics.IncrementFragmentsReceived();
                var reassembled = connection.FragmentBuffer.AddFragment(seqId, index, total, fragmentData);
                if (reassembled != null) {
                    connection.Metrics.IncrementPacketsReassembled();
                    connection.Metrics.AddBytesReceived(reassembled.Length);
                    if (PooledReceive != null) {
                        PooledReceive(reassembled, 0, reassembled.Length);
                    }
                    else {
                        DataReceived?.Invoke(reassembled);
                    }
                }
                return;
            }

            connection.Metrics.IncrementPacketsReceived();
            connection.Metrics.AddBytesReceived(length);
            if (PooledReceive != null) {
                PooledReceive(buffer, 0, length);
            }
            else {
                DataReceived?.Invoke(span.ToArray());
            }
        }

        private async Task PingLoopAsync(Connection connection) {
            var ct = connection.Cts.Token;
            try {
                while (!ct.IsCancellationRequested) {
                    var ping = PacketProtocol.RentPingPacket(NetTime.NowMs);
                    if (!connection.SendQueue.TryEnqueue(ping, PacketProtocol.PingPacketSize, pooled: true, SendPriority.Critical)) {
                        PacketProtocol.Return(ping);
                    }

                    if (_pingTimeoutMs > 0 && NetTime.NowMs - Volatile.Read(ref connection.LastPongAtMs) > _pingTimeoutMs) {
                        connection.Metrics.IncrementPingTimeouts();
                        CloseConnection(connection, DisconnectReason.Timeout);
                        return;
                    }
                    await Task.Delay(_pingIntervalMs, ct).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) { }
        }

        private static async Task<bool> ReadExactAsync(Stream stream, byte[] buffer, int offset, int count, CancellationToken ct) {
            int total = 0;
            while (total < count) {
                int read = await stream.ReadAsync(buffer, offset + total, count - total, ct).ConfigureAwait(false);
                if (read == 0) {
                    return false;
                }
                total += read;
            }
            return true;
        }

        public void Disconnect() {
            var connection = _connection;
            if (connection != null) {
                CloseConnection(connection, DisconnectReason.Local);
            }
        }

        private void CloseConnection(Connection connection, DisconnectReason reason) {
            // First caller wins; loops racing into shutdown fire the event exactly once.
            if (Interlocked.Exchange(ref connection.DisconnectSignaled, 1) == 1) {
                return;
            }

            if (ReferenceEquals(_connection, connection)) {
                _connection = null;
            }

            try {
                connection.Cts.Cancel();
                connection.SendQueue.Dispose();
                try {
                    if (connection.Client.Connected) {
                        connection.Client.Client.Shutdown(SocketShutdown.Both);
                    }
                }
                catch { }
                connection.Stream.Close();
                connection.Client.Close();
                connection.WriteLock.Dispose();
                connection.FragmentBuffer.Clear();
                connection.Cts.Dispose();
            }
            catch (Exception ex) {
                _logger.Debug($"TcpTransport close: {ex.Message}");
            }

            Disconnected?.Invoke(reason);
        }

        public void Dispose() => Disconnect();
    }
}
