using System;
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
    /// Hot paths are allocation-free: frames are built in pooled buffers, coalesced into
    /// batched socket writes, and received through one pooled buffer that yields many
    /// frames per socket read.
    ///
    /// Hardened against a hostile or broken server: frame bodies above
    /// <see cref="PacketProtocol.MaxTcpFrameBody"/> and reassembled messages above
    /// <see cref="TransportSettings.MaxMessageSize"/> close the link instead of
    /// allocating; a write stuck longer than <see cref="TransportSettings.WriteTimeoutMs"/>
    /// closes it with <see cref="DisconnectReason.Timeout"/>.
    ///
    /// Reusable: each ConnectAsync builds a fresh connection state.
    /// </summary>
    public sealed class TcpTransport : ITransport, IPrioritizedTransport, IPooledReceiveTransport {
        /// <summary>Socket writes are batched up to this many bytes.</summary>
        internal const int CoalesceBytes = 64 * 1024;

        // Everything that belongs to one physical connection lives here, so a stale
        // receive loop from a previous connection can never touch the current one.
        private sealed class Connection {
            public readonly TcpClient Client;
            public readonly NetworkStream Stream;
            public readonly CancellationTokenSource Cts = new CancellationTokenSource();
            public readonly SendQueue SendQueue;
            public readonly FragmentBuffer FragmentBuffer;
            public readonly NetMetrics Metrics = new NetMetrics();
            public long LastPongAtMs = NetTime.NowMs;
            public int DisconnectSignaled; // 0 = live, 1 = disconnect already handled

            public Connection(TcpClient client, TransportSettings settings) {
                Client = client;
                Stream = client.GetStream();
                SendQueue = new SendQueue(WriteRawAsync, CoalesceBytes, settings.Logger);
                FragmentBuffer = CreateFragmentBuffer(settings.MaxMessageSize);
            }

            // Only the SendQueue drain loop writes, so no lock is needed.
            private ValueTask WriteRawAsync(byte[] buffer, int length, CancellationToken ct) {
                Metrics.AddBytesSent(length);
                return Stream.WriteAsync(buffer.AsMemory(0, length), ct);
            }
        }

        private readonly TransportSettings _settings;
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
        internal Action<byte[], int, int>? PooledReceive { get; set; }

        Action<byte[], int, int>? IPooledReceiveTransport.PooledReceive {
            get => PooledReceive;
            set => PooledReceive = value;
        }

        /// <param name="pingIntervalMs">How often to ping the server.</param>
        /// <param name="pingTimeoutMs">Disconnect when no pong arrives for this long; 0 disables the check.</param>
        /// <param name="logger">Optional logger; defaults to <see cref="NullNetLogger"/>.</param>
        public TcpTransport(int pingIntervalMs = 1000, int pingTimeoutMs = 5000, INetLogger? logger = null)
            : this(TransportSettings.From(pingIntervalMs, pingTimeoutMs, logger)) { }

        public TcpTransport(TransportSettings settings) {
            _settings = settings ?? throw new ArgumentNullException(nameof(settings));
            _logger = settings.Logger;
        }

        internal static FragmentBuffer CreateFragmentBuffer(int maxMessageSize) {
            return new FragmentBuffer {
                MaxPacketBytes = maxMessageSize,
                MaxBufferedBytes = 2L * maxMessageSize,
                MaxFragmentsPerPacket = maxMessageSize / PacketFragmenter.FragmentSizeTcp + 2
            };
        }

        public async Task ConnectAsync(IPEndPoint endpoint, CancellationToken ct = default) {
            Disconnect();

            var client = new TcpClient(endpoint.AddressFamily) { NoDelay = true };
            try {
                await TcpConnect.ConnectAsync(client, endpoint, ct).ConfigureAwait(false);
            }
            catch {
                client.Dispose();
                throw;
            }

            var connection = new Connection(client, _settings);
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
            return SendAsync(prefix, payload, SendPriority.Normal, ct);
        }

        public ValueTask SendAsync(ReadOnlyMemory<byte> prefix, ReadOnlyMemory<byte> payload, SendPriority priority, CancellationToken ct = default) {
            var connection = _connection ?? throw new InvalidOperationException("Not connected");
            if (priority == SendPriority.Critical) {
                priority = SendPriority.High; // Critical is reserved for keepalive
            }
            int totalLength = prefix.Length + payload.Length;

            if (totalLength >= PacketFragmenter.MaxUnfragmentedSize) {
                return BulkSender.SendAsync(connection.SendQueue, prefix, payload, PriorityMapping.ForFragments(priority),
                                            connection.Metrics, lengthPrefixed: true, ct);
            }

            var frame = PacketProtocol.RentTcpDataFrame(prefix.Span, payload.Span, out int length);
            if (!connection.SendQueue.TryEnqueue(frame, length, pooled: true, priority)) {
                PacketProtocol.Return(frame);
                throw new InvalidOperationException("Send queue overflow (the remote is not keeping up)");
            }
            connection.Metrics.IncrementPacketsSent();
            connection.Metrics.UpdateSendQueueSize(connection.SendQueue.Count);
            return default;
        }

        private async Task ReceiveLoopAsync(Connection connection) {
            var ct = connection.Cts.Token;
            var reason = DisconnectReason.Remote;
            var reader = new TcpFrameReader();
            try {
                while (!ct.IsCancellationRequested) {
                    int read = await connection.Stream.ReadAsync(reader.GetReadMemory(), ct).ConfigureAwait(false);
                    if (read == 0) {
                        break; // remote closed
                    }
                    reader.Advance(read);
                    if (!ProcessFrames(connection, reader, ref reason)) {
                        break;
                    }
                }
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
                reader.Dispose();
            }
        }

        /// <summary>Handles every complete buffered frame. False = protocol violation, close the link.</summary>
        private bool ProcessFrames(Connection connection, TcpFrameReader reader, ref DisconnectReason reason) {
            while (true) {
                var kind = reader.TryRead(PacketProtocol.MaxTcpFrameBody, out int offset, out int length, out long timestamp);
                switch (kind) {
                    case TcpFrameReader.FrameKind.None:
                        return true;

                    case TcpFrameReader.FrameKind.Ping: {
                        var pong = PacketProtocol.RentPongPacket(timestamp);
                        if (!connection.SendQueue.TryEnqueue(pong, PacketProtocol.PingPacketSize, pooled: true, SendPriority.Critical)) {
                            PacketProtocol.Return(pong);
                        }
                        break;
                    }

                    case TcpFrameReader.FrameKind.Pong:
                        Volatile.Write(ref _pingMs, (int)Math.Max(0, NetTime.NowMs - timestamp));
                        Volatile.Write(ref connection.LastPongAtMs, NetTime.NowMs);
                        break;

                    case TcpFrameReader.FrameKind.Data:
                        if (!DeliverPayload(connection, reader.Buffer, offset, length)) {
                            _logger.Error("TcpTransport: fragment limits violated by the remote");
                            reason = DisconnectReason.Error;
                            return false;
                        }
                        break;

                    default:
                        _logger.Error($"TcpTransport: invalid frame (type or length {offset})");
                        reason = DisconnectReason.Error;
                        return false;
                }
            }
        }

        // buffer is pooled and only valid during this call. Returns false on a limit violation.
        private bool DeliverPayload(Connection connection, byte[] buffer, int offset, int length) {
            var span = new ReadOnlySpan<byte>(buffer, offset, length);
            if (PacketFragmenter.TryParse(span, out uint seqId, out uint index, out uint total, out var chunk)) {
                connection.Metrics.IncrementFragmentsReceived();
                if (!connection.FragmentBuffer.TryAddFragmentPooled(seqId, index, total, chunk, out var reassembled, out int reassembledLength)) {
                    return false;
                }
                if (reassembled != null) {
                    connection.Metrics.IncrementPacketsReassembled();
                    connection.Metrics.AddBytesReceived(reassembledLength);
                    try {
                        Deliver(reassembled, 0, reassembledLength);
                    }
                    finally {
                        Pooled.Return(reassembled);
                    }
                }
                return true;
            }

            connection.Metrics.IncrementPacketsReceived();
            connection.Metrics.AddBytesReceived(length);
            Deliver(buffer, offset, length);
            return true;
        }

        private void Deliver(byte[] buffer, int offset, int length) {
            ReceivedPacket.Deliver(PooledReceive, DataReceived, buffer, offset, length); // zero-copy: valid only during the call
        }

        private async Task PingLoopAsync(Connection connection) {
            var ct = connection.Cts.Token;
            try {
                while (!ct.IsCancellationRequested) {
                    var ping = PacketProtocol.RentPingPacket(NetTime.NowMs);
                    if (!connection.SendQueue.TryEnqueue(ping, PacketProtocol.PingPacketSize, pooled: true, SendPriority.Critical)) {
                        PacketProtocol.Return(ping);
                    }

                    long now = NetTime.NowMs;
                    if (_settings.PingTimeoutMs > 0 && now - Volatile.Read(ref connection.LastPongAtMs) > _settings.PingTimeoutMs) {
                        connection.Metrics.IncrementPingTimeouts();
                        CloseConnection(connection, DisconnectReason.Timeout);
                        return;
                    }
                    long writeStarted = connection.SendQueue.WriteStartedAtMs;
                    if (_settings.WriteTimeoutMs > 0 && writeStarted != 0 && now - writeStarted > _settings.WriteTimeoutMs) {
                        _logger.Warning("TcpTransport: socket write stalled, the remote stopped reading");
                        CloseConnection(connection, DisconnectReason.Timeout);
                        return;
                    }
                    await Task.Delay(_settings.PingIntervalMs, ct).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) { }
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
                try {
                    if (connection.Client.Connected) {
                        connection.Client.Client.Shutdown(SocketShutdown.Both);
                    }
                }
                catch { }
                // Close the socket before stopping the queue: a write blocked on a
                // remote that stopped reading only returns once the socket is gone.
                connection.Stream.Close();
                connection.Client.Close();
                connection.SendQueue.Dispose();
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
