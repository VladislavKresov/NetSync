using System;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using NetSync.Diagnostics;
using NetSync.Internal;

namespace NetSync.Transports.Udp {
    /// <summary>
    /// Client-side UDP transport. Connectionless at the socket level; liveness is
    /// tracked via ping/pong. The reliability layer (stage 4) adds retransmission,
    /// ordering and fragmentation on top.
    ///
    /// Payload limit: one datagram, ≤ <see cref="MaxDatagramPayload"/> bytes.
    /// </summary>
    public sealed class UdpTransport : ITransport {
        /// <summary>65507 (max UDP payload) minus our 1-byte packet type header.</summary>
        public const int MaxDatagramPayload = 65506;

        private sealed class Connection {
            public readonly UdpClient Client;
            public readonly IPEndPoint Remote;
            public readonly CancellationTokenSource Cts = new CancellationTokenSource();
            public readonly NetMetrics Metrics = new NetMetrics();
            public long LastPongAtMs = NetTime.NowMs;
            public int DisconnectSignaled;

            public Connection(IPEndPoint remote) {
                Remote = remote;
                Client = new UdpClient(0);
                // Default OS buffers (~64 KB) drop datagrams under bursts; real-time
                // and media traffic needs headroom.
                Client.Client.ReceiveBufferSize = 1 << 20;
                Client.Client.SendBufferSize = 1 << 20;
            }
        }

        private readonly int _pingIntervalMs;
        private readonly int _pingTimeoutMs;
        private readonly INetLogger _logger;
        private Connection? _connection;
        private int _pingMs = -1;

        public bool IsConnected => _connection != null;
        public int PingMs => Volatile.Read(ref _pingMs);
        public NetMetrics? Metrics => _connection?.Metrics;

        public event Action? Connected;
        public event Action<DisconnectReason>? Disconnected;
        public event Action<byte[]>? DataReceived;

        /// <summary>Zero-copy delivery hook, see <see cref="Tcp.TcpTransport.PooledReceive"/>.</summary>
        internal Action<byte[], int, int>? PooledReceive;

        public UdpTransport(int pingIntervalMs = 1000, int pingTimeoutMs = 5000, INetLogger? logger = null) {
            _pingIntervalMs = pingIntervalMs;
            _pingTimeoutMs = pingTimeoutMs;
            _logger = logger ?? NullNetLogger.Instance;
        }

        public Task ConnectAsync(IPEndPoint endpoint, CancellationToken ct = default) {
            Disconnect();

            var connection = new Connection(endpoint);
            _connection = connection;
            _pingMs = -1;

            _ = Task.Run(() => ReceiveLoopAsync(connection));
            _ = Task.Run(() => PingLoopAsync(connection));
            Connected?.Invoke();
            return Task.CompletedTask;
        }

        public ValueTask SendAsync(ReadOnlyMemory<byte> data, CancellationToken ct = default) {
            return SendAsync(ReadOnlyMemory<byte>.Empty, data, ct);
        }

        public async ValueTask SendAsync(ReadOnlyMemory<byte> prefix, ReadOnlyMemory<byte> payload, CancellationToken ct = default) {
            var connection = _connection ?? throw new InvalidOperationException("Not connected");
            int totalLength = prefix.Length + payload.Length;
            if (totalLength > MaxDatagramPayload) {
                throw new ArgumentException(
                    $"UDP payload of {totalLength} bytes exceeds the {MaxDatagramPayload}-byte datagram limit. " +
                    "Use TCP for bulk data until the UDP fragmentation/reliability layer lands.", nameof(payload));
            }

            var packet = PacketProtocol.RentUdpDataPacket(prefix.Span, payload.Span, out int length);
            try {
                await connection.Client.SendAsync(packet, length, connection.Remote).ConfigureAwait(false);
                connection.Metrics.IncrementPacketsSent();
                connection.Metrics.AddBytesSent(length);
            }
            finally {
                PacketProtocol.Return(packet);
            }
        }

        private async Task ReceiveLoopAsync(Connection connection) {
            var ct = connection.Cts.Token;
            var reason = DisconnectReason.Remote;
            try {
                while (!ct.IsCancellationRequested) {
                    // netstandard2.1 has no cancellable ReceiveAsync; Disconnect() closes
                    // the socket, which faults this await and exits the loop.
                    var result = await connection.Client.ReceiveAsync().ConfigureAwait(false);
                    var buf = result.Buffer;
                    if (buf.Length == 0) {
                        continue;
                    }

                    switch ((PacketType)buf[0]) {
                        case PacketType.Ping:
                            if (buf.Length >= PacketProtocol.PingPacketSize) {
                                long ts = PacketProtocol.ReadTimestamp(buf);
                                var pong = PacketProtocol.RentPongPacket(ts);
                                try {
                                    await connection.Client.SendAsync(pong, PacketProtocol.PingPacketSize, result.RemoteEndPoint).ConfigureAwait(false);
                                }
                                finally {
                                    PacketProtocol.Return(pong);
                                }
                            }
                            break;

                        case PacketType.Pong:
                            if (buf.Length >= PacketProtocol.PingPacketSize) {
                                long sentAt = PacketProtocol.ReadTimestamp(buf);
                                Volatile.Write(ref _pingMs, (int)Math.Max(0, NetTime.NowMs - sentAt));
                                Volatile.Write(ref connection.LastPongAtMs, NetTime.NowMs);
                            }
                            break;

                        case PacketType.Data: {
                            connection.Metrics.IncrementPacketsReceived();
                            connection.Metrics.AddBytesReceived(buf.Length);
                            if (PooledReceive != null) {
                                // buf is a fresh array from UdpClient: safe to hand out
                                // sliced, no extra copy needed.
                                PooledReceive(buf, 1, buf.Length - 1);
                            }
                            else {
                                var payload = new byte[buf.Length - 1];
                                Array.Copy(buf, 1, payload, 0, payload.Length);
                                DataReceived?.Invoke(payload);
                            }
                            break;
                        }
                    }
                }
            }
            catch (OperationCanceledException) {
                reason = DisconnectReason.Local;
            }
            catch (Exception ex) when (ex is SocketException || ex is ObjectDisposedException) {
                reason = ct.IsCancellationRequested ? DisconnectReason.Local : DisconnectReason.Error;
            }
            catch (Exception ex) {
                _logger.Error($"UdpTransport receive loop: {ex}");
                reason = DisconnectReason.Error;
            }
            finally {
                CloseConnection(connection, reason);
            }
        }

        private async Task PingLoopAsync(Connection connection) {
            var ct = connection.Cts.Token;
            try {
                while (!ct.IsCancellationRequested) {
                    var ping = PacketProtocol.RentPingPacket(NetTime.NowMs);
                    try {
                        await connection.Client.SendAsync(ping, PacketProtocol.PingPacketSize, connection.Remote).ConfigureAwait(false);
                    }
                    finally {
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
            catch (Exception ex) when (ex is SocketException || ex is ObjectDisposedException) { }
        }

        public void Disconnect() {
            var connection = _connection;
            if (connection != null) {
                CloseConnection(connection, DisconnectReason.Local);
            }
        }

        private void CloseConnection(Connection connection, DisconnectReason reason) {
            if (Interlocked.Exchange(ref connection.DisconnectSignaled, 1) == 1) {
                return;
            }
            if (ReferenceEquals(_connection, connection)) {
                _connection = null;
            }
            try {
                connection.Cts.Cancel();
                connection.Client.Close();
                connection.Cts.Dispose();
            }
            catch (Exception ex) {
                _logger.Debug($"UdpTransport close: {ex.Message}");
            }
            Disconnected?.Invoke(reason);
        }

        public void Dispose() => Disconnect();
    }
}
