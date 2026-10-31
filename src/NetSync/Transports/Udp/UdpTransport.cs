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
    /// tracked via ping/pong. The reliability layer adds retransmission, ordering and
    /// fragmentation on top.
    ///
    /// The socket is connected to the server: the OS drops datagrams from any other
    /// source, and both directions run without allocations. Sends are copied into a
    /// <see cref="DatagramBatcher"/> (packets queued during one socket call share the
    /// next datagram); receives run on a dedicated thread into one reused buffer.
    ///
    /// Payload limit: one datagram, ≤ <see cref="MaxDatagramPayload"/> bytes.
    /// </summary>
    public sealed class UdpTransport : ITransport, IPooledReceiveTransport, IDatagramBoundaryTransport {
        /// <summary>65507 (max UDP payload) minus our 1-byte packet type header.</summary>
        public const int MaxDatagramPayload = 65506;

        private sealed class Connection {
            public readonly Socket Socket;
            public readonly CancellationTokenSource Cts = new CancellationTokenSource();
            public readonly NetMetrics Metrics = new NetMetrics();
            public readonly DatagramBatcher Batcher;
            public readonly DatagramBatcher.Target Target;
            public long LastPongAtMs = NetTime.NowMs;
            public int DisconnectSignaled;

            public Connection(IPEndPoint remote, INetLogger logger) {
                Socket = UdpSockets.CreateClient(remote.AddressFamily);
                try {
                    Socket.Connect(remote);
                }
                catch {
                    Socket.Dispose();
                    throw;
                }
                Batcher = new DatagramBatcher(SendDatagram, logger);
                Target = Batcher.CreateTarget(this);
            }

            private static void SendDatagram(object? destination, byte[] buffer, int offset, int length) {
                var connection = (Connection)destination!;
                SendRaw(connection, buffer, offset, length);
                connection.Metrics.IncrementPacketsSent();
                connection.Metrics.AddBytesSent(length);
            }
        }

        private readonly TransportSettings _settings;
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
        internal Action<byte[], int, int>? PooledReceive { get; set; }

        Action<byte[], int, int>? IPooledReceiveTransport.PooledReceive {
            get => PooledReceive;
            set => PooledReceive = value;
        }

        /// <summary>Raised after the packets of one received datagram were delivered.</summary>
        internal Action? DatagramCompleted { get; set; }

        Action? IDatagramBoundaryTransport.DatagramCompleted {
            get => DatagramCompleted;
            set => DatagramCompleted = value;
        }

        public UdpTransport(int pingIntervalMs = 1000, int pingTimeoutMs = 5000, INetLogger? logger = null)
            : this(TransportSettings.From(pingIntervalMs, pingTimeoutMs, logger)) { }

        public UdpTransport(TransportSettings settings) {
            _settings = settings ?? throw new ArgumentNullException(nameof(settings));
            _logger = settings.Logger;
        }

        public Task ConnectAsync(IPEndPoint endpoint, CancellationToken ct = default) {
            Disconnect();

            var connection = new Connection(endpoint, _logger);
            _connection = connection;
            _pingMs = -1;

            var receiveThread = new Thread(() => ReceiveLoop(connection)) {
                IsBackground = true,
                Name = "NetSync UDP receive"
            };
            receiveThread.Start();
            _ = Task.Run(() => PingLoopAsync(connection));
            Connected?.Invoke();
            return Task.CompletedTask;
        }

        public ValueTask SendAsync(ReadOnlyMemory<byte> data, CancellationToken ct = default) {
            return SendAsync(ReadOnlyMemory<byte>.Empty, data, ct);
        }

        public ValueTask SendAsync(ReadOnlyMemory<byte> prefix, ReadOnlyMemory<byte> payload, CancellationToken ct = default) {
            var connection = _connection ?? throw new InvalidOperationException("Not connected");
            int totalLength = prefix.Length + payload.Length;
            if (totalLength > MaxDatagramPayload) {
                throw new ArgumentException(
                    $"UDP payload of {totalLength} bytes exceeds the {MaxDatagramPayload}-byte datagram limit. " +
                    "Use a Reliable/ReliableOrdered or UnreliableFragmented channel for large messages.", nameof(payload));
            }

            if (Volatile.Read(ref connection.DisconnectSignaled) == 1) {
                throw new InvalidOperationException("Not connected");
            }
            connection.Batcher.Enqueue(connection.Target, prefix.Span, payload.Span); // false = dropped, as on a full socket
            return default;
        }

        private static void SendRaw(Connection connection, byte[] packet, int offset, int length) {
            try {
                connection.Socket.Send(packet, offset, length, SocketFlags.None);
            }
            catch (SocketException ex) when (UdpSockets.IsTransient(ex.SocketErrorCode)) {
                // ICMP feedback for an earlier datagram, or a momentarily full buffer: the
                // datagram is lost, exactly as if the network dropped it.
            }
            catch (ObjectDisposedException) {
                throw new InvalidOperationException("Not connected");
            }
        }

        private void ReceiveLoop(Connection connection) {
            var ct = connection.Cts.Token;
            var reason = DisconnectReason.Remote;
            var buffer = Pooled.Rent(UdpSockets.ReceiveBufferSize);
            try {
                while (!ct.IsCancellationRequested) {
                    int received;
                    try {
                        received = connection.Socket.Receive(buffer, 0, buffer.Length, SocketFlags.None);
                    }
                    catch (SocketException ex) when (!ct.IsCancellationRequested && UdpSockets.IsTransient(ex.SocketErrorCode)) {
                        // ICMP "port unreachable" for an earlier datagram (e.g. the server is
                        // restarting). Real link death is detected by the ping timeout.
                        continue;
                    }
                    if (received == 0) {
                        continue;
                    }

                    switch ((PacketType)buffer[0]) {
                        case PacketType.Ping:
                            if (received >= PacketProtocol.PingPacketSize) {
                                var pong = PacketProtocol.RentPongPacket(PacketProtocol.ReadTimestamp(buffer));
                                try {
                                    SendRaw(connection, pong, 0, PacketProtocol.PingPacketSize);
                                }
                                finally {
                                    PacketProtocol.Return(pong);
                                }
                            }
                            break;

                        case PacketType.Pong:
                            if (received >= PacketProtocol.PingPacketSize) {
                                long sentAt = PacketProtocol.ReadTimestamp(buffer);
                                Volatile.Write(ref _pingMs, (int)Math.Max(0, NetTime.NowMs - sentAt));
                                Volatile.Write(ref connection.LastPongAtMs, NetTime.NowMs);
                            }
                            break;

                        case PacketType.Data:
                        case PacketType.Batch:
                            connection.Metrics.IncrementPacketsReceived();
                            connection.Metrics.AddBytesReceived(received);
                            connection.Batcher.Cork(); // replies to this datagram leave together
                            try {
                                if (buffer[0] == (byte)PacketType.Data) {
                                    Deliver(this, buffer, 1, received - 1);
                                }
                                else {
                                    DatagramBatcher.Unpack(buffer, received, int.MaxValue, this, DeliverPacket);
                                }
                                DatagramCompleted?.Invoke(); // collapsed acks join the corked replies
                            }
                            finally {
                                connection.Batcher.Uncork();
                            }
                            break;
                    }
                }
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
                Pooled.Return(buffer);
            }
        }

        private static readonly Action<UdpTransport, byte[], int, int> DeliverPacket = Deliver;

        private static void Deliver(UdpTransport transport, byte[] buffer, int offset, int length) {
            ReceivedPacket.Deliver(transport.PooledReceive, transport.DataReceived, buffer, offset, length); // zero-copy: valid only during the call
        }

        private async Task PingLoopAsync(Connection connection) {
            var ct = connection.Cts.Token;
            try {
                while (!ct.IsCancellationRequested) {
                    var ping = PacketProtocol.RentPingPacket(NetTime.NowMs);
                    try {
                        SendRaw(connection, ping, 0, PacketProtocol.PingPacketSize);
                    }
                    finally {
                        PacketProtocol.Return(ping);
                    }

                    if (_settings.PingTimeoutMs > 0 && NetTime.NowMs - Volatile.Read(ref connection.LastPongAtMs) > _settings.PingTimeoutMs) {
                        connection.Metrics.IncrementPingTimeouts();
                        CloseConnection(connection, DisconnectReason.Timeout);
                        return;
                    }
                    await Task.Delay(_settings.PingIntervalMs, ct).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) { }
            catch (InvalidOperationException) { } // socket closed under us
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
                connection.Batcher.Dispose();
                connection.Socket.Close(); // unblocks the receive thread
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
