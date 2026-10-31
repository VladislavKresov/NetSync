using System;
using System.Net;
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Tasks;
using NetSync.Diagnostics;
using NetSync.Sending;

namespace NetSync.Transports.WebSockets {
    /// <summary>
    /// Client side of <see cref="WebSocketTransportFactory"/>: NetSync packets as WebSocket
    /// messages (<see cref="MessageLink"/>), with ping/pong keepalive and a write watchdog.
    /// </summary>
    internal sealed class WebSocketClientTransport : ITransport, IPrioritizedTransport, IPooledReceiveTransport {
        private sealed class Connection {
            public readonly ClientWebSocket Socket;
            public readonly CancellationTokenSource Cts = new CancellationTokenSource();
            public readonly MessageLink Link;
            public int DisconnectSignaled;

            public Connection(ClientWebSocket socket, TransportSettings settings) {
                Socket = socket;
                Link = new MessageLink((buffer, length, ct) =>
                    Socket.SendAsync(buffer.AsMemory(0, length), WebSocketMessageType.Binary, endOfMessage: true, ct), settings);
            }
        }

        private readonly WebSocketTransportFactory _options;
        private readonly TransportSettings _settings;
        private readonly INetLogger _logger;
        private Connection? _connection;

        public bool IsConnected => _connection?.Socket.State == WebSocketState.Open;
        public int PingMs => _connection?.Link.PingMs ?? -1;
        public NetMetrics? Metrics => _connection?.Link.Metrics;

        public event Action? Connected;
        public event Action<DisconnectReason>? Disconnected;
        public event Action<byte[]>? DataReceived;

        internal Action<byte[], int, int>? PooledReceive { get; set; }

        Action<byte[], int, int>? IPooledReceiveTransport.PooledReceive {
            get => PooledReceive;
            set => PooledReceive = value;
        }

        public WebSocketClientTransport(WebSocketTransportFactory options, TransportSettings settings) {
            _options = options;
            _settings = settings;
            _logger = settings.Logger;
        }

        public async Task ConnectAsync(IPEndPoint endpoint, CancellationToken ct = default) {
            Disconnect();

            var socket = new ClientWebSocket();
            socket.Options.KeepAliveInterval = TimeSpan.Zero; // our own ping/pong measures RTT too
            if (_options.RemoteCertificateValidation != null) {
                socket.Options.RemoteCertificateValidationCallback = _options.RemoteCertificateValidation;
            }
            var uri = new UriBuilder(_options.UseTls ? "wss" : "ws",
                                     _options.HostName ?? endpoint.Address.ToString(),
                                     endpoint.Port,
                                     _options.Path).Uri;
            try {
                await socket.ConnectAsync(uri, ct).ConfigureAwait(false);
            }
            catch {
                socket.Dispose();
                throw;
            }

            var connection = new Connection(socket, _settings);
            _connection = connection;
            connection.Link.Start();
            _ = Task.Run(() => ReceiveLoopAsync(connection));
            _ = Task.Run(() => PingLoopAsync(connection));
            Connected?.Invoke();
        }

        public ValueTask SendAsync(ReadOnlyMemory<byte> data, CancellationToken ct = default) {
            return SendAsync(ReadOnlyMemory<byte>.Empty, data, SendPriority.Normal, ct);
        }

        public ValueTask SendAsync(ReadOnlyMemory<byte> prefix, ReadOnlyMemory<byte> payload, CancellationToken ct = default) {
            return SendAsync(prefix, payload, SendPriority.Normal, ct);
        }

        public ValueTask SendAsync(ReadOnlyMemory<byte> prefix, ReadOnlyMemory<byte> payload, SendPriority priority, CancellationToken ct = default) {
            var connection = _connection ?? throw new InvalidOperationException("Not connected");
            return connection.Link.SendAsync(prefix, payload, priority, ct);
        }

        private async Task ReceiveLoopAsync(Connection connection) {
            var ct = connection.Cts.Token;
            var reason = DisconnectReason.Remote;
            var buffer = new ReceiveBuffer();
            Action<byte[], int, int> deliver = Deliver;
            try {
                while (!ct.IsCancellationRequested) {
                    int length = await WebSocketMessages.ReceiveAsync(connection.Socket, buffer, MessageLink.MaxMessage, ct).ConfigureAwait(false);
                    if (length == WebSocketMessages.Closed) {
                        break;
                    }
                    if (length == WebSocketMessages.Violation || !connection.Link.HandleMessage(buffer.Array, buffer.Start, length, deliver)) {
                        _logger.Error("WebSocketTransport: protocol violation by the server");
                        reason = DisconnectReason.Error;
                        break;
                    }
                    buffer.Consume(length);
                }
            }
            catch (OperationCanceledException) {
                reason = DisconnectReason.Local;
            }
            catch (Exception ex) when (ex is WebSocketException || ex is System.IO.IOException || ex is ObjectDisposedException) {
                reason = ct.IsCancellationRequested ? DisconnectReason.Local : DisconnectReason.Remote;
            }
            catch (Exception ex) {
                _logger.Error($"WebSocketTransport receive loop: {ex}");
                reason = DisconnectReason.Error;
            }
            finally {
                CloseConnection(connection, reason);
                buffer.Dispose();
            }
        }

        private void Deliver(byte[] buffer, int offset, int length) {
            ReceivedPacket.Deliver(PooledReceive, DataReceived, buffer, offset, length); // zero-copy: valid only during the call
        }

        private async Task PingLoopAsync(Connection connection) {
            var ct = connection.Cts.Token;
            try {
                while (!ct.IsCancellationRequested) {
                    connection.Link.SendPing();
                    var problem = connection.Link.CheckHealth(_settings, expectPongs: true);
                    if (problem != null) {
                        _logger.Warning($"WebSocketTransport: {problem}");
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
            if (Interlocked.Exchange(ref connection.DisconnectSignaled, 1) == 1) {
                return;
            }
            if (ReferenceEquals(_connection, connection)) {
                _connection = null;
            }
            try {
                connection.Cts.Cancel();
                connection.Socket.Abort(); // unblocks a stuck send or receive at once
                connection.Socket.Dispose();
                connection.Link.Dispose();
                connection.Cts.Dispose();
            }
            catch (Exception ex) {
                _logger.Debug($"WebSocketTransport close: {ex.Message}");
            }
            Disconnected?.Invoke(reason);
        }

        public void Dispose() => Disconnect();
    }
}
