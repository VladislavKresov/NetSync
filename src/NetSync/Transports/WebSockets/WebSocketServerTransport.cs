using System;
using System.Collections.Concurrent;
using System.IO;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Tasks;
using NetSync.Diagnostics;
using NetSync.Internal;
using NetSync.Sending;

namespace NetSync.Transports.WebSockets {
    /// <summary>
    /// Server side of <see cref="WebSocketTransportFactory"/>: a TCP listener that performs
    /// the HTTP upgrade itself (optionally inside TLS), then speaks the same packets as the
    /// client. Same hardening as the TCP server: per-peer priority queue, write watchdog,
    /// tiny message limit until the handshake vouches for the peer, bounded handshake time.
    /// </summary>
    internal sealed class WebSocketServerTransport : IPeerFlushingServerTransport, IServerTransport, IPrioritizedServerTransport,
                                                     IPooledReceiveServerTransport, ITrustAwareServerTransport {
        private const int HandshakeTimeoutMs = 10_000;

        private sealed class PeerState {
            public readonly TcpClient Client;
            public readonly WebSocket Socket;
            public readonly MessageLink Link;

            public PeerState(TcpClient client, WebSocket socket, TransportSettings settings) {
                Client = client;
                Socket = socket;
                Link = new MessageLink((buffer, length, ct) =>
                    Socket.SendAsync(buffer.AsMemory(0, length), WebSocketMessageType.Binary, endOfMessage: true, ct), settings);
            }
        }

        private readonly WebSocketTransportFactory _options;
        private readonly TransportSettings _settings;
        private readonly INetLogger _logger;
        private readonly ConcurrentDictionary<long, TransportPeer> _peers = new ConcurrentDictionary<long, TransportPeer>();
        private TcpListener? _listener;
        private CancellationTokenSource? _cts;
        private int _running;

        public int Port { get; private set; }
        public bool IsRunning => _running == 1;

        public event Action<TransportPeer>? PeerConnected;
        public event Action<TransportPeer, DisconnectReason>? PeerDisconnected;
        public event Action<TransportPeer, byte[]>? DataReceived;

        internal Action<TransportPeer, byte[], int, int>? PooledReceive { get; set; }
        internal bool RequireTrust { get; set; }

        Action<TransportPeer, byte[], int, int>? IPooledReceiveServerTransport.PooledReceive {
            get => PooledReceive;
            set => PooledReceive = value;
        }

        bool ITrustAwareServerTransport.RequireTrust {
            get => RequireTrust;
            set => RequireTrust = value;
        }

        void ITrustAwareServerTransport.MarkTrusted(TransportPeer peer) => peer.MarkTrusted();

        public WebSocketServerTransport(WebSocketTransportFactory options, TransportSettings settings) {
            _options = options;
            _settings = settings;
            _logger = settings.Logger;
        }

        public Task<int> StartAsync(int port, CancellationToken ct = default) {
            if (Interlocked.Exchange(ref _running, 1) == 1) {
                throw new InvalidOperationException("Server transport already running");
            }
            try {
                _listener = DualStack.StartTcpListener(port);
            }
            catch {
                Volatile.Write(ref _running, 0);
                throw;
            }
            _cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            Port = ((IPEndPoint)_listener.LocalEndpoint).Port;
            var token = _cts.Token;
            var listener = _listener;
            _ = Task.Run(() => TcpAcceptLoop.RunAsync(listener, client => UpgradeAsync(client, token), _logger, nameof(WebSocketServerTransport), token));
            _ = Task.Run(() => PingLoopAsync(token));
            _logger.Info($"WebSocketServerTransport listening on port {Port} ({(_options.ServerCertificate != null ? "wss" : "ws")}://*{_options.Path})");
            return Task.FromResult(Port);
        }

        /// <summary>Runs per client (see <see cref="TcpAcceptLoop"/>): a slow or silent one can't hold up the others.</summary>
        private async Task UpgradeAsync(TcpClient client, CancellationToken serverCt) {
            client.NoDelay = true;
            var endPoint = DualStack.Normalize((IPEndPoint)client.Client.RemoteEndPoint!);
            var stream = await HandshakeAsync(client, endPoint, serverCt).ConfigureAwait(false);
            if (stream == null) {
                client.Close();
                return;
            }

            var socket = WebSocket.CreateFromStream(stream, isServer: true, subProtocol: null, keepAliveInterval: TimeSpan.Zero);
            var state = new PeerState(client, socket, _settings);
            var peer = new TransportPeer(endPoint, NetTime.NowMs) { TransportState = state };
            state.Link.Start();
            _peers[peer.Id] = peer;
            PeerConnected?.Invoke(peer);
            await ReceiveLoopAsync(peer, state, serverCt).ConfigureAwait(false);
        }

        /// <summary>TLS (when configured) and the HTTP upgrade, within a deadline. Null = refused or failed.</summary>
        private async Task<Stream?> HandshakeAsync(TcpClient client, IPEndPoint endPoint, CancellationToken serverCt) {
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(serverCt);
            timeout.CancelAfter(HandshakeTimeoutMs);
            // TLS and HTTP reads ignore tokens on some runtimes: closing the socket always works.
            using var abortOnTimeout = timeout.Token.Register(() => client.Close());
            try {
                Stream stream = client.GetStream();
                if (_options.ServerCertificate != null) {
                    var tls = new SslStream(stream, leaveInnerStreamOpen: false);
                    await tls.AuthenticateAsServerAsync(_options.ServerCertificate).ConfigureAwait(false);
                    stream = tls;
                }
                var result = await WebSocketHandshake.AcceptAsync(stream, _options.Path, _options.AllowedOrigins, timeout.Token).ConfigureAwait(false);
                if (result != WebSocketHandshake.Result.Accepted) {
                    _logger.Debug($"WebSocket upgrade from {endPoint} refused: {result}");
                    return null;
                }
                return stream;
            }
            catch (Exception ex) when (ex is IOException || ex is SocketException || ex is ObjectDisposedException ||
                                       ex is OperationCanceledException || ex is System.Security.Authentication.AuthenticationException) {
                _logger.Debug($"WebSocket upgrade from {endPoint} failed: {ex.Message}");
                return null;
            }
        }

        private async Task ReceiveLoopAsync(TransportPeer peer, PeerState state, CancellationToken ct) {
            var reason = DisconnectReason.Remote;
            var buffer = new ReceiveBuffer();
            Action<byte[], int, int> deliver = (data, offset, count) => Deliver(peer, data, offset, count);
            try {
                while (!ct.IsCancellationRequested) {
                    // Until the handshake vouches for the peer, only small control messages.
                    int limit = RequireTrust && !peer.IsTrusted ? 1 + PacketProtocol.MaxUntrustedFrameBody : MessageLink.MaxMessage;
                    int length = await WebSocketMessages.ReceiveAsync(state.Socket, buffer, limit, ct).ConfigureAwait(false);
                    if (length == WebSocketMessages.Closed) {
                        break;
                    }
                    peer.Touch(NetTime.NowMs);
                    if (length == WebSocketMessages.Violation || !state.Link.HandleMessage(buffer.Array, buffer.Start, length, deliver)) {
                        _logger.Warning($"WebSocketServerTransport: protocol violation by {peer}; dropping peer");
                        reason = DisconnectReason.Error;
                        break;
                    }
                    buffer.Consume(length);
                }
            }
            catch (OperationCanceledException) {
                reason = DisconnectReason.Local;
            }
            catch (Exception ex) when (ex is WebSocketException || ex is IOException || ex is SocketException || ex is ObjectDisposedException) {
                reason = ct.IsCancellationRequested ? DisconnectReason.Local : DisconnectReason.Remote;
            }
            catch (Exception ex) {
                _logger.Error($"WebSocketServerTransport receive loop ({peer}): {ex}");
                reason = DisconnectReason.Error;
            }
            finally {
                RemovePeer(peer, reason);
                buffer.Dispose();
            }
        }

        private void Deliver(TransportPeer peer, byte[] buffer, int offset, int length) {
            ReceivedPacket.Deliver(PooledReceive, DataReceived, peer, buffer, offset, length); // zero-copy: valid only during the call
        }

        private async Task PingLoopAsync(CancellationToken ct) {
            try {
                while (!ct.IsCancellationRequested) {
                    long now = NetTime.NowMs;
                    foreach (var kvp in _peers) {
                        var peer = kvp.Value;
                        if (peer.TransportState is not PeerState state) {
                            continue;
                        }
                        if (!RequireTrust || peer.IsTrusted) {
                            state.Link.SendPing();
                            peer.SetPing(state.Link.PingMs);
                        }
                        if (_settings.PingTimeoutMs > 0 && now - peer.LastSeenMs > _settings.PingTimeoutMs) {
                            RemovePeer(peer, DisconnectReason.Timeout);
                            continue;
                        }
                        var problem = state.Link.CheckHealth(_settings, expectPongs: false);
                        if (problem != null) {
                            _logger.Warning($"WebSocketServerTransport: {peer}: {problem}; dropping peer");
                            RemovePeer(peer, DisconnectReason.Timeout);
                        }
                    }
                    await Task.Delay(_settings.PingIntervalMs, ct).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) { }
        }

        public ValueTask SendAsync(TransportPeer peer, ReadOnlyMemory<byte> data, CancellationToken ct = default) {
            return SendAsync(peer, ReadOnlyMemory<byte>.Empty, data, SendPriority.Normal, ct);
        }

        public ValueTask SendAsync(TransportPeer peer, ReadOnlyMemory<byte> prefix, ReadOnlyMemory<byte> payload, CancellationToken ct = default) {
            return SendAsync(peer, prefix, payload, SendPriority.Normal, ct);
        }

        public ValueTask SendAsync(TransportPeer peer, ReadOnlyMemory<byte> prefix, ReadOnlyMemory<byte> payload, SendPriority priority, CancellationToken ct = default) {
            if (peer.TransportState is not PeerState state) {
                throw new ArgumentException("Peer does not belong to this transport", nameof(peer));
            }
            return state.Link.SendAsync(prefix, payload, priority, ct);
        }

        System.Threading.Tasks.Task IPeerFlushingServerTransport.FlushPeerAsync(TransportPeer peer, int timeoutMs) {
            return peer.TransportState is PeerState state ? state.Link.SendQueue.WaitUntilIdleAsync(timeoutMs) : System.Threading.Tasks.Task.CompletedTask;
        }

        public void DisconnectPeer(TransportPeer peer) => RemovePeer(peer, DisconnectReason.Local);

        private void RemovePeer(TransportPeer peer, DisconnectReason reason) {
            if (!_peers.TryRemove(peer.Id, out _)) {
                return;
            }
            if (peer.TransportState is PeerState state) {
                try {
                    state.Socket.Abort();
                    state.Client.Close(); // socket first: unblocks a write stuck on a client that stopped reading
                    state.Link.Dispose();
                    state.Socket.Dispose();
                }
                catch (Exception ex) {
                    _logger.Debug($"WebSocketServerTransport peer close: {ex.Message}");
                }
            }
            PeerDisconnected?.Invoke(peer, reason);
        }

        public async Task StopAsync() {
            if (Interlocked.Exchange(ref _running, 0) == 0) {
                return;
            }
            _cts?.Cancel();
            foreach (var kvp in _peers) {
                RemovePeer(kvp.Value, DisconnectReason.Local);
            }
            try {
                _listener?.Stop();
            }
            catch { }
            _listener = null;
            Port = 0;
            _cts?.Dispose();
            _cts = null;
            await Task.CompletedTask;
        }

        public void Dispose() => StopAsync().GetAwaiter().GetResult();
    }
}
