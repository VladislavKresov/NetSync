using System;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NetSync.Diagnostics;
using NetSync.Internal;
using NetSync.Sending;
using NetSync.Transports;

namespace NetSync.Relay {
    /// <summary>Client stream slot through a relay: one TCP connection, NetSync packets as Data frames.</summary>
    internal sealed class RelayStreamClientTransport : ITransport, IPrioritizedTransport, IPooledReceiveTransport {
        private sealed class Connection {
            public readonly FramedConnection Framed;
            public readonly MessageLink Link;
            public readonly CancellationTokenSource Cts = new CancellationTokenSource();
            public int DisconnectSignaled;

            public Connection(FramedConnection framed, TransportSettings settings) {
                Framed = framed;
                Link = new MessageLink((buffer, length, ct) =>
                    Framed.SendAsync(RelayProtocol.Data, ReadOnlySpan<byte>.Empty, new ReadOnlySpan<byte>(buffer, 0, length), SendPriority.Normal, ct),
                    settings);
            }
        }

        private readonly RelayRoom _room;
        private readonly TransportSettings _settings;
        private readonly INetLogger _logger;
        private Connection? _connection;

        public bool IsConnected => _connection != null;
        public int PingMs => _connection?.Link.PingMs ?? -1;

        public event Action? Connected;
        public event Action<DisconnectReason>? Disconnected;
        public event Action<byte[]>? DataReceived;

        internal Action<byte[], int, int>? PooledReceive { get; set; }

        Action<byte[], int, int>? IPooledReceiveTransport.PooledReceive {
            get => PooledReceive;
            set => PooledReceive = value;
        }

        public RelayStreamClientTransport(RelayRoom room, TransportSettings settings) {
            _room = room;
            _settings = settings;
            _logger = settings.Logger;
        }

        /// <param name="endpoint">The relay server.</param>
        /// <param name="ct">Cancels the attempt.</param>
        public async Task ConnectAsync(IPEndPoint endpoint, CancellationToken ct = default) {
            Disconnect();
            var client = new TcpClient(endpoint.AddressFamily);
            try {
                await TcpConnect.ConnectAsync(client, endpoint, ct).ConfigureAwait(false);
            }
            catch {
                client.Dispose();
                throw;
            }
            var framed = new FramedConnection(client, _logger);
            try {
                framed.TrySend(RelayProtocol.ClientHello, RelayProtocol.EncodeRoom(_room.RoomId), ReadOnlySpan<byte>.Empty, SendPriority.High);
                using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
                timeout.CancelAfter(10_000);
                using var abort = timeout.Token.Register(framed.Dispose);
                var (ok, kind, body) = await framed.ReadOneAsync(1024, timeout.Token).ConfigureAwait(false);
                if (!ok || kind != RelayProtocol.ClientWelcome) {
                    string reason = ok && kind == RelayProtocol.Refused ? Encoding.UTF8.GetString(body) : "no answer";
                    throw new InvalidOperationException($"Relay {endpoint} refused room '{_room.RoomId}': {reason}");
                }
            }
            catch {
                framed.Dispose();
                throw;
            }

            var connection = new Connection(framed, _settings);
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
            Action<byte[], int, int> deliver = Deliver;
            bool clean = await connection.Framed.RunReceiveLoopAsync((kind, buffer, offset, length) => kind switch {
                RelayProtocol.Data => connection.Link.HandleMessage(buffer, offset, length, deliver),
                RelayProtocol.Keepalive => true,
                _ => false
            }, connection.Cts.Token).ConfigureAwait(false);
            CloseConnection(connection, connection.Cts.IsCancellationRequested ? DisconnectReason.Local
                                       : clean ? DisconnectReason.Remote : DisconnectReason.Error);
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
                        _logger.Warning($"Relay stream link: {problem}");
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
            connection.Cts.Cancel();
            connection.Framed.Dispose();
            connection.Link.Dispose();
            Disconnected?.Invoke(reason);
        }

        public void Dispose() => Disconnect();
    }

    /// <summary>
    /// Host stream slot through a relay: every stream client of the room is a peer,
    /// multiplexed over the host's single control connection.
    /// </summary>
    internal sealed class RelayStreamHostTransport : IServerTransport, IPeerFlushingServerTransport, IPrioritizedServerTransport,
                                                     IPooledReceiveServerTransport, ITrustAwareServerTransport {
        private sealed class PeerState {
            public readonly uint ClientId;
            public readonly MessageLink Link;
            public Action<byte[], int, int> Deliver = (_, _, _) => { }; // set once per peer: no closure per message

            public PeerState(uint clientId, RelayHostSession session, TransportSettings settings) {
                ClientId = clientId;
                var id = new byte[4];
                BinaryPrimitives.WriteUInt32BigEndian(id, clientId);
                Link = new MessageLink((buffer, length, ct) =>
                    session.Connection.SendAsync(RelayProtocol.PeerData, id, new ReadOnlySpan<byte>(buffer, 0, length), SendPriority.Normal, ct),
                    settings);
            }
        }

        private readonly RelayRoom _room;
        private readonly TransportSettings _settings;
        private readonly INetLogger _logger;
        private readonly ConcurrentDictionary<uint, TransportPeer> _peers = new ConcurrentDictionary<uint, TransportPeer>();
        private readonly Action _onSessionClosed;
        private readonly Action<RelayHostSession> _onSessionRestored;
        private RelayHostSession? _session;
        private CancellationTokenSource? _cts;
        private int _running;

        public int Port { get; private set; }
        public bool IsRunning => Volatile.Read(ref _running) == 1;

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

        public RelayStreamHostTransport(RelayRoom room, TransportSettings settings) {
            _room = room;
            _settings = settings;
            _logger = settings.Logger;
            _onSessionClosed = OnSessionClosed;
            _onSessionRestored = OnSessionRestored;
        }

        /// <summary>Claims the room on the relay. The port argument is ignored; returns the relay's.</summary>
        public async Task<int> StartAsync(int port, CancellationToken ct = default) {
            if (Interlocked.Exchange(ref _running, 1) == 1) {
                throw new InvalidOperationException("Server transport already running");
            }
            try {
                _session = await _room.AcquireHostSessionAsync(_logger, ct).ConfigureAwait(false);
            }
            catch {
                _room.ReleaseHostSession();
                Volatile.Write(ref _running, 0);
                throw;
            }
            Attach(_session);
            _session.StartReceiving();
            _room.HostSessionRestored += _onSessionRestored;
            _cts = new CancellationTokenSource();
            var token = _cts.Token;
            _ = Task.Run(() => PingLoopAsync(token));
            Port = _session.RelayEndPoint.Port;
            return Port;
        }

        private void Attach(RelayHostSession session) {
            session.PeerOpened = OnPeerOpened;
            session.PeerData = OnPeerData;
            session.PeerClosed = id => RemovePeer(id, DisconnectReason.Remote, notifyRelay: false);
            session.OnClosed(_onSessionClosed);
        }

        private static void Detach(RelayHostSession session, Action onClosed) {
            session.PeerOpened = null;
            session.PeerData = null;
            session.PeerClosed = null;
            session.RemoveOnClosed(onClosed);
        }

        /// <summary>The room got back onto a restarted relay: serve the clients that arrive through it.</summary>
        private void OnSessionRestored(RelayHostSession session) {
            if (!IsRunning || ReferenceEquals(session, Volatile.Read(ref _session))) {
                return;
            }
            Volatile.Write(ref _session, session);
            Attach(session);
        }

        private TransportPeer GetOrCreatePeer(uint clientId, IPEndPoint? endPoint) {
            return _peers.TryGetValue(clientId, out var existing) ? existing : CreatePeer(clientId, endPoint);
        }

        /// <summary>Kept apart from the per-message lookup: its closure would be allocated on every call.</summary>
        private TransportPeer CreatePeer(uint clientId, IPEndPoint? endPoint) {
            var state = new PeerState(clientId, _session!, _settings);
            var peer = new TransportPeer(endPoint ?? _session!.RelayEndPoint, NetTime.NowMs) { TransportState = state };
            state.Deliver = (data, offset, count) => Deliver(peer, data, offset, count);
            if (!_peers.TryAdd(clientId, peer)) {
                state.Link.Dispose();
                return _peers[clientId];
            }
            state.Link.Start();
            PeerConnected?.Invoke(peer);
            return peer;
        }

        private void OnPeerOpened(uint clientId, IPEndPoint? endPoint) => GetOrCreatePeer(clientId, endPoint);

        private void OnPeerData(uint clientId, byte[] buffer, int offset, int length) {
            var peer = GetOrCreatePeer(clientId, null);
            if (RequireTrust && !peer.IsTrusted && length > 1 + PacketProtocol.MaxUntrustedFrameBody) {
                RemovePeer(clientId, DisconnectReason.Error, notifyRelay: true);
                return;
            }
            peer.Touch(NetTime.NowMs);
            var state = (PeerState)peer.TransportState!;
            if (!state.Link.HandleMessage(buffer, offset, length, state.Deliver)) {
                _logger.Warning($"Relay stream: protocol violation by {peer}; dropping it");
                RemovePeer(clientId, DisconnectReason.Error, notifyRelay: true);
            }
        }

        private void Deliver(TransportPeer peer, byte[] buffer, int offset, int length) {
            ReceivedPacket.Deliver(PooledReceive, DataReceived, peer, buffer, offset, length); // zero-copy: valid only during the call
        }

        private void OnSessionClosed() {
            _logger.Warning($"Relay room '{_room.RoomId}': lost the relay; its stream clients are gone");
            foreach (var id in _peers.Keys) {
                RemovePeer(id, DisconnectReason.Remote, notifyRelay: false);
            }
        }

        private async Task PingLoopAsync(CancellationToken ct) {
            try {
                while (!ct.IsCancellationRequested) {
                    long now = NetTime.NowMs;
                    foreach (var kvp in _peers) {
                        var peer = kvp.Value;
                        var state = (PeerState)peer.TransportState!;
                        if (!RequireTrust || peer.IsTrusted) {
                            state.Link.SendPing();
                            peer.SetPing(state.Link.PingMs);
                        }
                        if (_settings.PingTimeoutMs > 0 && now - peer.LastSeenMs > _settings.PingTimeoutMs) {
                            RemovePeer(kvp.Key, DisconnectReason.Timeout, notifyRelay: true);
                        }
                        else if (state.Link.CheckHealth(_settings, expectPongs: false) is { } problem) {
                            _logger.Warning($"Relay stream: {peer}: {problem}");
                            RemovePeer(kvp.Key, DisconnectReason.Timeout, notifyRelay: true);
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

        public void DisconnectPeer(TransportPeer peer) {
            if (peer.TransportState is PeerState state) {
                RemovePeer(state.ClientId, DisconnectReason.Local, notifyRelay: true);
            }
        }

        private void RemovePeer(uint clientId, DisconnectReason reason, bool notifyRelay) {
            if (!_peers.TryRemove(clientId, out var peer)) {
                return;
            }
            if (notifyRelay && _session != null) {
                var id = new byte[4];
                BinaryPrimitives.WriteUInt32BigEndian(id, clientId);
                // Same priority as PeerData: the close must not overtake the peer's last frames.
                _session.Connection.TrySend(RelayProtocol.PeerClose, id, ReadOnlySpan<byte>.Empty, SendPriority.Normal);
            }
            ((PeerState)peer.TransportState!).Link.Dispose();
            PeerDisconnected?.Invoke(peer, reason);
        }

        public Task StopAsync() {
            if (Interlocked.Exchange(ref _running, 0) == 0) {
                return Task.CompletedTask;
            }
            _cts?.Cancel();
            _room.HostSessionRestored -= _onSessionRestored;
            foreach (var id in _peers.Keys) {
                RemovePeer(id, DisconnectReason.Local, notifyRelay: true);
            }
            if (_session != null) {
                Detach(_session, _onSessionClosed);
            }
            _session = null;
            _room.ReleaseHostSession();
            Port = 0;
            return Task.CompletedTask;
        }

        public void Dispose() => StopAsync().GetAwaiter().GetResult();
    }
}
