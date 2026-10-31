using System;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Net;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using NetSync.Diagnostics;
using NetSync.Internal;
using NetSync.Transports;
using NetSync.Transports.Udp;

namespace NetSync.Relay {
    /// <summary>
    /// Client datagram slot through a relay, with UDP hole punching. Registers with the
    /// relay (which answers with a client id and the host's public endpoint), sends via the
    /// relay at once, and meanwhile punches towards the host. The first datagram straight
    /// from the host proves a direct path; from then on traffic bypasses the relay. If the
    /// direct path goes quiet, it falls back to the relay and punches again.
    ///
    /// Datagram payloads are NetSync UDP packets ([type][payload]), so ping/pong and data
    /// work exactly as on the plain UDP transport.
    /// </summary>
    internal sealed class RelayDatagramClientTransport : ITransport, IPooledReceiveTransport {
        private const int RegisterRetryMs = 200;
        private const int RegisterTimeoutMs = 5000;
        private const int PunchIntervalMs = 100;
        private const int PunchDurationMs = 3000;

        private sealed class Connection {
            public readonly IDatagramSocket Socket;
            public readonly IPEndPoint Relay;
            public readonly byte[] Nonce = new byte[RelayProtocol.NonceSize];
            public readonly CancellationTokenSource Cts = new CancellationTokenSource();
            public readonly TaskCompletionSource<bool> Registered = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            public readonly NetMetrics Metrics = new NetMetrics();
            public uint ClientId;
            public IPEndPoint? Host;          // the host's public endpoint, for punching
            public int Direct;                // 1 = a datagram came straight from the host
            public long LastDirectMs;
            public long LastPongAtMs = NetTime.NowMs;
            public int DisconnectSignaled;

            public Connection(IDatagramSocket socket, IPEndPoint relay) {
                Socket = socket;
                Relay = relay;
                RandomNumberGenerator.Fill(Nonce);
            }

            public bool IsDirect => Volatile.Read(ref Direct) == 1;
        }

        private readonly RelayRoom _room;
        private readonly TransportSettings _settings;
        private readonly INetLogger _logger;
        private Connection? _connection;
        private int _pingMs = -1;

        public bool IsConnected => _connection != null;
        public int PingMs => Volatile.Read(ref _pingMs);

        /// <summary>True once traffic flows directly between client and host (test/diagnostic).</summary>
        internal bool IsDirect => _connection?.IsDirect ?? false;

        public event Action? Connected;
        public event Action<DisconnectReason>? Disconnected;
        public event Action<byte[]>? DataReceived;

        internal Action<byte[], int, int>? PooledReceive { get; set; }

        Action<byte[], int, int>? IPooledReceiveTransport.PooledReceive {
            get => PooledReceive;
            set => PooledReceive = value;
        }

        public RelayDatagramClientTransport(RelayRoom room, TransportSettings settings) {
            _room = room;
            _settings = settings;
            _logger = settings.Logger;
        }

        /// <param name="endpoint">The relay server.</param>
        /// <param name="ct">Cancels registration.</param>
        public async Task ConnectAsync(IPEndPoint endpoint, CancellationToken ct = default) {
            Disconnect();
            var connection = new Connection(_room.SocketFactory(endpoint.AddressFamily), endpoint);
            var receiveThread = new Thread(() => ReceiveLoop(connection)) { IsBackground = true, Name = "NetSync relay UDP" };
            receiveThread.Start();

            var room = RelayProtocol.EncodeRoom(_room.RoomId);
            var register = new byte[1 + room.Length + RelayProtocol.NonceSize];
            register[0] = RelayProtocol.ClientRegister;
            room.CopyTo(register, 1);
            connection.Nonce.CopyTo(register, 1 + room.Length);
            long deadline = NetTime.NowMs + RegisterTimeoutMs;
            try {
                while (!connection.Registered.Task.IsCompleted) {
                    if (NetTime.NowMs > deadline) {
                        throw new TimeoutException($"Relay {endpoint} did not register us in room '{_room.RoomId}' (no such room, or its host has no UDP side)");
                    }
                    connection.Socket.SendTo(register, 0, register.Length, endpoint);
                    await Task.WhenAny(connection.Registered.Task, Task.Delay(RegisterRetryMs, ct)).ConfigureAwait(false);
                    ct.ThrowIfCancellationRequested();
                }
            }
            catch {
                connection.Cts.Cancel();
                connection.Socket.Dispose();
                throw;
            }

            _connection = connection;
            _pingMs = -1;
            if (_room.AllowDirect && connection.Host != null) {
                _ = Task.Run(() => PunchAsync(connection));
            }
            _ = Task.Run(() => PingLoopAsync(connection));
            Connected?.Invoke();
        }

        private async Task PunchAsync(Connection connection) {
            var punch = new byte[1 + 4 + RelayProtocol.NonceSize];
            punch[0] = RelayProtocol.Punch;
            BinaryPrimitives.WriteUInt32BigEndian(punch.AsSpan(1), connection.ClientId);
            connection.Nonce.CopyTo(punch, 5);
            long until = NetTime.NowMs + PunchDurationMs;
            try {
                while (!connection.IsDirect && NetTime.NowMs < until && !connection.Cts.IsCancellationRequested) {
                    connection.Socket.SendTo(punch, 0, punch.Length, connection.Host!);
                    await Task.Delay(PunchIntervalMs, connection.Cts.Token).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) { }
            catch (ObjectDisposedException) { }
            if (connection.IsDirect) {
                _logger.Info($"Relay room '{_room.RoomId}': direct UDP path to {connection.Host} established");
            }
        }

        public ValueTask SendAsync(ReadOnlyMemory<byte> data, CancellationToken ct = default) {
            return SendAsync(ReadOnlyMemory<byte>.Empty, data, ct);
        }

        public ValueTask SendAsync(ReadOnlyMemory<byte> prefix, ReadOnlyMemory<byte> payload, CancellationToken ct = default) {
            var connection = _connection ?? throw new InvalidOperationException("Not connected");
            SendPacket(connection, (byte)PacketType.Data, prefix.Span, payload.Span);
            connection.Metrics.IncrementPacketsSent();
            return default;
        }

        /// <summary>[DirectData|RelayData][clientId][nonce][type][prefix][payload] — direct when proven, else via the relay.</summary>
        private static void SendPacket(Connection connection, byte type, ReadOnlySpan<byte> prefix, ReadOnlySpan<byte> payload) {
            const int header = 1 + 4 + RelayProtocol.NonceSize;
            int length = header + 1 + prefix.Length + payload.Length;
            var datagram = Pooled.Rent(length);
            try {
                bool direct = connection.IsDirect;
                datagram[0] = direct ? RelayProtocol.DirectData : RelayProtocol.RelayData;
                BinaryPrimitives.WriteUInt32BigEndian(datagram.AsSpan(1), connection.ClientId);
                connection.Nonce.CopyTo(datagram, 5);
                datagram[header] = type;
                prefix.CopyTo(datagram.AsSpan(header + 1));
                payload.CopyTo(datagram.AsSpan(header + 1 + prefix.Length));
                connection.Socket.SendTo(datagram, 0, length, direct ? connection.Host! : connection.Relay);
            }
            catch (ObjectDisposedException) {
                throw new InvalidOperationException("Not connected");
            }
            finally {
                Pooled.Return(datagram);
            }
        }

        private void ReceiveLoop(Connection connection) {
            var buffer = new byte[UdpSockets.ReceiveBufferSize];
            var reason = DisconnectReason.Local;
            try {
                while (!connection.Cts.IsCancellationRequested) {
                    int received = connection.Socket.ReceiveFrom(buffer, out var source);
                    if (received < 1) {
                        continue;
                    }
                    if (source.Equals(connection.Relay)) {
                        HandleFromRelay(connection, buffer, received);
                    }
                    else if (connection.Host != null && DualStack.Normalize(source).Equals(connection.Host)) {
                        HandleFromHost(connection, buffer, received);
                    }
                    // anything else: not ours
                }
            }
            catch (Exception) when (connection.Cts.IsCancellationRequested) {
                // closing
            }
            catch (Exception ex) {
                _logger.Error($"Relay UDP receive loop: {ex.Message}");
                reason = DisconnectReason.Error;
            }
            finally {
                CloseConnection(connection, reason);
            }
        }

        private void HandleFromRelay(Connection connection, byte[] buffer, int received) {
            switch (buffer[0]) {
                case RelayProtocol.ClientRegistered:
                    if (received >= 5 && !connection.Registered.Task.IsCompleted) {
                        connection.ClientId = BinaryPrimitives.ReadUInt32BigEndian(buffer.AsSpan(1));
                        RelayProtocol.TryReadEndPoint(buffer.AsSpan(5, received - 5), out var host, out _);
                        connection.Host = host;
                        connection.Registered.TrySetResult(true);
                    }
                    break;
                case RelayProtocol.RelayData:
                    HandlePacket(connection, buffer, 1, received - 1);
                    break;
            }
        }

        private void HandleFromHost(Connection connection, byte[] buffer, int received) {
            switch (buffer[0]) {
                case RelayProtocol.Punch:
                    MarkDirect(connection);
                    break;
                case RelayProtocol.DirectData:
                    MarkDirect(connection);
                    HandlePacket(connection, buffer, 1, received - 1);
                    break;
            }
        }

        private static void MarkDirect(Connection connection) {
            Volatile.Write(ref connection.LastDirectMs, NetTime.NowMs);
            Volatile.Write(ref connection.Direct, 1);
        }

        /// <summary>A NetSync UDP packet, the same as the plain UDP transport handles.</summary>
        private void HandlePacket(Connection connection, byte[] buffer, int offset, int count) {
            if (count < 1) {
                return;
            }
            switch ((PacketType)buffer[offset]) {
                case PacketType.Ping:
                    if (count >= PacketProtocol.PingPacketSize) {
                        Span<byte> timestamp = stackalloc byte[8];
                        buffer.AsSpan(offset + 1, 8).CopyTo(timestamp);
                        SendPacket(connection, (byte)PacketType.Pong, ReadOnlySpan<byte>.Empty, timestamp);
                    }
                    break;
                case PacketType.Pong:
                    if (count >= PacketProtocol.PingPacketSize) {
                        long sentAt = BinaryPrimitives.ReadInt64BigEndian(buffer.AsSpan(offset + 1, 8));
                        Volatile.Write(ref _pingMs, (int)Math.Max(0, NetTime.NowMs - sentAt));
                        Volatile.Write(ref connection.LastPongAtMs, NetTime.NowMs);
                    }
                    break;
                case PacketType.Data:
                    connection.Metrics.IncrementPacketsReceived();
                    ReceivedPacket.Deliver(PooledReceive, DataReceived, buffer, offset + 1, count - 1); // zero-copy: valid only during the call
                    break;
            }
        }

        private async Task PingLoopAsync(Connection connection) {
            var ct = connection.Cts.Token;
            try {
                while (!ct.IsCancellationRequested) {
                    SendPing(connection);
                    long now = NetTime.NowMs;
                    if (_settings.PingTimeoutMs > 0 && now - Volatile.Read(ref connection.LastPongAtMs) > _settings.PingTimeoutMs) {
                        connection.Metrics.IncrementPingTimeouts();
                        CloseConnection(connection, DisconnectReason.Timeout);
                        return;
                    }
                    // A direct path that went quiet (NAT mapping expired): back to the relay, punch again.
                    if (connection.IsDirect && now - Volatile.Read(ref connection.LastDirectMs) > 3L * _settings.PingIntervalMs + 500) {
                        Volatile.Write(ref connection.Direct, 0);
                        _logger.Info($"Relay room '{_room.RoomId}': direct path quiet, back to the relay");
                        _ = Task.Run(() => PunchAsync(connection));
                    }
                    await Task.Delay(_settings.PingIntervalMs, ct).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) { }
            catch (InvalidOperationException) { }
        }

        private static void SendPing(Connection connection) {
            var timestamp = new byte[8];
            BinaryPrimitives.WriteInt64BigEndian(timestamp, NetTime.NowMs);
            SendPacket(connection, (byte)PacketType.Ping, ReadOnlySpan<byte>.Empty, timestamp);
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
            connection.Socket.Dispose(); // unblocks the receive thread
            Disconnected?.Invoke(reason);
        }

        public void Dispose() => Disconnect();
    }

    /// <summary>
    /// Host datagram slot through a relay: one socket registered with the relay (repeatedly —
    /// that keeps the host's NAT mapping open), relayed client traffic arriving tagged with a
    /// client id, and direct paths learned from punches carrying the client's nonce.
    /// </summary>
    internal sealed class RelayDatagramHostTransport : IServerTransport, IPooledReceiveServerTransport, ITrustAwareServerTransport {
        private const int RegisterIntervalMs = 5000;
        private const int PunchCount = 15;
        private const int PunchIntervalMs = 100;

        private sealed class ClientRecord {
            public readonly uint Id;
            public byte[]? Nonce;
            public IPEndPoint? PublicEndPoint;
            public IPEndPoint? Direct;
            public long LastDirectMs;
            public TransportPeer? Peer;

            public ClientRecord(uint id) => Id = id;
        }

        private readonly RelayRoom _room;
        private readonly TransportSettings _settings;
        private readonly INetLogger _logger;
        private readonly ConcurrentDictionary<uint, ClientRecord> _clients = new ConcurrentDictionary<uint, ClientRecord>();
        private readonly TaskCompletionSource<bool> _registered = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly Action<RelayHostSession> _onSessionRestored;
        private IDatagramSocket? _socket;
        private IPEndPoint? _relay;
        private RelayHostSession? _session;
        private byte[] _register = Array.Empty<byte>(); // HostRegister for the current session
        private bool _sessionAcquired;
        private CancellationTokenSource? _cts;
        private int _running;
        private int _untrusted;

        public int Port { get; private set; }
        public bool IsRunning => Volatile.Read(ref _running) == 1;

        public event Action<TransportPeer>? PeerConnected;
        public event Action<TransportPeer, DisconnectReason>? PeerDisconnected;
        public event Action<TransportPeer, byte[]>? DataReceived;

        internal Action<TransportPeer, byte[], int, int>? PooledReceive { get; set; }
        internal bool RequireTrust { get; set; }

        /// <summary>Test hook: ignore punches (simulates a NAT that never lets them through).</summary>
        internal bool IgnorePunches { get; set; }

        Action<TransportPeer, byte[], int, int>? IPooledReceiveServerTransport.PooledReceive {
            get => PooledReceive;
            set => PooledReceive = value;
        }

        bool ITrustAwareServerTransport.RequireTrust {
            get => RequireTrust;
            set => RequireTrust = value;
        }

        void ITrustAwareServerTransport.MarkTrusted(TransportPeer peer) {
            if (!peer.IsTrusted) {
                peer.MarkTrusted();
                Interlocked.Decrement(ref _untrusted);
            }
        }

        public RelayDatagramHostTransport(RelayRoom room, TransportSettings settings) {
            _room = room;
            _settings = settings;
            _logger = settings.Logger;
            _onSessionRestored = OnSessionRestored;
        }

        /// <summary>Registers the room's UDP side with the relay. The port argument is ignored; returns the relay's.</summary>
        public async Task<int> StartAsync(int port, CancellationToken ct = default) {
            if (Interlocked.Exchange(ref _running, 1) == 1) {
                throw new InvalidOperationException("Server transport already running");
            }
            try {
                var sessionTask = _room.AcquireHostSessionAsync(_logger, ct);
                _sessionAcquired = true; // counted even if the connect fails: StopAsync releases it
                _session = await sessionTask.ConfigureAwait(false);
                _session.StartReceiving();
                _relay = _session.RelayEndPoint;
                _socket = _room.SocketFactory(_relay.AddressFamily);
                _cts = new CancellationTokenSource();
                var socket = _socket;
                var token = _cts.Token;
                new Thread(() => ReceiveLoop(socket, token)) { IsBackground = true, Name = "NetSync relay host UDP" }.Start();

                var register = BuildRegister(_session);
                Volatile.Write(ref _register, register);
                long deadline = NetTime.NowMs + 5000;
                while (!_registered.Task.IsCompleted) {
                    if (NetTime.NowMs > deadline) {
                        throw new TimeoutException($"Relay {_relay} did not confirm the UDP side of room '{_room.RoomId}'");
                    }
                    socket.SendTo(register, 0, register.Length, _relay);
                    await Task.WhenAny(_registered.Task, Task.Delay(200, ct)).ConfigureAwait(false);
                    ct.ThrowIfCancellationRequested();
                }
                _room.HostSessionRestored += _onSessionRestored;
                _ = Task.Run(() => MaintenanceLoopAsync(socket, token));
                Port = _relay.Port;
                return Port;
            }
            catch {
                await StopAsync().ConfigureAwait(false);
                throw;
            }
        }

        private byte[] BuildRegister(RelayHostSession session) {
            var room = RelayProtocol.EncodeRoom(_room.RoomId);
            var register = new byte[1 + room.Length + RelayProtocol.SecretSize];
            register[0] = RelayProtocol.HostRegister;
            room.CopyTo(register, 1);
            session.Secret.CopyTo(register, 1 + room.Length);
            return register;
        }

        /// <summary>
        /// The room got back onto a restarted relay: register the UDP side under the new secret.
        /// Client ids restart on a new relay, so every old client record is void.
        /// </summary>
        private void OnSessionRestored(RelayHostSession session) {
            var socket = _socket;
            if (!IsRunning || socket == null || ReferenceEquals(session, Volatile.Read(ref _session))) {
                return;
            }
            Volatile.Write(ref _session, session);
            foreach (var record in _clients.Values) {
                RemoveClient(record, DisconnectReason.Remote);
            }
            var register = BuildRegister(session);
            Volatile.Write(ref _register, register);
            try {
                socket.SendTo(register, 0, register.Length, _relay!); // the maintenance loop repeats it
            }
            catch (ObjectDisposedException) { }
        }

        private void ReceiveLoop(IDatagramSocket socket, CancellationToken ct) {
            var buffer = new byte[UdpSockets.ReceiveBufferSize];
            try {
                while (!ct.IsCancellationRequested) {
                    int received = socket.ReceiveFrom(buffer, out var source);
                    if (received < 1) {
                        continue;
                    }
                    try {
                        if (source.Equals(_relay)) {
                            HandleFromRelay(buffer, received);
                        }
                        else {
                            HandleDirect(socket, buffer, received, DualStack.Normalize(source));
                        }
                    }
                    catch (Exception ex) when (!(ex is ObjectDisposedException)) {
                        _logger.Error($"Relay host UDP: {ex}");
                    }
                }
            }
            catch (Exception) {
                // socket closed
            }
        }

        private void HandleFromRelay(byte[] buffer, int received) {
            switch (buffer[0]) {
                case RelayProtocol.HostRegistered:
                    _registered.TrySetResult(true);
                    break;

                case RelayProtocol.Introduce:
                    if (received >= 1 + 4 + RelayProtocol.NonceSize + 1) {
                        OnIntroduce(buffer, received);
                    }
                    break;

                case RelayProtocol.RelayData: {
                    if (received < 1 + 4 + 1) {
                        return;
                    }
                    var record = _clients.GetOrAdd(BinaryPrimitives.ReadUInt32BigEndian(buffer.AsSpan(1)), key => new ClientRecord(key));
                    HandlePacket(record, buffer, 5, received - 5);
                    break;
                }
            }
        }

        /// <summary>A client registered with the relay: remember its nonce, punch towards it.</summary>
        private void OnIntroduce(byte[] buffer, int received) {
            uint id = BinaryPrimitives.ReadUInt32BigEndian(buffer.AsSpan(1));
            var record = _clients.GetOrAdd(id, key => new ClientRecord(key));
            record.Nonce = buffer.AsSpan(5, RelayProtocol.NonceSize).ToArray();
            if (RelayProtocol.TryReadEndPoint(buffer.AsSpan(5 + RelayProtocol.NonceSize, received - 5 - RelayProtocol.NonceSize), out var endPoint, out _)) {
                record.PublicEndPoint = endPoint;
            }
            if (_room.AllowDirect && record.PublicEndPoint != null) {
                _ = Task.Run(() => PunchAsync(record));
            }
        }

        /// <summary>Straight from a client: a punch or data, proven by the nonce the relay introduced.</summary>
        private void HandleDirect(IDatagramSocket socket, byte[] buffer, int received, IPEndPoint source) {
            if (IgnorePunches || received < 1 + 4 + RelayProtocol.NonceSize ||
                (buffer[0] != RelayProtocol.Punch && buffer[0] != RelayProtocol.DirectData)) {
                return;
            }
            if (!_clients.TryGetValue(BinaryPrimitives.ReadUInt32BigEndian(buffer.AsSpan(1)), out var record) || record.Nonce == null ||
                !Pipeline.Security.ConnectionCrypto.FixedTimeEquals(buffer.AsSpan(5, RelayProtocol.NonceSize), record.Nonce)) {
                return;
            }
            bool firstContact = record.Direct == null || !record.Direct.Equals(source);
            record.Direct = source; // may differ from what the relay saw: a peer-reflexive endpoint
            Volatile.Write(ref record.LastDirectMs, NetTime.NowMs);
            if (buffer[0] == RelayProtocol.Punch) {
                if (firstContact) {
                    // Answer so the client learns the path works both ways.
                    var punch = new byte[1 + 4 + RelayProtocol.NonceSize];
                    Buffer.BlockCopy(buffer, 0, punch, 0, punch.Length);
                    socket.SendTo(punch, 0, punch.Length, source);
                }
                return;
            }
            HandlePacket(record, buffer, 1 + 4 + RelayProtocol.NonceSize, received - 1 - 4 - RelayProtocol.NonceSize);
        }

        private async Task PunchAsync(ClientRecord record) {
            var socket = _socket;
            var nonce = record.Nonce;
            var target = record.PublicEndPoint;
            if (socket == null || nonce == null || target == null) {
                return;
            }
            var punch = new byte[1 + 4 + RelayProtocol.NonceSize];
            punch[0] = RelayProtocol.Punch;
            BinaryPrimitives.WriteUInt32BigEndian(punch.AsSpan(1), record.Id);
            nonce.CopyTo(punch, 5);
            try {
                // Opens our NAT towards the client, so its punches (and data) get in.
                for (int i = 0; i < PunchCount && record.Direct == null && IsRunning; i++) {
                    socket.SendTo(punch, 0, punch.Length, target);
                    await Task.Delay(PunchIntervalMs).ConfigureAwait(false);
                }
            }
            catch (ObjectDisposedException) { }
        }

        /// <summary>A NetSync UDP packet from a client (either path).</summary>
        private void HandlePacket(ClientRecord record, byte[] buffer, int offset, int count) {
            if (count < 1) {
                return;
            }
            var type = (PacketType)buffer[offset];
            var peer = record.Peer;
            if (peer == null) {
                // Only data opens a peer (as on the plain UDP server), capped while untrusted.
                if (type != PacketType.Data || (RequireTrust && Volatile.Read(ref _untrusted) >= UdpServerTransport.MaxUntrustedPeers)) {
                    return;
                }
                peer = new TransportPeer(record.PublicEndPoint ?? _relay!, NetTime.NowMs) { TransportState = record };
                record.Peer = peer;
                Interlocked.Increment(ref _untrusted);
                PeerConnected?.Invoke(peer);
            }
            peer.Touch(NetTime.NowMs);

            switch (type) {
                case PacketType.Ping:
                    if (count >= PacketProtocol.PingPacketSize) {
                        var pong = PacketProtocol.RentPongPacket(PacketProtocol.ReadTimestamp(buffer.AsSpan(offset)));
                        try {
                            SendRaw(record, pong, PacketProtocol.PingPacketSize);
                        }
                        finally {
                            PacketProtocol.Return(pong);
                        }
                    }
                    break;
                case PacketType.Pong:
                    if (count >= PacketProtocol.PingPacketSize) {
                        peer.SetPing((int)Math.Max(0, NetTime.NowMs - PacketProtocol.ReadTimestamp(buffer.AsSpan(offset))));
                    }
                    break;
                case PacketType.Data:
                    if (RequireTrust && !peer.IsTrusted && count - 1 > PacketProtocol.MaxUntrustedFrameBody) {
                        return;
                    }
                    ReceivedPacket.Deliver(PooledReceive, DataReceived, peer, buffer, offset + 1, count - 1); // zero-copy: valid only during the call
                    break;
            }
        }

        /// <summary>Sends a NetSync UDP packet to a client: directly when that path is alive, else via the relay.</summary>
        private void SendRaw(ClientRecord record, byte[] packet, int length) {
            var socket = _socket ?? throw new InvalidOperationException("Server transport not started");
            var direct = record.Direct;
            var datagram = Pooled.Rent(5 + length);
            try {
                if (direct != null) {
                    datagram[0] = RelayProtocol.DirectData;
                    Buffer.BlockCopy(packet, 0, datagram, 1, length);
                    socket.SendTo(datagram, 0, 1 + length, direct);
                }
                else {
                    datagram[0] = RelayProtocol.RelayData;
                    BinaryPrimitives.WriteUInt32BigEndian(datagram.AsSpan(1), record.Id);
                    Buffer.BlockCopy(packet, 0, datagram, 5, length);
                    socket.SendTo(datagram, 0, 5 + length, _relay!);
                }
            }
            catch (ObjectDisposedException) {
                throw new InvalidOperationException("Server transport stopped");
            }
            finally {
                Pooled.Return(datagram);
            }
        }

        private async Task MaintenanceLoopAsync(IDatagramSocket socket, CancellationToken ct) {
            long lastRegister = NetTime.NowMs;
            try {
                while (!ct.IsCancellationRequested) {
                    long now = NetTime.NowMs;
                    if (now - lastRegister >= RegisterIntervalMs) {
                        // Keeps our NAT mapping to the relay open — and re-registers us if the relay restarted.
                        var register = Volatile.Read(ref _register);
                        socket.SendTo(register, 0, register.Length, _relay!);
                        lastRegister = now;
                    }
                    foreach (var kvp in _clients) {
                        var record = kvp.Value;
                        var peer = record.Peer;
                        if (record.Direct != null && now - Volatile.Read(ref record.LastDirectMs) > 3L * _settings.PingIntervalMs + 500) {
                            record.Direct = null; // the direct path went quiet: relay again
                        }
                        if (peer == null) {
                            continue;
                        }
                        if (!RequireTrust || peer.IsTrusted) {
                            var ping = PacketProtocol.RentPingPacket(now);
                            try {
                                SendRaw(record, ping, PacketProtocol.PingPacketSize);
                            }
                            finally {
                                PacketProtocol.Return(ping);
                            }
                        }
                        if (_settings.PingTimeoutMs > 0 && now - peer.LastSeenMs > _settings.PingTimeoutMs) {
                            RemoveClient(record, DisconnectReason.Timeout);
                        }
                    }
                    await Task.Delay(_settings.PingIntervalMs, ct).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) { }
            catch (InvalidOperationException) { } // includes ObjectDisposedException: stopped
        }

        public ValueTask SendAsync(TransportPeer peer, ReadOnlyMemory<byte> data, CancellationToken ct = default) {
            return SendAsync(peer, ReadOnlyMemory<byte>.Empty, data, ct);
        }

        public ValueTask SendAsync(TransportPeer peer, ReadOnlyMemory<byte> prefix, ReadOnlyMemory<byte> payload, CancellationToken ct = default) {
            if (peer.TransportState is not ClientRecord record) {
                throw new ArgumentException("Peer does not belong to this transport", nameof(peer));
            }
            var packet = PacketProtocol.RentUdpDataPacket(prefix.Span, payload.Span, out int length);
            try {
                SendRaw(record, packet, length);
            }
            finally {
                PacketProtocol.Return(packet);
            }
            return default;
        }

        public void DisconnectPeer(TransportPeer peer) {
            if (peer.TransportState is ClientRecord record) {
                RemoveClient(record, DisconnectReason.Local);
            }
        }

        private void RemoveClient(ClientRecord record, DisconnectReason reason) {
            if (!_clients.TryRemove(record.Id, out _)) {
                return;
            }
            var peer = record.Peer;
            if (peer != null) {
                if (!peer.IsTrusted) {
                    Interlocked.Decrement(ref _untrusted);
                }
                PeerDisconnected?.Invoke(peer, reason);
            }
        }

        public Task StopAsync() {
            if (Interlocked.Exchange(ref _running, 0) == 0) {
                return Task.CompletedTask;
            }
            _cts?.Cancel();
            _room.HostSessionRestored -= _onSessionRestored;
            foreach (var record in _clients.Values) {
                RemoveClient(record, DisconnectReason.Local);
            }
            _socket?.Dispose();
            _socket = null;
            _session = null;
            if (_sessionAcquired) {
                _sessionAcquired = false;
                _room.ReleaseHostSession();
            }
            Port = 0;
            return Task.CompletedTask;
        }

        public void Dispose() => StopAsync().GetAwaiter().GetResult();
    }
}
