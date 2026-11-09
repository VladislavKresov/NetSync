using System;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using NetSync.Diagnostics;
using NetSync.Internal;
using NetSync.Sending;
using NetSync.Transports;
using NetSync.Transports.Udp;

namespace NetSync.Relay {
    /// <summary>
    /// Relay (and hole-punching rendezvous) for games and apps hosted behind NAT. A host
    /// claims a room; clients join it by the room code; the relay forwards their packets.
    /// For UDP it also introduces client and host to each other so they can try a direct
    /// path — when both NATs allow it, the relay drops out of the data path entirely.
    ///
    /// The relay is deliberately dumb: it forwards NetSync packets without understanding
    /// them. On connections with a key exchange those packets are authenticated (and
    /// optionally encrypted) end to end, so a relay can neither read nor forge traffic —
    /// run it anywhere, trust it with nothing but availability.
    ///
    /// Runs TCP and UDP on one port. Host it as a small service:
    /// <code>using var relay = new NetRelayServer(); await relay.StartAsync(7000);</code>
    /// </summary>
    public sealed class NetRelayServer : IDisposable {
        private sealed class Room {
            public readonly string Id;
            public readonly FramedConnection Host;
            public readonly byte[] Secret;
            public readonly ConcurrentDictionary<uint, FramedConnection> StreamClients = new ConcurrentDictionary<uint, FramedConnection>();
            public readonly ConcurrentDictionary<uint, UdpClientRecord> UdpClients = new ConcurrentDictionary<uint, UdpClientRecord>();
            public IPEndPoint? HostUdp;
            private int _nextClientId;

            public Room(string id, FramedConnection host, byte[] secret) {
                Id = id;
                Host = host;
                Secret = secret;
            }

            public uint NextClientId() => unchecked((uint)Interlocked.Increment(ref _nextClientId));
        }

        private sealed class UdpClientRecord {
            public readonly Room Room;
            public readonly uint Id;
            public readonly IPEndPoint EndPoint;
            public readonly byte[] Nonce;
            public long LastSeenMs = NetTime.NowMs;

            public UdpClientRecord(Room room, uint id, IPEndPoint endPoint, byte[] nonce) {
                Room = room;
                Id = id;
                EndPoint = endPoint;
                Nonce = nonce;
            }
        }

        private readonly INetLogger _logger;
        private readonly ConcurrentDictionary<string, Room> _rooms = new ConcurrentDictionary<string, Room>(StringComparer.Ordinal);
        private readonly ConcurrentDictionary<IPEndPoint, Room> _roomsByHostUdp = new ConcurrentDictionary<IPEndPoint, Room>();
        private readonly ConcurrentDictionary<IPEndPoint, UdpClientRecord> _udpClients = new ConcurrentDictionary<IPEndPoint, UdpClientRecord>();
        private TcpListener? _listener;
        private Socket? _udp;
#if NET8_0_OR_GREATER
        private SocketAddressCache? _targets;
#endif
        private CancellationTokenSource? _cts;
        private long _relayedUdpBytes;
        private long _relayedTcpBytes;

        /// <summary>Most rooms at once.</summary>
        public int MaxRooms { get; set; } = 10_000;

        /// <summary>Most clients per room (stream and datagram sides counted separately).</summary>
        public int MaxClientsPerRoom { get; set; } = 1024;

        /// <summary>Whether to introduce UDP clients and hosts for a direct (hole-punched) path.</summary>
        public bool AllowDirect { get; set; } = true;

        public int Port { get; private set; }
        public int RoomCount => _rooms.Count;

        /// <summary>UDP payload bytes forwarded (direct paths don't count — the point of punching).</summary>
        public long RelayedUdpBytes => Interlocked.Read(ref _relayedUdpBytes);

        /// <summary>TCP payload bytes forwarded.</summary>
        public long RelayedTcpBytes => Interlocked.Read(ref _relayedTcpBytes);

        public NetRelayServer(INetLogger? logger = null) {
            _logger = logger ?? NullNetLogger.Instance;
        }

        /// <summary>Listens for TCP and UDP on <paramref name="port"/> (0 = any free port); returns it.</summary>
        public Task<int> StartAsync(int port, CancellationToken ct = default) {
            if (_cts != null) {
                throw new InvalidOperationException("Relay already running");
            }
            for (int attempt = 0; ; attempt++) {
                _listener = DualStack.StartTcpListener(port);
                Port = ((IPEndPoint)_listener.LocalEndpoint).Port;
                try {
                    _udp = UdpSockets.BindServer(Port);
                    break;
                }
                catch (SocketException) when (port == 0 && attempt < 20) {
                    _listener.Stop(); // the OS picked a TCP port that is busy for UDP: pick again
                }
                catch {
                    _listener.Stop();
                    throw;
                }
            }
            _cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            var token = _cts.Token;
            var listener = _listener;
            _ = Task.Run(() => TcpAcceptLoop.RunAsync(listener, client => ServeAsync(client, token), _logger, nameof(NetRelayServer), token));
            var udp = _udp;
            new Thread(() => UdpLoop(udp, token)) { IsBackground = true, Name = "NetSync relay UDP" }.Start();
            _ = Task.Run(() => MaintenanceLoopAsync(token));
            _logger.Info($"NetRelayServer listening on port {Port}");
            return Task.FromResult(Port);
        }

        // ------------------------------------------------------------------- TCP

        private async Task ServeAsync(TcpClient client, CancellationToken ct) {
            var endPoint = DualStack.Normalize((IPEndPoint)client.Client.RemoteEndPoint!);
            var connection = new FramedConnection(client, _logger);
            try {
                (bool ok, byte kind, byte[] body) hello;
                using (var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct)) {
                    timeout.CancelAfter(10_000);
                    using var abort = timeout.Token.Register(connection.Dispose); // a silent client can't hold a socket
                    hello = await connection.ReadOneAsync(1 + RelayProtocol.MaxRoomBytes, timeout.Token).ConfigureAwait(false);
                }
                if (!hello.ok || !RelayProtocol.TryReadRoom(hello.body, out var roomId, out _)) {
                    return;
                }
                if (hello.kind == RelayProtocol.HostHello) {
                    await ServeHostAsync(connection, roomId, endPoint, ct).ConfigureAwait(false);
                }
                else if (hello.kind == RelayProtocol.ClientHello) {
                    await ServeStreamClientAsync(connection, roomId, endPoint, ct).ConfigureAwait(false);
                }
            }
            catch (Exception ex) when (!(ex is OutOfMemoryException)) {
                _logger.Debug($"Relay connection {endPoint}: {ex.Message}");
            }
            finally {
                connection.Dispose();
            }
        }

        private async Task ServeHostAsync(FramedConnection host, string roomId, IPEndPoint endPoint, CancellationToken ct) {
            if (_rooms.Count >= MaxRooms) {
                await RefuseAsync(host, "relay full").ConfigureAwait(false);
                return;
            }
            var secret = new byte[RelayProtocol.SecretSize];
            RandomNumberGenerator.Fill(secret);
            var room = new Room(roomId, host, secret);
            if (!_rooms.TryAdd(roomId, room)) {
                await RefuseAsync(host, $"room '{roomId}' is taken").ConfigureAwait(false);
                return;
            }
            _logger.Info($"Relay: {endPoint} hosts room '{roomId}'");
            host.TrySend(RelayProtocol.HostWelcome, secret, ReadOnlySpan<byte>.Empty, SendPriority.High);
            try {
                await host.RunReceiveLoopAsync((kind, buffer, offset, length) => {
                    switch (kind) {
                        case RelayProtocol.PeerData:
                            if (length >= 4 && room.StreamClients.TryGetValue(BinaryPrimitives.ReadUInt32BigEndian(buffer.AsSpan(offset)), out var target)) {
                                Interlocked.Add(ref _relayedTcpBytes, length - 4);
                                if (!target.TrySend(RelayProtocol.Data, ReadOnlySpan<byte>.Empty, buffer.AsSpan(offset + 4, length - 4))) {
                                    target.Dispose(); // that client can't keep up: drop it, not the host
                                }
                            }
                            return true;
                        case RelayProtocol.PeerClose:
                            if (length >= 4 && room.StreamClients.TryRemove(BinaryPrimitives.ReadUInt32BigEndian(buffer.AsSpan(offset)), out var kicked)) {
                                _ = kicked.DisposeAfterFlushAsync(CloseFlushTimeoutMs); // the host's last frames, its Bye among them
                            }
                            return true;
                        case RelayProtocol.Keepalive:
                            return true;
                        default:
                            return false;
                    }
                }, ct).ConfigureAwait(false);
            }
            finally {
                _rooms.TryRemove(roomId, out _);
                if (room.HostUdp != null) {
                    _roomsByHostUdp.TryRemove(room.HostUdp, out _);
                }
                foreach (var client in room.StreamClients.Values) {
                    client.Dispose();
                }
                foreach (var record in room.UdpClients.Values) {
                    _udpClients.TryRemove(record.EndPoint, out _);
                }
                _logger.Info($"Relay: room '{roomId}' closed");
            }
        }

        private async Task ServeStreamClientAsync(FramedConnection client, string roomId, IPEndPoint endPoint, CancellationToken ct) {
            if (!_rooms.TryGetValue(roomId, out var room)) {
                await RefuseAsync(client, $"no room '{roomId}'").ConfigureAwait(false);
                return;
            }
            if (room.StreamClients.Count >= MaxClientsPerRoom) {
                await RefuseAsync(client, "room full").ConfigureAwait(false);
                return;
            }
            uint id = room.NextClientId();
            room.StreamClients[id] = client;
            var idBytes = new byte[4];
            BinaryPrimitives.WriteUInt32BigEndian(idBytes, id);
            var open = new byte[4 + RelayProtocol.EndPointSize(endPoint)];
            idBytes.CopyTo(open, 0);
            RelayProtocol.WriteEndPoint(open.AsSpan(4), endPoint);
            room.Host.TrySend(RelayProtocol.PeerOpen, open, ReadOnlySpan<byte>.Empty, SendPriority.High);
            client.TrySend(RelayProtocol.ClientWelcome, ReadOnlySpan<byte>.Empty, ReadOnlySpan<byte>.Empty, SendPriority.High);
            try {
                await client.RunReceiveLoopAsync((kind, buffer, offset, length) => {
                    switch (kind) {
                        case RelayProtocol.Data:
                            Interlocked.Add(ref _relayedTcpBytes, length);
                            if (!room.Host.TrySend(RelayProtocol.PeerData, idBytes, buffer.AsSpan(offset, length))) {
                                return false; // the host's queue is full: this client is flooding it
                            }
                            return true;
                        case RelayProtocol.Keepalive:
                            return true;
                        default:
                            return false;
                    }
                }, ct).ConfigureAwait(false);
            }
            finally {
                if (room.StreamClients.TryRemove(id, out _)) {
                    room.Host.TrySend(RelayProtocol.PeerClose, idBytes, ReadOnlySpan<byte>.Empty, SendPriority.High);
                }
            }
        }

        private const int CloseFlushTimeoutMs = 1000;

        private static async Task RefuseAsync(FramedConnection connection, string reason) {
            connection.TrySend(RelayProtocol.Refused, System.Text.Encoding.UTF8.GetBytes(reason), ReadOnlySpan<byte>.Empty, SendPriority.High);
            await Task.Delay(200).ConfigureAwait(false); // let it leave before the socket closes
        }

        // ------------------------------------------------------------------- UDP

        private void UdpLoop(Socket socket, CancellationToken ct) {
            var buffer = new byte[UdpSockets.ReceiveBufferSize];
#if NET8_0_OR_GREATER
            // Forwarding allocates nothing per datagram: sources resolve to cached endpoints.
            var sources = new EndPointCache(socket.AddressFamily, capacity: 1 << 16);
            _targets = new SocketAddressCache(socket.AddressFamily, capacity: 1 << 16);
#else
            EndPoint any = new IPEndPoint(socket.AddressFamily == AddressFamily.InterNetworkV6 ? IPAddress.IPv6Any : IPAddress.Any, 0);
#endif
            while (!ct.IsCancellationRequested) {
                int received;
                IPEndPoint from;
                try {
#if NET8_0_OR_GREATER
                    received = socket.ReceiveFrom(buffer.AsSpan(), SocketFlags.None, sources.Address);
                    from = sources.Resolve();
#else
                    EndPoint source = any;
                    received = socket.ReceiveFrom(buffer, 0, buffer.Length, SocketFlags.None, ref source);
                    from = (IPEndPoint)source;
#endif
                }
                catch (SocketException ex) when (!ct.IsCancellationRequested && UdpSockets.IsTransient(ex.SocketErrorCode)) {
                    continue;
                }
                catch (Exception) {
                    return; // socket closed
                }
                if (received == 0) {
                    continue;
                }
                try {
                    HandleDatagram(socket, buffer, received, from);
                }
                catch (Exception ex) {
                    _logger.Error($"Relay UDP: {ex}");
                }
            }
        }

        private void HandleDatagram(Socket socket, byte[] buffer, int received, IPEndPoint source) {
            var span = new ReadOnlySpan<byte>(buffer, 1, received - 1);
            switch (buffer[0]) {
                case RelayProtocol.HostRegister: {
                    if (!RelayProtocol.TryReadRoom(span, out var roomId, out int used) || span.Length < used + RelayProtocol.SecretSize ||
                        !_rooms.TryGetValue(roomId, out var room) ||
                        !Pipeline.Security.ConnectionCrypto.FixedTimeEquals(span.Slice(used, RelayProtocol.SecretSize), room.Secret)) {
                        return; // only the TCP-registered host knows the secret
                    }
                    if (room.HostUdp == null || !room.HostUdp.Equals(source)) {
                        if (room.HostUdp != null) {
                            _roomsByHostUdp.TryRemove(room.HostUdp, out _);
                        }
                        room.HostUdp = source;
                        _roomsByHostUdp[source] = room;
                    }
                    Send(socket, new[] { RelayProtocol.HostRegistered }, source);
                    return;
                }

                case RelayProtocol.ClientRegister: {
                    if (!RelayProtocol.TryReadRoom(span, out var roomId, out int used) || span.Length < used + RelayProtocol.NonceSize ||
                        !_rooms.TryGetValue(roomId, out var room) || room.HostUdp == null) {
                        return; // unknown room, or its host's UDP side isn't up yet: the client retries
                    }
                    var nonce = span.Slice(used, RelayProtocol.NonceSize).ToArray();
                    if (!_udpClients.TryGetValue(source, out var record) || !record.Nonce.AsSpan().SequenceEqual(nonce) || record.Room != room) {
                        if (room.UdpClients.Count >= MaxClientsPerRoom) {
                            return;
                        }
                        record = new UdpClientRecord(room, room.NextClientId(), source, nonce);
                        _udpClients[source] = record;
                        room.UdpClients[record.Id] = record;
                    }
                    record.LastSeenMs = NetTime.NowMs;
                    // Endpoints are advertised in plain form (an IPv4 peer must not see ::ffff:a.b.c.d).
                    var hostUdp = AllowDirect ? DualStack.Normalize(room.HostUdp) : null;
                    var reply = new byte[1 + 4 + RelayProtocol.EndPointSize(hostUdp)];
                    reply[0] = RelayProtocol.ClientRegistered;
                    BinaryPrimitives.WriteUInt32BigEndian(reply.AsSpan(1), record.Id);
                    RelayProtocol.WriteEndPoint(reply.AsSpan(5), hostUdp);
                    Send(socket, reply, source);

                    var clientEndPoint = DualStack.Normalize(source);
                    var introduce = new byte[1 + 4 + RelayProtocol.NonceSize + RelayProtocol.EndPointSize(clientEndPoint)];
                    introduce[0] = RelayProtocol.Introduce;
                    BinaryPrimitives.WriteUInt32BigEndian(introduce.AsSpan(1), record.Id);
                    nonce.CopyTo(introduce, 5);
                    RelayProtocol.WriteEndPoint(introduce.AsSpan(5 + RelayProtocol.NonceSize), clientEndPoint);
                    Send(socket, introduce, room.HostUdp);
                    return;
                }

                case RelayProtocol.RelayData:
                    if (_roomsByHostUdp.TryGetValue(source, out var hostRoom)) {
                        // host → client: [clientId][message]
                        if (span.Length >= 4 && hostRoom.UdpClients.TryGetValue(BinaryPrimitives.ReadUInt32BigEndian(span), out var target)) {
                            Interlocked.Add(ref _relayedUdpBytes, span.Length - 4);
                            buffer[4] = RelayProtocol.RelayData; // reuse the buffer: [kind][message]
                            Forward(socket, buffer, 4, received - 4, target.EndPoint);
                        }
                    }
                    else if (_udpClients.TryGetValue(source, out var client) && client.Room.HostUdp != null &&
                             span.Length >= 4 + RelayProtocol.NonceSize &&
                             BinaryPrimitives.ReadUInt32BigEndian(span) == client.Id &&
                             span.Slice(4, RelayProtocol.NonceSize).SequenceEqual(client.Nonce)) {
                        // client → host: [clientId][nonce][message] becomes [clientId][message]
                        client.LastSeenMs = NetTime.NowMs;
                        Interlocked.Add(ref _relayedUdpBytes, span.Length - 4 - RelayProtocol.NonceSize);
                        int messageLength = received - 1 - 4 - RelayProtocol.NonceSize;
                        Buffer.BlockCopy(buffer, 1 + 4 + RelayProtocol.NonceSize, buffer, 1 + 4, messageLength);
                        Forward(socket, buffer, 0, 1 + 4 + messageLength, client.Room.HostUdp);
                    }
                    return;
            }
        }

        private void Forward(Socket socket, byte[] buffer, int offset, int count, IPEndPoint target) {
            try {
#if NET8_0_OR_GREATER
                socket.SendTo(buffer.AsSpan(offset, count), SocketFlags.None, _targets!.For(target));
#else
                socket.SendTo(buffer, offset, count, SocketFlags.None, target);
#endif
            }
            catch (SocketException) { }
        }

        private static void Send(Socket socket, byte[] datagram, IPEndPoint target) {
            try {
                socket.SendTo(datagram, SocketFlags.None, target);
            }
            catch (SocketException) { }
        }

        private async Task MaintenanceLoopAsync(CancellationToken ct) {
            try {
                while (!ct.IsCancellationRequested) {
                    await Task.Delay(5000, ct).ConfigureAwait(false);
                    long now = NetTime.NowMs;
                    foreach (var kvp in _udpClients) {
                        if (now - kvp.Value.LastSeenMs > 60_000 && _udpClients.TryRemove(kvp.Key, out var stale)) {
                            stale.Room.UdpClients.TryRemove(stale.Id, out _);
                        }
                    }
                    foreach (var room in _rooms.Values) {
                        foreach (var client in room.StreamClients.Values) {
                            if (client.IsWriteStalled(15_000)) {
                                client.Dispose();
                            }
                        }
                        if (room.Host.IsWriteStalled(15_000)) {
                            room.Host.Dispose();
                        }
                    }
                }
            }
            catch (OperationCanceledException) { }
        }

        public void Stop() {
            var cts = Interlocked.Exchange(ref _cts, null);
            if (cts == null) {
                return;
            }
            cts.Cancel();
            try {
                _listener?.Stop();
            }
            catch { }
            _udp?.Dispose();
            foreach (var room in _rooms.Values) {
                room.Host.Dispose();
            }
            _rooms.Clear();
            _roomsByHostUdp.Clear();
            _udpClients.Clear();
            Port = 0;
            cts.Dispose();
        }

        public void Dispose() => Stop();
    }
}
