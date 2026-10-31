using System;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using NetSync.Diagnostics;
using NetSync.Internal;

namespace NetSync.Transports.Udp {
    /// <summary>
    /// Server-side UDP transport. Tracks peers by source address, pings them actively
    /// and expires the silent ones.
    ///
    /// One socket, one receive thread, one reused buffer. On net8.0 source addresses are
    /// read into a reused <see cref="SocketAddress"/> and peers are looked up by it, so a
    /// datagram from a known peer costs no allocation in either direction. Sends go
    /// through one <see cref="DatagramBatcher"/>: small packets to the same peer share
    /// datagrams while the send loop is busy.
    ///
    /// Spoofing-aware: UDP source addresses are trivially forged, so a peer is only
    /// created by a Data datagram (pings from unknown sources are ignored — no
    /// reflection), and in trust-required mode untrusted peers are capped, get no
    /// keepalive traffic and expire unless the peer layer marks them trusted.
    /// </summary>
    public sealed class UdpServerTransport : IServerTransport, IPooledReceiveServerTransport, IDatagramBoundaryServerTransport,
                                             ITrustAwareServerTransport {
        /// <summary>Upper bound on peers that have not completed the handshake.</summary>
        public const int MaxUntrustedPeers = 1024;

        private readonly TransportSettings _settings;
        private readonly INetLogger _logger;
        // Key: SocketAddress (net8.0) or IPEndPoint (netstandard2.1) — both compare by value.
        private readonly ConcurrentDictionary<object, TransportPeer> _peersByAddress =
            new ConcurrentDictionary<object, TransportPeer>(AddressComparer.Instance);
        private Socket? _socket;
        private DatagramBatcher? _batcher;
        private CancellationTokenSource? _cts;
        private int _running;
        private int _untrustedCount;

        public int Port { get; private set; }
        public bool IsRunning => _running == 1;

        public event Action<TransportPeer>? PeerConnected;
        public event Action<TransportPeer, DisconnectReason>? PeerDisconnected;
        public event Action<TransportPeer, byte[]>? DataReceived;

        /// <summary>Zero-copy delivery hook, see <see cref="Tcp.TcpTransport.PooledReceive"/>.</summary>
        internal Action<TransportPeer, byte[], int, int>? PooledReceive { get; set; }

        Action<TransportPeer, byte[], int, int>? IPooledReceiveServerTransport.PooledReceive {
            get => PooledReceive;
            set => PooledReceive = value;
        }

        /// <summary>Raised after the packets of one received datagram were delivered.</summary>
        internal Action<TransportPeer>? DatagramCompleted { get; set; }

        Action<TransportPeer>? IDatagramBoundaryServerTransport.DatagramCompleted {
            get => DatagramCompleted;
            set => DatagramCompleted = value;
        }

        bool ITrustAwareServerTransport.RequireTrust {
            get => RequireTrust;
            set => RequireTrust = value;
        }

        void ITrustAwareServerTransport.MarkTrusted(TransportPeer peer) => MarkTrusted(peer);

        /// <summary>Set by NetServer: enables the untrusted-peer cap and keepalive suppression.</summary>
        internal bool RequireTrust { get; set; }

        /// <summary>Answers connectionless queries (server browsers); null = queries ignored.</summary>
        internal Func<IPEndPoint, ReadOnlyMemory<byte>, byte[]?>? QueryHandler { get; set; }

        /// <summary>Connectionless packets (queries, MTU probes) answered per second, at most.</summary>
        internal int UnconnectedPerSecond { get; set; } = 1000;

        // Token bucket for connectionless traffic; touched only by the receive thread.
        private double _unconnectedTokens = 1000;
        private long _unconnectedRefillMs = NetTime.NowMs;

        public UdpServerTransport(int pingIntervalMs = 1000, int pingTimeoutMs = 5000, INetLogger? logger = null)
            : this(TransportSettings.From(pingIntervalMs, pingTimeoutMs, logger)) { }

        public UdpServerTransport(TransportSettings settings) {
            _settings = settings ?? throw new ArgumentNullException(nameof(settings));
            _logger = settings.Logger;
        }

        public Task<int> StartAsync(int port, CancellationToken ct = default) {
            if (Interlocked.Exchange(ref _running, 1) == 1) {
                throw new InvalidOperationException("Server transport already running");
            }

            try {
                _socket = UdpSockets.BindServer(port);
            }
            catch {
                Volatile.Write(ref _running, 0);
                throw;
            }
            _cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            Port = ((IPEndPoint)_socket.LocalEndPoint!).Port;

            var socket = _socket;
            var token = _cts.Token;
            _batcher = new DatagramBatcher((destination, buffer, offset, length) =>
                SendRaw(socket, (TransportPeer)destination!, buffer, offset, length), _logger);
            var receiveThread = new Thread(() => ReceiveLoop(socket, token)) {
                IsBackground = true,
                Name = "NetSync UDP server receive"
            };
            receiveThread.Start();
            _ = Task.Run(() => PingLoopAsync(socket, token));

            _logger.Info($"UdpServerTransport listening on port {Port}");
            return Task.FromResult(Port);
        }

        /// <summary>Called by the peer layer when a peer completes the handshake.</summary>
        internal void MarkTrusted(TransportPeer peer) {
            if (!peer.IsTrusted) {
                peer.MarkTrusted();
                Interlocked.Decrement(ref _untrustedCount);
            }
        }

        private void ReceiveLoop(Socket socket, CancellationToken ct) {
            var buffer = Pooled.Rent(UdpSockets.ReceiveBufferSize);
#if NET8_0_OR_GREATER
            var address = new SocketAddress(socket.AddressFamily);
#else
            EndPoint anyEndPoint = new IPEndPoint(socket.AddressFamily == AddressFamily.InterNetworkV6 ? IPAddress.IPv6Any : IPAddress.Any, 0);
#endif
            try {
                while (!ct.IsCancellationRequested) {
                    int received;
                    object key;
                    try {
#if NET8_0_OR_GREATER
                        received = socket.ReceiveFrom(buffer.AsSpan(0, UdpSockets.ReceiveBufferSize), SocketFlags.None, address);
                        key = address;
#else
                        EndPoint remote = anyEndPoint;
                        received = socket.ReceiveFrom(buffer, 0, UdpSockets.ReceiveBufferSize, SocketFlags.None, ref remote);
                        key = remote;
#endif
                    }
                    catch (SocketException ex) when (!ct.IsCancellationRequested && UdpSockets.IsTransient(ex.SocketErrorCode)) {
                        continue; // ICMP "port unreachable" from a vanished client
                    }
                    if (received == 0) {
                        continue;
                    }
                    var batcher = _batcher;
                    batcher?.Cork(); // replies to this datagram leave together
                    try {
                        HandleDatagram(socket, buffer, received, key);
                    }
                    finally {
                        batcher?.Uncork();
                    }
                }
            }
            catch (Exception) when (ct.IsCancellationRequested) {
                // stopping
            }
            catch (ObjectDisposedException) {
                // stopping
            }
            catch (Exception ex) {
                _logger.Error($"UdpServerTransport receive loop: {ex}");
            }
            finally {
                Pooled.Return(buffer);
            }
        }

        private void HandleDatagram(Socket socket, byte[] buffer, int received, object key) {
            long now = NetTime.NowMs;
            var type = (PacketType)buffer[0];
            if (type == PacketType.Query || type == PacketType.MtuProbe) {
                HandleUnconnected(socket, buffer, received, key, type, now);
                return;
            }

            if (!_peersByAddress.TryGetValue(key, out var peer)) {
                // Only data may open a peer: answering pings from unknown sources would
                // turn us into a reflector for spoofed traffic.
                if ((type != PacketType.Data && type != PacketType.Batch) || !TryCreatePeer(key, now, out peer)) {
                    return;
                }
            }
            peer.Touch(now);

            switch (type) {
                case PacketType.Ping:
                    if (received >= PacketProtocol.PingPacketSize) {
                        var pong = PacketProtocol.RentPongPacket(PacketProtocol.ReadTimestamp(buffer));
                        try {
                            SendRaw(socket, peer, pong, 0, PacketProtocol.PingPacketSize);
                        }
                        finally {
                            PacketProtocol.Return(pong);
                        }
                    }
                    break;

                case PacketType.Pong:
                    if (received >= PacketProtocol.PingPacketSize) {
                        peer.SetPing((int)Math.Max(0, now - PacketProtocol.ReadTimestamp(buffer)));
                    }
                    break;

                case PacketType.Data:
                    if (RequireTrust && !peer.IsTrusted && received - 1 > PacketProtocol.MaxUntrustedFrameBody) {
                        return; // pre-handshake: only small control messages
                    }
                    Deliver((this, peer), buffer, 1, received - 1);
                    DatagramCompleted?.Invoke(peer);
                    break;

                case PacketType.Batch:
                    if (peer.IsTrusted || !RequireTrust) {
                        DatagramBatcher.Unpack(buffer, received, int.MaxValue, (this, peer), DeliverPacket);
                        DatagramCompleted?.Invoke(peer);
                    }
                    else if (received - 1 <= PacketProtocol.MaxUntrustedFrameBody) {
                        // Pre-handshake traffic is one control message at a time: a batch from an
                        // untrusted source gets the work of one packet, not of hundreds.
                        DatagramBatcher.Unpack(buffer, received, 1, (this, peer), DeliverPacket);
                        DatagramCompleted?.Invoke(peer);
                    }
                    break;
            }
        }

        private static readonly Action<(UdpServerTransport, TransportPeer), byte[], int, int> DeliverPacket = Deliver;

        private static void Deliver((UdpServerTransport Transport, TransportPeer Peer) target, byte[] buffer, int offset, int length) {
            ReceivedPacket.Deliver(target.Transport.PooledReceive, target.Transport.DataReceived, target.Peer, buffer, offset, length); // zero-copy: valid only during the call
        }

        /// <summary>
        /// Connectionless requests from anyone. Two rules keep them from being abused as a
        /// reflector: a reply is never larger than the request (no amplification — clients pad
        /// their queries to the reply size they want), and the total rate is capped.
        /// </summary>
        private void HandleUnconnected(Socket socket, byte[] buffer, int received, object key, PacketType type, long now) {
            _unconnectedTokens = Math.Min(UnconnectedPerSecond, _unconnectedTokens + (now - _unconnectedRefillMs) * UnconnectedPerSecond / 1000.0);
            _unconnectedRefillMs = now;
            if (_unconnectedTokens < 1) {
                return;
            }
            _unconnectedTokens -= 1;

            if (type == PacketType.MtuProbe) {
                if (received < 5) {
                    return;
                }
                var reply = Pooled.Rent(7);
                try {
                    reply[0] = (byte)PacketType.MtuReply;
                    Buffer.BlockCopy(buffer, 1, reply, 1, 4);
                    System.Buffers.Binary.BinaryPrimitives.WriteUInt16BigEndian(reply.AsSpan(5), (ushort)received);
                    SendUnconnected(socket, key, reply, 7);
                }
                finally {
                    Pooled.Return(reply);
                }
                return;
            }

            var handler = QueryHandler;
            if (handler == null || received < 7) {
                return;
            }
            int requestLength = System.Buffers.Binary.BinaryPrimitives.ReadUInt16BigEndian(buffer.AsSpan(5));
            if (7 + requestLength > received) {
                return;
            }
            byte[]? response;
            try {
                response = handler(ToEndPoint(key), new ReadOnlyMemory<byte>(buffer, 7, requestLength).ToArray());
            }
            catch (Exception ex) {
                _logger.Error($"Query handler threw: {ex}");
                return;
            }
            if (response == null) {
                return;
            }
            int maxResponse = received - 5;
            if (response.Length > maxResponse) {
                _logger.Debug($"Query response of {response.Length} B dropped: the query was only {received} B " +
                              "(replies never exceed the request — pass a larger maxResponseSize to NetQuery)");
                return;
            }
            var packet = Pooled.Rent(5 + response.Length);
            try {
                packet[0] = (byte)PacketType.QueryReply;
                Buffer.BlockCopy(buffer, 1, packet, 1, 4);
                response.CopyTo(packet, 5);
                SendUnconnected(socket, key, packet, 5 + response.Length);
            }
            finally {
                Pooled.Return(packet);
            }
        }

        private static IPEndPoint ToEndPoint(object key) {
#if NET8_0_OR_GREATER
            if (key is SocketAddress address) {
                return DualStack.Normalize((IPEndPoint)new IPEndPoint(IPAddress.IPv6Any, 0).Create(address));
            }
#endif
            return DualStack.Normalize((IPEndPoint)key);
        }

        private static void SendUnconnected(Socket socket, object key, byte[] packet, int length) {
            try {
#if NET8_0_OR_GREATER
                if (key is SocketAddress address) {
                    socket.SendTo(packet.AsSpan(0, length), SocketFlags.None, address);
                    return;
                }
#endif
                socket.SendTo(packet, 0, length, SocketFlags.None, (EndPoint)key);
            }
            catch (SocketException) {
                // best effort
            }
        }

        private bool TryCreatePeer(object receivedKey, long now, out TransportPeer peer) {
            peer = null!;
            if (RequireTrust && Volatile.Read(ref _untrustedCount) >= MaxUntrustedPeers) {
                return false; // under a spoofing flood; legit peers retry their Hello
            }
#if NET8_0_OR_GREATER
            // The receive loop reuses its SocketAddress: the dictionary keeps a copy.
            var source = (SocketAddress)receivedKey;
            var key = new SocketAddress(source.Family, source.Size);
            source.Buffer.Span.Slice(0, source.Size).CopyTo(key.Buffer.Span);
            var endPoint = (IPEndPoint)new IPEndPoint(IPAddress.IPv6Any, 0).Create(key);
#else
            var key = (IPEndPoint)receivedKey;
            var endPoint = key;
#endif
            var created = new TransportPeer(DualStack.Normalize(endPoint), now) { TransportState = key };
            created.SendState = _batcher?.CreateTarget(created);
            if (!_peersByAddress.TryAdd(key, created)) {
                return _peersByAddress.TryGetValue(key, out peer!);
            }
            Interlocked.Increment(ref _untrustedCount);
            peer = created;
            PeerConnected?.Invoke(created);
            return true;
        }

        private async Task PingLoopAsync(Socket socket, CancellationToken ct) {
            try {
                while (!ct.IsCancellationRequested) {
                    foreach (var kvp in _peersByAddress) {
                        var peer = kvp.Value;
                        if (!RequireTrust || peer.IsTrusted) {
                            var ping = PacketProtocol.RentPingPacket(NetTime.NowMs);
                            try {
                                SendRaw(socket, peer, ping, 0, PacketProtocol.PingPacketSize);
                            }
                            finally {
                                PacketProtocol.Return(ping);
                            }
                        }

                        if (_settings.PingTimeoutMs > 0 && NetTime.NowMs - peer.LastSeenMs > _settings.PingTimeoutMs) {
                            RemovePeer(peer, DisconnectReason.Timeout);
                        }
                    }
                    await Task.Delay(_settings.PingIntervalMs, ct).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) { }
            catch (ObjectDisposedException) { }
            catch (InvalidOperationException) { } // stopped under us
        }

        public ValueTask SendAsync(TransportPeer peer, ReadOnlyMemory<byte> data, CancellationToken ct = default) {
            return SendAsync(peer, ReadOnlyMemory<byte>.Empty, data, ct);
        }

        public ValueTask SendAsync(TransportPeer peer, ReadOnlyMemory<byte> prefix, ReadOnlyMemory<byte> payload, CancellationToken ct = default) {
            int totalLength = prefix.Length + payload.Length;
            if (totalLength > UdpTransport.MaxDatagramPayload) {
                throw new ArgumentException(
                    $"UDP payload of {totalLength} bytes exceeds the {UdpTransport.MaxDatagramPayload}-byte datagram limit.", nameof(payload));
            }
            var batcher = _batcher;
            if (batcher == null || !(peer.SendState is DatagramBatcher.Target target)) {
                throw new InvalidOperationException("Server transport not started");
            }
            batcher.Enqueue(target, prefix.Span, payload.Span); // false = dropped (peer gone or backlog full)
            return default;
        }

        private static void SendRaw(Socket socket, TransportPeer peer, byte[] packet, int offset, int length) {
            try {
#if NET8_0_OR_GREATER
                if (peer.TransportState is SocketAddress address) {
                    socket.SendTo(packet.AsSpan(offset, length), SocketFlags.None, address);
                    return;
                }
#endif
                socket.SendTo(packet, offset, length, SocketFlags.None, RawEndPoint(peer));
            }
            catch (SocketException ex) when (UdpSockets.IsTransient(ex.SocketErrorCode)) {
                // lost, exactly as if the network had dropped it
            }
            catch (ObjectDisposedException) {
                throw new InvalidOperationException("Server transport stopped");
            }
        }

        public void DisconnectPeer(TransportPeer peer) => RemovePeer(peer, DisconnectReason.Local);

        private static EndPoint RawEndPoint(TransportPeer peer) => peer.TransportState as IPEndPoint ?? peer.EndPoint;

        private void RemovePeer(TransportPeer peer, DisconnectReason reason) {
            var key = peer.TransportState ?? peer.EndPoint;
            if (_peersByAddress.TryRemove(key, out var removed) && ReferenceEquals(removed, peer)) {
                if (peer.SendState is DatagramBatcher.Target target) {
                    _batcher?.Close(target);
                }
                if (!peer.IsTrusted) {
                    Interlocked.Decrement(ref _untrustedCount);
                }
                PeerDisconnected?.Invoke(peer, reason);
            }
            else if (removed != null) {
                _peersByAddress.TryAdd(key, removed); // a newer peer took the address; keep it
            }
        }

        public async Task StopAsync() {
            if (Interlocked.Exchange(ref _running, 0) == 0) {
                return;
            }
            _cts?.Cancel();
            foreach (var kvp in _peersByAddress) {
                RemovePeer(kvp.Value, DisconnectReason.Local);
            }
            _batcher?.Dispose();
            _batcher = null;
            try {
                _socket?.Close(); // unblocks the receive thread
            }
            catch { }
            _socket = null;
            Port = 0;
            _untrustedCount = 0;
            _cts?.Dispose();
            _cts = null;
            await Task.CompletedTask;
        }

        public void Dispose() => StopAsync().GetAwaiter().GetResult();

        /// <summary>
        /// Compares socket addresses by content, computing the hash from the bytes every
        /// time — the receive loop rewrites one SocketAddress in place, so nothing about it
        /// may be cached.
        /// </summary>
        private sealed class AddressComparer : System.Collections.Generic.IEqualityComparer<object> {
            public static readonly AddressComparer Instance = new AddressComparer();

            public new bool Equals(object? x, object? y) {
#if NET8_0_OR_GREATER
                if (x is SocketAddress a && y is SocketAddress b) {
                    return a.Size == b.Size && a.Buffer.Span.Slice(0, a.Size).SequenceEqual(b.Buffer.Span.Slice(0, b.Size));
                }
#endif
                return object.Equals(x, y);
            }

            public int GetHashCode(object obj) {
#if NET8_0_OR_GREATER
                if (obj is SocketAddress address) {
                    var hash = new HashCode();
                    hash.AddBytes(address.Buffer.Span.Slice(0, address.Size));
                    return hash.ToHashCode();
                }
#endif
                return obj.GetHashCode();
            }
        }
    }
}
