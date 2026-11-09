using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using NetSync.Diagnostics;
using NetSync.Internal;
using NetSync.Pipeline.Reliability;
using NetSync.Pipeline.Security;
using NetSync.Transports;
using NetSync.Transports.Udp;

namespace NetSync.Peers {
    /// <summary>
    /// High-level server: runs one server transport per transport type used by the
    /// channel table, groups links arriving with the same session token into logical
    /// <see cref="NetConnection"/>s and routes channel data.
    ///
    /// Opening a connection takes two steps: the handshake (Hello/Welcome on every link:
    /// keys, cipher suite, server identity), then Join — the client's connect data, which
    /// the application may vet in <see cref="ConnectionApprover"/>. Only then does
    /// <see cref="ConnectionOpened"/> fire.
    /// </summary>
    public sealed partial class NetServer : IDisposable, IDataEventSink {
        /// <summary>Connections still in the handshake/join phase, at most (cheap insurance against floods).</summary>
        public const int MaxPendingConnections = 1024;

        private const int CookieEpochMs = 30_000;
        private const int RejectFlushDelayMs = 250;

        private readonly Dictionary<byte, ChannelConfig> _channels;
        private readonly List<TransportType> _transportTypes;
        private readonly NetConfig _config;
        private readonly TransportSettings _settings;
        private readonly uint _configHash;
        private readonly bool _hasReliableUdp;
        private readonly INetLogger _logger;
        private readonly EventDispatcher _dispatcher;
        private readonly byte[] _cookieKey = new byte[32];
        private readonly object _createLock = new object();
        // Every running transport: one primary per slot (the shared port) plus AddListener extras.
        private readonly List<(TransportType Slot, IServerTransport Transport)> _transports = new List<(TransportType, IServerTransport)>();
        private readonly List<NetListener> _extraListeners = new List<NetListener>();
        private readonly ConcurrentDictionary<long, NetConnection> _connectionsByToken = new ConcurrentDictionary<long, NetConnection>();
        private readonly ConcurrentDictionary<long, NetConnection> _connections = new ConcurrentDictionary<long, NetConnection>();
        private readonly ConcurrentDictionary<TransportPeer, (TransportType Type, long DeadlineMs)> _pendingPeers = new ConcurrentDictionary<TransportPeer, (TransportType, long)>();
        private CancellationTokenSource? _cts;
        private long _nextConnectionId;
        private int _slotsTaken; // MaxConnections accounting: open connections plus approvals in flight
        private int _running;
        private double _startedAtMs = NetTime.PreciseMs;

        public int Port { get; private set; }
        public bool IsRunning => _running == 1;

        /// <summary>
        /// The server's clock: milliseconds since <see cref="StartAsync"/>, monotonic. Clients see
        /// the same timeline through <see cref="NetClient.ServerTimeMs"/> — stamp snapshots,
        /// schedule events or interpolate against it.
        /// </summary>
        public double TimeMs => NetTime.PreciseMs - _startedAtMs;
        public int ConnectionCount => _connections.Count;

        /// <summary>Snapshot of the currently open connections.</summary>
        public IReadOnlyCollection<NetConnection> Connections => new List<NetConnection>(_connections.Values);

        /// <summary>
        /// Optional gatekeeper: decides whether a client that completed the handshake may
        /// join, based on its connect data (login token, version…). Null = everyone joins
        /// (up to <see cref="NetConfig.MaxConnections"/>).
        /// </summary>
        public ConnectionApprover? ConnectionApprover { get; set; }

        /// <summary>
        /// Answers connectionless queries sent with <see cref="Discovery.NetQuery.QueryAsync"/> to the
        /// server's UDP port — for server browsers: name, map, player count, ping. Runs on the
        /// UDP receive thread for anyone on the internet: keep it fast and side-effect free.
        /// The reply may not be longer than the query (clients pad it); null = no reply.
        /// </summary>
        public Func<System.Net.IPEndPoint, ReadOnlyMemory<byte>, byte[]?>? QueryHandler { get; set; }

        public event Action<NetConnection>? ConnectionOpened;
        public event Action<NetConnection, DisconnectReason>? ConnectionClosed;

        /// <summary>
        /// Application data, zero-copy: (connection, channel, data). The data is valid only
        /// until the handler returns — see <see cref="NetConnectionDataHandler"/>. The
        /// allocation-free way to receive; prefer it on hot paths.
        /// </summary>
        public event NetConnectionDataHandler? MessageReceived;

        /// <summary>
        /// Application data as an owned array: (connection, channel, payload). Each message
        /// is copied into a new array — use <see cref="MessageReceived"/> on hot paths.
        /// </summary>
        public event Action<NetConnection, byte, byte[]>? DataReceived;

        public NetServer(NetConfig config) {
            _config = config ?? throw new ArgumentNullException(nameof(config));
            _logger = config.Logger;
            _channels = config.BuildChannelTable();
            config.ValidateLimits();
            config.ValidateSecurity(_channels);
            _transportTypes = config.BuildTransportList(_channels);
            _settings = config.BuildTransportSettings();
            _configHash = config.ComputeConfigHash(_channels);
            _hasReliableUdp = NetConfig.HasReliableUdpChannels(_channels);
            _dispatcher = new EventDispatcher(config.EventDelivery, _logger, config.MaxPendingEvents, sink: this);
            RandomNumberGenerator.Fill(_cookieKey);
        }

        /// <summary>
        /// Accept clients on another transport too — e.g. WebSockets for browsers next to plain
        /// TCP for native clients, both feeding the same connections, channels and events.
        /// Call before <see cref="StartAsync"/>. The listener's <see cref="NetListener.Port"/>
        /// is filled in once the server runs.
        /// </summary>
        /// <param name="slot">Which slot it serves (a WebSocket transport serves <see cref="TransportType.Tcp"/>).</param>
        /// <param name="factory">The transport implementation.</param>
        /// <param name="port">Its own port; 0 = any free port.</param>
        public NetListener AddListener(TransportType slot, ITransportFactory factory, int port) {
            if (factory == null) {
                throw new ArgumentNullException(nameof(factory));
            }
            if (IsRunning) {
                throw new InvalidOperationException("Add listeners before StartAsync");
            }
            if (!_transportTypes.Contains(slot)) {
                throw new ArgumentException($"The channel table doesn't use the {slot} slot", nameof(slot));
            }
            var listener = new NetListener(slot, factory, port);
            _extraListeners.Add(listener);
            return listener;
        }

        /// <summary>
        /// Starts listening on <paramref name="port"/> for every configured transport.
        /// Pass 0 to let the OS pick one port shared by all transports. Returns the bound port.
        /// Listeners added with <see cref="AddListener"/> start on their own ports.
        /// </summary>
        public async Task<int> StartAsync(int port, CancellationToken ct = default) {
            if (Interlocked.Exchange(ref _running, 1) == 1) {
                throw new InvalidOperationException("Server already running");
            }

            try {
                const int maxAttempts = 20;
                for (int attempt = 0; ; attempt++) {
                    _transports.Clear();
                    foreach (var type in _transportTypes) {
                        _transports.Add((type, CreateServerTransport(type, _config.GetTransportFactory(type))));
                    }
                    try {
                        int boundPort = port;
                        foreach (var (_, transport) in _transports) { // TCP first (sorted)
                            boundPort = await transport.StartAsync(boundPort, ct).ConfigureAwait(false);
                        }
                        Port = boundPort;
                        break;
                    }
                    catch (SocketException) when (port == 0 && _transportTypes.Count > 1 && attempt < maxAttempts) {
                        // The OS gave TCP a port that is busy for UDP; retry with a new pick.
                        await StopTransportsAsync().ConfigureAwait(false);
                    }
                    catch {
                        await StopTransportsAsync().ConfigureAwait(false);
                        throw;
                    }
                }

                try {
                    foreach (var listener in _extraListeners) {
                        var factory = _config.Simulation == null
                            ? listener.Factory
                            : new Transports.Simulation.SimulatedTransportFactory(listener.Factory, _config.Simulation, listener.Slot == TransportType.Udp);
                        var transport = CreateServerTransport(listener.Slot, factory);
                        _transports.Add((listener.Slot, transport));
                        listener.Port = await transport.StartAsync(listener.RequestedPort, ct).ConfigureAwait(false);
                    }
                }
                catch {
                    await StopTransportsAsync().ConfigureAwait(false);
                    throw;
                }

                _startedAtMs = NetTime.PreciseMs;
                _cts = new CancellationTokenSource();
                _ = Task.Run(() => MaintenanceLoopAsync(_cts.Token));
                _logger.Info($"NetServer listening on port {Port} ({string.Join("+", _transportTypes)})" +
                             (_extraListeners.Count > 0 ? $", plus {string.Join(", ", _extraListeners)}" : ""));
                return Port;
            }
            catch {
                Volatile.Write(ref _running, 0);
                throw;
            }
        }

        private IServerTransport CreateServerTransport(TransportType type, ITransportFactory factory) {
            var transport = factory.CreateServer(_settings);
            var core = Unwrap(transport);
            // Trust mode: pre-handshake peers get tiny message limits, no keepalive, and
            // (UDP) a cap on how many may exist at once.
            if (core is ITrustAwareServerTransport trustAware) {
                trustAware.RequireTrust = true;
            }
            transport.PeerConnected += peer => {
                peer.Owner ??= transport;
                OnPeerConnected(type, peer);
            };
            transport.PeerDisconnected += (peer, reason) => OnPeerDisconnected(peer, reason);
            // Zero-copy hook on our own transports (buffer valid only during the call —
            // HandleTransportData copies what it keeps).
            if (transport is IPooledReceiveServerTransport pooled) {
                pooled.PooledReceive = (peer, buffer, offset, count) => {
                    peer.Owner ??= transport;
                    HandleTransportData(type, peer, buffer, offset, count);
                };
            }
            else {
                transport.DataReceived += (peer, data) => {
                    peer.Owner ??= transport;
                    HandleTransportData(type, peer, data, 0, data.Length);
                };
            }
            if (transport is IDatagramBoundaryServerTransport bounded) {
                bounded.DatagramCompleted = peer => (peer.PeerLayerState as NetConnection)?.Reliable?.FlushAcks();
            }
            if (core is UdpServerTransport queryable) {
                queryable.QueryHandler = (endPoint, request) => QueryHandler?.Invoke(endPoint, request);
            }
            return transport;
        }

        /// <summary>The real transport behind wrappers such as the network simulator.</summary>
        private static IServerTransport Unwrap(IServerTransport transport) {
            while (transport is Transports.Simulation.IWrappedServerTransport wrapped) {
                transport = wrapped.Inner;
            }
            return transport;
        }

        private void OnPeerConnected(TransportType type, TransportPeer peer) {
            // Not a NetSync connection yet — only a transport link. It has until the
            // deadline to say Hello, then the maintenance loop evicts it.
            _pendingPeers[peer] = (type, NetTime.NowMs + _config.ConnectTimeoutMs);
        }

        private void OnPeerDisconnected(TransportPeer peer, DisconnectReason reason) {
            _pendingPeers.TryRemove(peer, out _);
            if (peer.PeerLayerState is NetConnection connection) {
                CloseConnection(connection, reason, notifyLinks: true);
            }
        }

        // buffer may be a pooled/transport-owned array: valid only during this call,
        // so anything kept (the payload) is copied here — exactly once per packet.
        private void HandleTransportData(TransportType type, TransportPeer peer, byte[] buffer, int offset, int count) {
            if (count == 0) {
                return;
            }

            if (_config.Encryption.Mode != KeyExchangeMode.None && buffer[offset] != PeerProtocol.MsgHello) {
                // Encrypted connection: after the handshake only authenticated envelopes count.
                if (buffer[offset] != PeerProtocol.MsgSecure ||
                    peer.PeerLayerState is not NetConnection secured ||
                    secured.Security[(int)type] is not { } security) {
                    return; // unauthenticated: drop silently
                }
                var result = security.Unwrap(buffer, offset, count, out offset, out count);
                if (result != LinkSecurity.UnwrapResult.Ok) {
                    if (result == LinkSecurity.UnwrapResult.BadTag) {
                        OnUnverifiablePacket(type, peer, secured);
                    }
                    return; // forged, replayed, tampered: drop silently
                }
                byte inner = buffer[offset];
                if (inner == PeerProtocol.MsgHello || inner == PeerProtocol.MsgSecure) {
                    return; // never valid inside an envelope
                }
            }

            switch (buffer[offset]) {
                case PeerProtocol.MsgHello:
                    HandleHello(type, peer, buffer, offset, count);
                    break;

                case PeerProtocol.MsgJoin:
                    if (peer.PeerLayerState is NetConnection joining) {
                        HandleJoin(type, joining, buffer, offset, count);
                    }
                    break;

                case PeerProtocol.MsgData: {
                    if (peer.PeerLayerState is not NetConnection connection ||
                        Volatile.Read(ref connection.Announced) != 1 ||
                        count < PeerProtocol.DataHeaderSize) {
                        return; // data before the connection is open is dropped
                    }
                    DeliverIncoming(connection, buffer[offset + 1], buffer,
                                    offset + PeerProtocol.DataHeaderSize, count - PeerProtocol.DataHeaderSize);
                    break;
                }

                case PeerProtocol.MsgRelData:
                    if (peer.PeerLayerState is NetConnection relConnection &&
                        Volatile.Read(ref relConnection.Announced) == 1) {
                        relConnection.Reliable?.HandleRelData(buffer, offset, count);
                    }
                    break;

                case PeerProtocol.MsgRelAck:
                    if (peer.PeerLayerState is NetConnection ackConnection) {
                        ackConnection.Reliable?.HandleAck(buffer, offset, count);
                    }
                    break;

                case PeerProtocol.MsgBye:
                    if (peer.PeerLayerState is NetConnection byeConnection) {
                        CloseConnection(byeConnection, DisconnectReason.Remote, notifyLinks: true);
                    }
                    break;

                case PeerProtocol.MsgDegrade:
                    if (type == TransportType.Tcp && peer.PeerLayerState is NetConnection degradedConnection) {
                        HandleDegrade(degradedConnection);
                    }
                    break;

                case PeerProtocol.MsgPathMtu:
                    if (count >= 3 && peer.PeerLayerState is NetConnection measured && measured.Reliable is { } reliable) {
                        int datagramSize = System.Buffers.Binary.BinaryPrimitives.ReadUInt16BigEndian(buffer.AsSpan(offset + 1, 2));
                        reliable.FragmentSize = ReliableEndpoint.FragmentSizeForDatagram(datagramSize, enveloped: measured.Crypto != null);
                    }
                    break;

                case PeerProtocol.MsgTimeRequest:
                    if (count >= PeerProtocol.TimeRequestSize && peer.PeerLayerState is NetConnection timed &&
                        Volatile.Read(ref timed.Announced) == 1) {
                        AnswerTimeRequest(timed, type, buffer, offset);
                    }
                    break;
            }
        }

        /// <summary>Answered on the receive thread, straight away: any delay would skew the client's estimate.</summary>
        private void AnswerTimeRequest(NetConnection connection, TransportType type, byte[] buffer, int offset) {
            var response = Pooled.Rent(PeerProtocol.TimeResponseSize);
            try {
                PeerProtocol.WriteTimeResponse(response, buffer.AsSpan(offset, PeerProtocol.TimeRequestSize), TimeMs);
                SendToLinkAsync(connection, type, ReadOnlyMemory<byte>.Empty,
                                new ReadOnlyMemory<byte>(response, 0, PeerProtocol.TimeResponseSize),
                                Sending.SendPriority.High, CancellationToken.None).Forget(_logger);
            }
            catch (Exception ex) {
                _logger.Debug($"Time sync reply to {connection} failed: {ex.Message}");
            }
            finally {
                Pooled.Return(response);
            }
        }

        // ---------------------------------------------------------------- receive

        // buffer[offset..offset+count) is only valid during this call.
        private void DeliverIncoming(NetConnection connection, byte channel, byte[] buffer, int offset, int count) {
            if (!_channels.TryGetValue(channel, out var channelConfig)) {
                return; // unknown channel — config mismatch, drop
            }
            if (count > _config.MaxMessageSize + ChannelTransform.MaxOverhead) {
                _logger.Warning($"{connection} sent a {count}-byte message, above MaxMessageSize ({_config.MaxMessageSize}); disconnecting");
                CloseConnection(connection, DisconnectReason.Error, notifyLinks: true);
                return;
            }
            if (!channelConfig.NeedsTransform) {
                connection.Traffic.OnReceived(count);
                DispatchDataEvent(connection, channel, buffer, offset, count);
                return;
            }
            var decoded = ChannelTransform.Decode(channelConfig, connection.Crypto, buffer, offset, count,
                                                  _config.MaxMessageSize, _logger, out int length);
            if (decoded == null) {
                return;
            }
            try {
                connection.Traffic.OnReceived(length);
                DispatchDataEvent(connection, channel, decoded, 0, length);
            }
            finally {
                Pooled.Return(decoded);
            }
        }

        private void DispatchDataEvent(NetConnection connection, byte channel, byte[] buffer, int offset, int count) {
            if (_dispatcher.IsImmediate) {
                RaiseData(connection, channel, buffer, offset, count); // straight from the network buffer
            }
            else {
                _dispatcher.QueueData(connection, channel, buffer, offset, count);
            }
        }

        void IDataEventSink.RaiseQueuedData(object? source, byte channel, byte[] buffer, int length) {
            RaiseData((NetConnection)source!, channel, buffer, 0, length);
        }

        private void RaiseData(NetConnection connection, byte channel, byte[] buffer, int offset, int count) {
            var zeroCopy = MessageReceived;
            if (zeroCopy != null) {
                try {
                    zeroCopy(connection, channel, new ReadOnlyMemory<byte>(buffer, offset, count));
                }
                catch (Exception ex) {
                    _logger.Error($"Unhandled exception in MessageReceived handler: {ex}");
                }
            }
            var copying = DataReceived;
            if (copying != null) {
                try {
                    copying(connection, channel, Pooled.ToArray(buffer, offset, count));
                }
                catch (Exception ex) {
                    _logger.Error($"Unhandled exception in DataReceived handler: {ex}");
                }
            }
        }

        // ------------------------------------------------------------------- send

        internal ValueTask SendAsync(NetConnection connection, byte channel, ReadOnlyMemory<byte> data, CancellationToken ct = default) {
            var channelConfig = _config.ResolveOutgoing(_channels, channel, data.Length);
            connection.Traffic.OnSent(data.Length);
            if (!channelConfig.NeedsTransform) {
                return Route(connection, channel, channelConfig, data, ct);
            }
            // Compress/encrypt first; reliability and transports see opaque bytes.
            var encoded = ChannelTransform.Encode(channelConfig, connection.Crypto, data.Span, out int length);
            try {
                return Route(connection, channel, channelConfig, new ReadOnlyMemory<byte>(encoded, 0, length), ct);
            }
            finally {
                Pooled.Return(encoded); // every send path consumes the bytes before returning
            }
        }

        private ValueTask Route(NetConnection connection, byte channel, ChannelConfig channelConfig, ReadOnlyMemory<byte> data, CancellationToken ct) {
            var slot = channelConfig.Transport == TransportType.Udp && connection.IsDegraded
                ? TransportType.Tcp
                : channelConfig.Transport;
            if (slot == TransportType.Udp && channelConfig.Reliability != ReliabilityMode.Unreliable) {
                var reliable = connection.Reliable
                    ?? throw new InvalidOperationException($"Connection#{connection.Id} has no UDP link");
                return reliable.SendAsync(channel, data, ct);
            }
            if (!connection.Links.ContainsKey(slot)) {
                throw new InvalidOperationException($"Connection#{connection.Id} has no {slot} link");
            }
            // Cached 2-byte prefix + gather send: no per-send allocations at this layer.
            return SendToLinkAsync(connection, slot, PeerProtocol.DataPrefix(channel), data,
                                   PriorityMapping.ToSendPriority(channelConfig.Priority), ct);
        }

        /// <summary>
        /// Single exit for post-handshake traffic: wraps it in the authenticated envelope
        /// on encrypted connections, applies priority when the transport supports it.
        /// </summary>
        private ValueTask SendToLinkAsync(NetConnection connection, TransportType slot, ReadOnlyMemory<byte> prefix,
                                          ReadOnlyMemory<byte> payload, Sending.SendPriority priority, CancellationToken ct) {
            if (!connection.Links.TryGetValue(slot, out var peer) || peer.Owner is not { } transport) {
                return default; // link gone (e.g. degraded UDP): nothing to send on
            }
            if (connection.Crypto == null) {
                return SendRaw(transport, peer, prefix, payload, priority, ct);
            }
            var security = connection.Security[(int)slot]
                ?? throw new InvalidOperationException($"{connection}: {slot} link has no authentication keys");
            var envelope = security.WrapPooled(prefix.Span, payload.Span, out int length);
            try {
                return SendRaw(transport, peer, ReadOnlyMemory<byte>.Empty, new ReadOnlyMemory<byte>(envelope, 0, length), priority, ct);
            }
            finally {
                Pooled.Return(envelope); // transports consume the bytes before returning
            }
        }

        private static ValueTask SendRaw(IServerTransport transport, TransportPeer peer, ReadOnlyMemory<byte> prefix,
                                         ReadOnlyMemory<byte> payload, Sending.SendPriority priority, CancellationToken ct) {
            return transport is IPrioritizedServerTransport prioritized
                ? prioritized.SendAsync(peer, prefix, payload, priority, ct)
                : transport.SendAsync(peer, prefix, payload, ct);
        }

        /// <summary>A control message through the envelope; failures are logged, never thrown.</summary>
        private async Task SafeSendAsync(NetConnection connection, TransportType slot, byte[] message) {
            try {
                await SendToLinkAsync(connection, slot, ReadOnlyMemory<byte>.Empty, message,
                                      Sending.SendPriority.High, CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception ex) {
                _logger.Debug($"Control send to {connection} failed: {ex.Message}");
            }
        }

        /// <summary>Sends to every open connection; individual failures are logged, not thrown.</summary>
        public async Task BroadcastAsync(byte channel, ReadOnlyMemory<byte> data, CancellationToken ct = default) {
            foreach (var connection in _connections.Values) {
                try {
                    await SendAsync(connection, channel, data, ct).ConfigureAwait(false);
                }
                catch (Exception ex) {
                    _logger.Debug($"Broadcast to {connection}: {ex.Message}");
                }
            }
        }

        private const int ByeFlushTimeoutMs = 1000;

        internal async Task KickAsync(NetConnection connection, bool kicked) {
            byte reason = kicked ? PeerProtocol.ByeKicked : PeerProtocol.ByeGraceful;
            foreach (var kvp in connection.Links) {
                try {
                    await SendToLinkAsync(connection, kvp.Key, ReadOnlyMemory<byte>.Empty, PeerProtocol.EncodeBye(reason),
                                          Sending.SendPriority.High, CancellationToken.None).ConfigureAwait(false);
                }
                catch (Exception ex) {
                    _logger.Debug($"Bye to {connection} failed: {ex.Message}");
                }
            }
            // The Bye is only queued so far, and closing a link discards its queue. Bounded:
            // a client that stopped reading can't hold the kick up.
            foreach (var kvp in connection.Links) {
                if (kvp.Value.Owner is IPeerFlushingServerTransport flushing) {
                    await flushing.FlushPeerAsync(kvp.Value, ByeFlushTimeoutMs).ConfigureAwait(false);
                }
            }
            CloseConnection(connection, DisconnectReason.Local, notifyLinks: true);
        }

        private async Task SafeSendRawAsync(TransportPeer peer, byte[] packet) {
            try {
                if (peer.Owner != null) {
                    await peer.Owner.SendAsync(peer, packet).ConfigureAwait(false);
                }
            }
            catch (Exception ex) {
                _logger.Debug($"Send to {peer.EndPoint} failed: {ex.Message}");
            }
        }

        private void CloseConnection(NetConnection connection, DisconnectReason reason, bool notifyLinks) {
            if (Interlocked.Exchange(ref connection.Closed, 1) == 1) {
                return;
            }
            _connectionsByToken.TryRemove(connection.Token, out _);
            _connections.TryRemove(connection.Id, out _);
            ReleaseSlot(connection);
            connection.Reliable?.Dispose();
            connection.Crypto?.Dispose();
            foreach (var security in connection.Security) {
                security?.Dispose();
            }

            foreach (var kvp in connection.Links) {
                kvp.Value.PeerLayerState = null;
                if (notifyLinks) {
                    kvp.Value.Owner?.DisconnectPeer(kvp.Value);
                }
            }

            if (Volatile.Read(ref connection.Announced) == 1) {
                _dispatcher.Dispatch(() => ConnectionClosed?.Invoke(connection, reason));
            }
        }

        private async Task MaintenanceLoopAsync(CancellationToken ct) {
            try {
                while (!ct.IsCancellationRequested) {
                    long now = NetTime.NowMs;

                    foreach (var kvp in _pendingPeers) {
                        if (now > kvp.Value.DeadlineMs && _pendingPeers.TryRemove(kvp.Key, out var entry)) {
                            _logger.Debug($"Evicting {kvp.Key.EndPoint}: no Hello within timeout");
                            kvp.Key.Owner?.DisconnectPeer(kvp.Key);
                        }
                    }

                    foreach (var kvp in _connectionsByToken) {
                        var connection = kvp.Value;
                        if (Volatile.Read(ref connection.Announced) == 0 &&
                            now - connection.CreatedAtMs > _config.ConnectTimeoutMs) {
                            _logger.Debug($"Closing {connection}: handshake/join incomplete within timeout");
                            CloseConnection(connection, DisconnectReason.Timeout, notifyLinks: true);
                        }
                    }

                    await Task.Delay(250, ct).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) { }
        }

        /// <summary>Drains queued events (Polled mode). Returns the number of events handled.</summary>
        public int PollEvents(int maxEvents = int.MaxValue) => _dispatcher.Poll(maxEvents);

        public async Task StopAsync() {
            if (Interlocked.Exchange(ref _running, 0) == 0) {
                return;
            }
            _cts?.Cancel();

            var byes = new List<Task>(); // together: one stalled client must not delay the rest
            foreach (var connection in _connections.Values) {
                byes.Add(KickAsync(connection, kicked: false)); // shutdown: clients may reconnect
            }
            await Task.WhenAll(byes).ConfigureAwait(false);
            foreach (var connection in _connectionsByToken.Values) {
                CloseConnection(connection, DisconnectReason.Local, notifyLinks: true); // half-open handshakes
            }
            await StopTransportsAsync().ConfigureAwait(false);

            _pendingPeers.Clear();
            _connectionsByToken.Clear();
            _connections.Clear();
            _transports.Clear();
            foreach (var listener in _extraListeners) {
                listener.Port = 0;
            }
            Port = 0;
            _cts?.Dispose();
            _cts = null;
        }

        private async Task StopTransportsAsync() {
            foreach (var (_, transport) in _transports) {
                try {
                    await transport.StopAsync().ConfigureAwait(false);
                }
                catch (Exception ex) {
                    _logger.Debug($"Transport stop: {ex.Message}");
                }
            }
        }

        public void Dispose() => StopAsync().GetAwaiter().GetResult();
    }
}
