using System;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using NetSync.Diagnostics;
using NetSync.Internal;
using NetSync.Pipeline.Reliability;
using NetSync.Pipeline.Security;
using NetSync.Transports;
using NetSync.Transports.Tcp;
using NetSync.Transports.Udp;

namespace NetSync.Peers {
    /// <summary>
    /// High-level server: runs one server transport per transport type used by the
    /// channel table, groups links arriving with the same session token into logical
    /// <see cref="NetConnection"/>s and routes channel data.
    ///
    /// A connection is announced via <see cref="ConnectionOpened"/> only once links for
    /// ALL configured transports completed the handshake; clients must therefore use
    /// the same channel table as the server.
    /// </summary>
    public sealed class NetServer : IDisposable {
        private readonly Dictionary<byte, ChannelConfig> _channels;
        private readonly List<TransportType> _transportTypes;
        private readonly NetConfig _config;
        private readonly INetLogger _logger;
        private readonly EventDispatcher _dispatcher;
        private readonly Dictionary<TransportType, IServerTransport> _transports = new Dictionary<TransportType, IServerTransport>();
        private readonly ConcurrentDictionary<long, NetConnection> _connectionsByToken = new ConcurrentDictionary<long, NetConnection>();
        private readonly ConcurrentDictionary<long, NetConnection> _connections = new ConcurrentDictionary<long, NetConnection>();
        private readonly ConcurrentDictionary<TransportPeer, (TransportType Type, long DeadlineMs)> _pendingPeers = new ConcurrentDictionary<TransportPeer, (TransportType, long)>();
        private CancellationTokenSource? _cts;
        private long _nextConnectionId;
        private int _running;

        public int Port { get; private set; }
        public bool IsRunning => _running == 1;
        public int ConnectionCount => _connections.Count;

        public event Action<NetConnection>? ConnectionOpened;
        public event Action<NetConnection, DisconnectReason>? ConnectionClosed;
        /// <summary>Application data: (connection, channel, payload).</summary>
        public event Action<NetConnection, byte, byte[]>? DataReceived;

        public NetServer(NetConfig config) {
            _config = config ?? throw new ArgumentNullException(nameof(config));
            _logger = config.Logger;
            _channels = config.BuildChannelTable();
            config.ValidateSecurity(_channels);
            _transportTypes = config.BuildTransportList(_channels);
            _dispatcher = new EventDispatcher(config.EventDelivery, _logger);
        }

        /// <summary>
        /// Starts listening on <paramref name="port"/> for every configured transport.
        /// Pass 0 to let the OS pick one port shared by all transports. Returns the bound port.
        /// </summary>
        public async Task<int> StartAsync(int port, CancellationToken ct = default) {
            if (Interlocked.Exchange(ref _running, 1) == 1) {
                throw new InvalidOperationException("Server already running");
            }

            try {
                const int maxAttempts = 20;
                for (int attempt = 0; ; attempt++) {
                    CreateTransports();
                    try {
                        int boundPort = port;
                        foreach (var type in _transportTypes) { // TCP first (sorted)
                            boundPort = await _transports[type].StartAsync(boundPort, ct).ConfigureAwait(false);
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

                _cts = new CancellationTokenSource();
                _ = Task.Run(() => MaintenanceLoopAsync(_cts.Token));
                _logger.Info($"NetServer listening on port {Port} ({string.Join("+", _transportTypes)})");
                return Port;
            }
            catch {
                Volatile.Write(ref _running, 0);
                throw;
            }
        }

        private void CreateTransports() {
            _transports.Clear();
            foreach (var type in _transportTypes) {
                IServerTransport transport = type switch {
                    TransportType.Tcp => new TcpServerTransport(_config.PingIntervalMs, _config.PingTimeoutMs, _logger),
                    TransportType.Udp => new UdpServerTransport(_config.PingIntervalMs, _config.PingTimeoutMs, _logger),
                    _ => throw new NotSupportedException($"Transport {type} is not supported")
                };
                var capturedType = type;
                transport.PeerConnected += peer => OnPeerConnected(capturedType, peer);
                transport.PeerDisconnected += (peer, reason) => OnPeerDisconnected(peer, reason);
                // Zero-copy hook on our own transports (buffer valid only during the
                // call — HandleTransportData copies what it keeps).
                switch (transport) {
                    case TcpServerTransport tcp:
                        tcp.PooledReceive = (peer, buffer, offset, count) => HandleTransportData(capturedType, peer, buffer, offset, count);
                        break;
                    case UdpServerTransport udp:
                        udp.PooledReceive = (peer, buffer, offset, count) => HandleTransportData(capturedType, peer, buffer, offset, count);
                        break;
                    default:
                        transport.DataReceived += (peer, data) => HandleTransportData(capturedType, peer, data, 0, data.Length);
                        break;
                }
                _transports[type] = transport;
            }
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

            switch (buffer[offset]) {
                case PeerProtocol.MsgHello:
                    HandleHello(type, peer, buffer, offset, count);
                    break;

                case PeerProtocol.MsgData: {
                    if (peer.PeerLayerState is not NetConnection connection ||
                        Volatile.Read(ref connection.Announced) != 1 ||
                        count < PeerProtocol.DataHeaderSize) {
                        return; // data before handshake completion is dropped
                    }
                    byte channel = buffer[offset + 1];
                    var payload = new byte[count - PeerProtocol.DataHeaderSize];
                    Array.Copy(buffer, offset + PeerProtocol.DataHeaderSize, payload, 0, payload.Length);
                    DeliverIncoming(connection, channel, payload);
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
            }
        }

        /// <summary>Derives per-connection keys on first Hello. Returns false when the Hello is unusable.</summary>
        private bool TryEstablishCrypto(NetConnection connection, byte[] buffer, int offset, int clientKeyLength) {
            switch (_config.Encryption.Mode) {
                case KeyExchangeMode.PresharedKey:
                    lock (connection.CryptoLock) {
                        connection.Crypto ??= ConnectionCrypto.FromPresharedKey(_config.Encryption.PresharedKey!, connection.Token);
                    }
                    return true;

                case KeyExchangeMode.Ecdh:
                    lock (connection.CryptoLock) {
                        if (connection.Crypto != null) {
                            return true; // derived by an earlier Hello
                        }
                        if (clientKeyLength != EcdhKeyExchange.PublicKeySize || connection.Ecdh == null) {
                            return false;
                        }
                        try {
                            var secret = EcdhKeyExchange.DeriveSessionSecret(
                                connection.Ecdh, buffer.AsSpan(offset + 12, clientKeyLength), connection.Token);
                            connection.Crypto = ConnectionCrypto.FromSecret(secret);
                            Array.Clear(secret, 0, secret.Length);
                        }
                        catch (Exception ex) {
                            _logger.Error($"ECDH key derivation failed for {connection}: {ex.Message}");
                            return false;
                        }
                        connection.Ecdh.Dispose();
                        connection.Ecdh = null; // public key stays cached for Welcome re-sends
                        return true;
                    }

                default:
                    return true;
            }
        }

        private void DeliverIncoming(NetConnection connection, byte channel, byte[] payload) {
            if (!_channels.TryGetValue(channel, out var channelConfig)) {
                return; // unknown channel — config mismatch, drop
            }
            var decoded = ChannelTransform.Decode(channelConfig, connection.Crypto, payload, _logger);
            if (decoded != null) {
                DispatchDataEvent(connection, channel, decoded);
            }
        }

        private void DispatchDataEvent(NetConnection connection, byte channel, byte[] payload) {
            if (_dispatcher.IsImmediate) {
                // Direct invoke: no closure allocation on the hot path.
                try {
                    DataReceived?.Invoke(connection, channel, payload);
                }
                catch (Exception ex) {
                    _logger.Error($"Unhandled exception in DataReceived handler: {ex}");
                }
            }
            else {
                _dispatcher.Dispatch(() => DataReceived?.Invoke(connection, channel, payload));
            }
        }

        private void HandleHello(TransportType type, TransportPeer peer, byte[] buffer, int offset, int count) {
            if (count < 10) {
                return;
            }
            byte version = buffer[offset + 1];
            if (version != PeerProtocol.ProtocolVersion) {
                _logger.Warning($"Rejecting {peer.EndPoint}: protocol version {version}, expected {PeerProtocol.ProtocolVersion}");
                _ = SafeSendRawAsync(type, peer, PeerProtocol.EncodeReject(PeerProtocol.RejectVersionMismatch));
                return;
            }

            long token = BinaryPrimitives.ReadInt64BigEndian(buffer.AsSpan(offset + 2, 8));
            if (token == 0) {
                return;
            }

            // Cipher negotiation tail: absent (legacy 10-byte Hello) means None.
            byte cipher = count >= 12 ? buffer[offset + 10] : (byte)0;
            int clientKeyLength = count >= 12 ? buffer[offset + 11] : 0;
            if (cipher != (byte)_config.Encryption.Mode || (count >= 12 && count < 12 + clientKeyLength)) {
                _logger.Warning($"Rejecting {peer.EndPoint}: encryption mode mismatch (client {cipher}, server {(byte)_config.Encryption.Mode})");
                _ = SafeSendRawAsync(type, peer, PeerProtocol.EncodeReject(PeerProtocol.RejectEncryptionMismatch));
                return;
            }

            // NOTE: the token itself is plaintext; with PSK/ECDH the payloads are
            // protected, but the handshake is not authenticated (see KeyExchangeMode docs).
            var connection = _connectionsByToken.GetOrAdd(token, _ => {
                long id = Interlocked.Increment(ref _nextConnectionId);
                var created = new NetConnection(this, id, token, NetTime.NowMs);
                if (_config.Encryption.Mode == KeyExchangeMode.Ecdh) {
                    created.Ecdh = EcdhKeyExchange.Create();
                    created.EcdhPublicKey = EcdhKeyExchange.ExportPublicKey(created.Ecdh);
                }
                return created;
            });

            if (Volatile.Read(ref connection.Closed) == 1) {
                return;
            }

            if (!TryEstablishCrypto(connection, buffer, offset, clientKeyLength)) {
                _ = SafeSendRawAsync(type, peer, PeerProtocol.EncodeReject(PeerProtocol.RejectEncryptionMismatch));
                return;
            }

            connection.Links[type] = peer; // duplicate Hello (UDP resend) rebinds harmlessly
            peer.PeerLayerState = connection;
            _pendingPeers.TryRemove(peer, out _);

            if (type == TransportType.Udp && connection.Reliable == null && HasReliableUdpChannels()) {
                var capturedConnection = connection;
                connection.Reliable = new ReliableEndpoint(
                    _channels,
                    // Resolve the peer per send: a UDP link can be rebound by a
                    // duplicate Hello after the original endpoint mapping expired.
                    message => capturedConnection.Links.TryGetValue(TransportType.Udp, out var udpPeer)
                        ? _transports[TransportType.Udp].SendAsync(udpPeer, message)
                        : default,
                    (channel, payload) => DeliverIncoming(capturedConnection, channel, payload),
                    () => capturedConnection.GetPing(TransportType.Udp),
                    reason => CloseConnection(capturedConnection, reason, notifyLinks: true),
                    _logger);
                connection.Reliable.Start();
            }

            _ = SafeSendRawAsync(type, peer, PeerProtocol.EncodeWelcome(connection.Id, connection.EcdhPublicKey));

            if (connection.Links.Count == _transportTypes.Count &&
                Interlocked.Exchange(ref connection.Announced, 1) == 0) {
                _connections[connection.Id] = connection;
                _dispatcher.Dispatch(() => ConnectionOpened?.Invoke(connection));
            }
        }

        internal ValueTask SendAsync(NetConnection connection, byte channel, ReadOnlyMemory<byte> data, CancellationToken ct = default) {
            if (!_channels.TryGetValue(channel, out var channelConfig)) {
                throw new ArgumentException($"Channel {channel} is not configured", nameof(channel));
            }

            // Compress/encrypt first; reliability and transports see opaque bytes.
            data = ChannelTransform.Encode(channelConfig, connection.Crypto, data);

            if (channelConfig.Transport == TransportType.Udp && channelConfig.Reliability != ReliabilityMode.Unreliable) {
                var reliable = connection.Reliable
                    ?? throw new InvalidOperationException($"Connection#{connection.Id} has no UDP link");
                return channelConfig.Reliability == ReliabilityMode.UnreliableSequenced
                    ? reliable.SendSequencedAsync(channel, data)
                    : reliable.SendReliableAsync(channel, data, ct);
            }

            if (!connection.Links.TryGetValue(channelConfig.Transport, out var peer)) {
                throw new InvalidOperationException($"Connection#{connection.Id} has no {channelConfig.Transport} link");
            }
            // Cached 2-byte prefix + gather send: no per-send allocations at this layer.
            return _transports[channelConfig.Transport].SendAsync(peer, PeerProtocol.DataPrefix(channel), data, ct);
        }

        private bool HasReliableUdpChannels() {
            foreach (var channel in _channels.Values) {
                if (channel.Transport == TransportType.Udp && channel.Reliability != ReliabilityMode.Unreliable) {
                    return true;
                }
            }
            return false;
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

        internal async Task KickAsync(NetConnection connection) {
            foreach (var kvp in connection.Links) {
                await SafeSendRawAsync(kvp.Key, kvp.Value, PeerProtocol.EncodeBye(PeerProtocol.ByeGraceful)).ConfigureAwait(false);
            }
            CloseConnection(connection, DisconnectReason.Local, notifyLinks: true);
        }

        private async Task SafeSendRawAsync(TransportType type, TransportPeer peer, byte[] packet) {
            try {
                await _transports[type].SendAsync(peer, packet).ConfigureAwait(false);
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
            connection.Reliable?.Dispose();
            lock (connection.CryptoLock) {
                connection.Crypto?.Dispose();
                connection.Ecdh?.Dispose();
                connection.Ecdh = null;
            }

            foreach (var kvp in connection.Links) {
                kvp.Value.PeerLayerState = null;
                if (notifyLinks && _transports.TryGetValue(kvp.Key, out var transport)) {
                    transport.DisconnectPeer(kvp.Value);
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
                            _transports[entry.Type].DisconnectPeer(kvp.Key);
                        }
                    }

                    foreach (var kvp in _connectionsByToken) {
                        var connection = kvp.Value;
                        if (Volatile.Read(ref connection.Announced) == 0 &&
                            now - connection.CreatedAtMs > _config.ConnectTimeoutMs) {
                            _logger.Debug($"Closing {connection}: handshake incomplete within timeout");
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

            foreach (var connection in _connections.Values) {
                await KickAsync(connection).ConfigureAwait(false);
            }
            await StopTransportsAsync().ConfigureAwait(false);

            _pendingPeers.Clear();
            _connectionsByToken.Clear();
            _connections.Clear();
            Port = 0;
            _cts?.Dispose();
            _cts = null;
        }

        private async Task StopTransportsAsync() {
            foreach (var transport in _transports.Values) {
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
