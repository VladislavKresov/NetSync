using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Net;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using NetSync.Diagnostics;
using NetSync.Pipeline.Reliability;
using NetSync.Pipeline.Security;
using NetSync.Transports;
using NetSync.Transports.Tcp;
using NetSync.Transports.Udp;

namespace NetSync.Peers {
    /// <summary>
    /// High-level client: connects all configured transports to one server in parallel
    /// and exposes them as channels. A random session token sent in the Hello message
    /// on every link lets the server group the links into a single logical connection.
    ///
    /// Threading: with EventDelivery.Polled (default) call <see cref="PollEvents"/>
    /// regularly; with Immediate, handlers run on network threads.
    /// </summary>
    public sealed class NetClient : IDisposable {
        // Per-connect state. A fresh Session per ConnectAsync means events from a dying
        // previous connection can never leak into the current one.
        private sealed class Session {
            public readonly Dictionary<TransportType, ITransport> Links = new Dictionary<TransportType, ITransport>();
            public readonly Dictionary<TransportType, TaskCompletionSource<bool>> Handshakes = new Dictionary<TransportType, TaskCompletionSource<bool>>();
            public readonly CancellationTokenSource Cts = new CancellationTokenSource();
            public readonly object CryptoLock = new object();
            public ReliableEndpoint? Reliable;
            public ConnectionCrypto? Crypto;
            public System.Security.Cryptography.ECDiffieHellman? Ecdh; // ephemeral, gone after key derivation
            public byte[] HelloMessage = Array.Empty<byte>();
            public long Token;
            public long ConnectionId;
            public int Established; // set once every link got its Welcome
            public int Closed;

            public bool AllWelcomed() {
                foreach (var handshake in Handshakes.Values) {
                    if (!handshake.Task.IsCompletedSuccessfully) {
                        return false;
                    }
                }
                return true;
            }
        }

        private readonly Dictionary<byte, ChannelConfig> _channels;
        private readonly List<TransportType> _transportTypes;
        private readonly NetConfig _config;
        private readonly INetLogger _logger;
        private readonly EventDispatcher _dispatcher;
        private Session? _session;
        private int _connectInProgress;

        /// <summary>Server-assigned connection id, 0 before the handshake completes.</summary>
        public long ConnectionId => _session?.ConnectionId ?? 0;

        public bool IsConnected {
            get {
                var session = _session;
                return session != null && Volatile.Read(ref session.Established) == 1;
            }
        }

        /// <summary>Lowest RTT across connected links, -1 when unknown.</summary>
        public int PingMs {
            get {
                var session = _session;
                if (session == null) {
                    return -1;
                }
                int best = -1;
                foreach (var link in session.Links.Values) {
                    int ping = link.PingMs;
                    if (ping >= 0 && (best < 0 || ping < best)) {
                        best = ping;
                    }
                }
                return best;
            }
        }

        public event Action? Connected;
        public event Action<DisconnectReason>? Disconnected;
        /// <summary>Application data: (channel, payload).</summary>
        public event Action<byte, byte[]>? DataReceived;

        public NetClient(NetConfig config) {
            _config = config ?? throw new ArgumentNullException(nameof(config));
            _logger = config.Logger;
            _channels = config.BuildChannelTable();
            config.ValidateSecurity(_channels);
            _transportTypes = config.BuildTransportList(_channels);
            _dispatcher = new EventDispatcher(config.EventDelivery, _logger);
        }

        public int GetPing(TransportType transport) {
            var session = _session;
            return session != null && session.Links.TryGetValue(transport, out var link) ? link.PingMs : -1;
        }

        public async Task ConnectAsync(string host, int port, CancellationToken ct = default) {
            if (!IPAddress.TryParse(host, out var address)) {
                var addresses = await Dns.GetHostAddressesAsync(host).ConfigureAwait(false);
                if (addresses.Length == 0) {
                    throw new ArgumentException($"Could not resolve host '{host}'", nameof(host));
                }
                address = Array.Find(addresses, a => a.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork) ?? addresses[0];
            }
            await ConnectAsync(new IPEndPoint(address, port), ct).ConfigureAwait(false);
        }

        public async Task ConnectAsync(IPEndPoint endpoint, CancellationToken ct = default) {
            if (Interlocked.Exchange(ref _connectInProgress, 1) == 1) {
                throw new InvalidOperationException("Connect already in progress");
            }
            try {
                if (_session != null) {
                    throw new InvalidOperationException("Already connected; call Disconnect() first");
                }

                var session = new Session { Token = GenerateToken() };

                switch (_config.Encryption.Mode) {
                    case KeyExchangeMode.PresharedKey:
                        session.Crypto = ConnectionCrypto.FromPresharedKey(_config.Encryption.PresharedKey!, session.Token);
                        session.HelloMessage = PeerProtocol.EncodeHello(session.Token, KeyExchangeMode.PresharedKey);
                        break;
                    case KeyExchangeMode.Ecdh:
                        session.Ecdh = EcdhKeyExchange.Create();
                        session.HelloMessage = PeerProtocol.EncodeHello(session.Token, KeyExchangeMode.Ecdh,
                            EcdhKeyExchange.ExportPublicKey(session.Ecdh));
                        break;
                    default:
                        session.HelloMessage = PeerProtocol.EncodeHello(session.Token);
                        break;
                }

                foreach (var type in _transportTypes) {
                    var link = CreateTransport(type);
                    var capturedType = type;
                    // Zero-copy hook on our own transports (buffer valid only during the
                    // call — HandleLinkData copies what it keeps).
                    switch (link) {
                        case TcpTransport tcp:
                            tcp.PooledReceive = (buffer, offset, count) => HandleLinkData(session, capturedType, buffer, offset, count);
                            break;
                        case UdpTransport udp:
                            udp.PooledReceive = (buffer, offset, count) => HandleLinkData(session, capturedType, buffer, offset, count);
                            break;
                        default:
                            link.DataReceived += data => HandleLinkData(session, capturedType, data, 0, data.Length);
                            break;
                    }
                    link.Disconnected += reason => OnLinkDisconnected(session, capturedType, reason);
                    session.Links[type] = link;
                    session.Handshakes[type] = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
                }

                if (session.Links.TryGetValue(TransportType.Udp, out var udpLink) && HasReliableUdpChannels()) {
                    session.Reliable = new ReliableEndpoint(
                        _channels,
                        message => udpLink.SendAsync(message),
                        (channel, payload) => DeliverIncoming(session, channel, payload),
                        () => udpLink.PingMs,
                        reason => CloseSession(session, reason, raiseEvent: true),
                        _logger);
                    session.Reliable.Start();
                }

                try {
                    foreach (var link in session.Links.Values) {
                        await link.ConnectAsync(endpoint, ct).ConfigureAwait(false);
                    }

                    // Hello is re-sent until Welcome arrives: mandatory on UDP (the
                    // datagram may be lost), harmless on TCP (duplicates are idempotent).
                    foreach (var kvp in session.Links) {
                        _ = Task.Run(() => SendHelloLoopAsync(session, kvp.Value, session.Handshakes[kvp.Key]));
                    }

                    var allWelcomed = Task.WhenAll(GetHandshakeTasks(session));
                    var finished = await Task.WhenAny(allWelcomed, Task.Delay(_config.ConnectTimeoutMs, ct)).ConfigureAwait(false);
                    if (finished != allWelcomed) {
                        ct.ThrowIfCancellationRequested();
                        throw new TimeoutException($"NetSync handshake timed out after {_config.ConnectTimeoutMs} ms");
                    }
                    await allWelcomed.ConfigureAwait(false); // propagate Reject / link failures

                    Volatile.Write(ref session.Established, 1);
                    _session = session;
                    _dispatcher.Dispatch(() => Connected?.Invoke());
                }
                catch {
                    CloseSession(session, DisconnectReason.Error, raiseEvent: false);
                    // Swallow late handshake faults so they don't surface as unobserved.
                    _ = Task.WhenAll(GetHandshakeTasks(session)).ContinueWith(
                        t => _ = t.Exception, TaskContinuationOptions.OnlyOnFaulted);
                    throw;
                }
            }
            finally {
                Volatile.Write(ref _connectInProgress, 0);
            }
        }

        public ValueTask SendAsync(byte channel, ReadOnlyMemory<byte> data, CancellationToken ct = default) {
            var session = _session ?? throw new InvalidOperationException("Not connected");
            if (!_channels.TryGetValue(channel, out var channelConfig)) {
                throw new ArgumentException($"Channel {channel} is not configured", nameof(channel));
            }

            // Compress/encrypt first; reliability and transports see opaque bytes.
            data = ChannelTransform.Encode(channelConfig, session.Crypto, data);

            if (channelConfig.Transport == TransportType.Udp && channelConfig.Reliability != ReliabilityMode.Unreliable) {
                var reliable = session.Reliable ?? throw new InvalidOperationException("Not connected");
                return channelConfig.Reliability == ReliabilityMode.UnreliableSequenced
                    ? reliable.SendSequencedAsync(channel, data)
                    : reliable.SendReliableAsync(channel, data, ct);
            }

            var link = session.Links[channelConfig.Transport];
            // Cached 2-byte prefix + gather send: no per-send allocations at this layer.
            return link.SendAsync(PeerProtocol.DataPrefix(channel), data, ct);
        }

        /// <summary>Drains queued events (Polled mode). Returns the number of events handled.</summary>
        public int PollEvents(int maxEvents = int.MaxValue) => _dispatcher.Poll(maxEvents);

        public void Disconnect() {
            var session = _session;
            if (session == null) {
                return;
            }
            SendByeBestEffort(session);
            CloseSession(session, DisconnectReason.Local, raiseEvent: true);
        }

        public void Dispose() => Disconnect();

        private ITransport CreateTransport(TransportType type) {
            return type switch {
                TransportType.Tcp => new TcpTransport(_config.PingIntervalMs, _config.PingTimeoutMs, _logger),
                TransportType.Udp => new UdpTransport(_config.PingIntervalMs, _config.PingTimeoutMs, _logger),
                _ => throw new NotSupportedException($"Transport {type} is not supported")
            };
        }

        private static Task[] GetHandshakeTasks(Session session) {
            var tasks = new Task[session.Handshakes.Count];
            int i = 0;
            foreach (var handshake in session.Handshakes.Values) {
                tasks[i++] = handshake.Task;
            }
            return tasks;
        }

        private async Task SendHelloLoopAsync(Session session, ITransport link, TaskCompletionSource<bool> handshake) {
            var hello = session.HelloMessage;
            try {
                while (!handshake.Task.IsCompleted && !session.Cts.IsCancellationRequested) {
                    try {
                        await link.SendAsync(hello).ConfigureAwait(false);
                    }
                    catch (Exception ex) {
                        _logger.Debug($"Hello send failed: {ex.Message}");
                    }
                    await Task.Delay(200, session.Cts.Token).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) { }
        }

        // buffer may be a pooled/transport-owned array: valid only during this call,
        // so anything kept (the payload) is copied here — exactly once per packet.
        private void HandleLinkData(Session session, TransportType type, byte[] buffer, int offset, int count) {
            if (count == 0 || Volatile.Read(ref session.Closed) == 1) {
                return;
            }

            switch (buffer[offset]) {
                case PeerProtocol.MsgWelcome:
                    if (count >= 9) {
                        session.ConnectionId = BinaryPrimitives.ReadInt64BigEndian(buffer.AsSpan(offset + 1, 8));
                        if (_config.Encryption.Mode == KeyExchangeMode.Ecdh &&
                            !TryDeriveEcdhKey(session, buffer, offset, count)) {
                            session.Handshakes[type].TrySetException(
                                new InvalidOperationException("Server Welcome did not carry a valid ECDH public key"));
                            break;
                        }
                        session.Handshakes[type].TrySetResult(true);
                    }
                    break;

                case PeerProtocol.MsgReject: {
                    byte reason = count > 1 ? buffer[offset + 1] : (byte)0;
                    string detail = reason switch {
                        PeerProtocol.RejectVersionMismatch => "protocol version mismatch",
                        PeerProtocol.RejectEncryptionMismatch => "encryption mode mismatch (client and server NetConfig.Encryption must agree)",
                        _ => $"reason {reason}"
                    };
                    session.Handshakes[type].TrySetException(
                        new InvalidOperationException($"Server rejected the connection: {detail}"));
                    break;
                }

                case PeerProtocol.MsgBye:
                    CloseSession(session, DisconnectReason.Remote, raiseEvent: true);
                    break;

                case PeerProtocol.MsgData:
                    if (count >= PeerProtocol.DataHeaderSize && session.AllWelcomed()) {
                        byte channel = buffer[offset + 1];
                        var payload = new byte[count - PeerProtocol.DataHeaderSize];
                        Array.Copy(buffer, offset + PeerProtocol.DataHeaderSize, payload, 0, payload.Length);
                        DeliverIncoming(session, channel, payload);
                    }
                    break;

                case PeerProtocol.MsgRelData:
                    session.Reliable?.HandleRelData(buffer, offset, count);
                    break;

                case PeerProtocol.MsgRelAck:
                    session.Reliable?.HandleAck(buffer, offset, count);
                    break;
            }
        }

        private bool TryDeriveEcdhKey(Session session, byte[] buffer, int offset, int count) {
            lock (session.CryptoLock) {
                if (session.Crypto != null) {
                    return true; // the other link's Welcome already derived it
                }
                if (count < 10 || session.Ecdh == null) {
                    return false;
                }
                int keyLength = buffer[offset + 9];
                if (keyLength != EcdhKeyExchange.PublicKeySize || count < 10 + keyLength) {
                    return false;
                }
                try {
                    var secret = EcdhKeyExchange.DeriveSessionSecret(
                        session.Ecdh, buffer.AsSpan(offset + 10, keyLength), session.Token);
                    session.Crypto = ConnectionCrypto.FromSecret(secret);
                    Array.Clear(secret, 0, secret.Length);
                }
                catch (Exception ex) {
                    _logger.Error($"ECDH key derivation failed: {ex.Message}");
                    return false;
                }
                session.Ecdh.Dispose();
                session.Ecdh = null;
                return true;
            }
        }

        private void DeliverIncoming(Session session, byte channel, byte[] payload) {
            if (!_channels.TryGetValue(channel, out var channelConfig)) {
                return; // unknown channel — config mismatch, drop
            }
            var decoded = ChannelTransform.Decode(channelConfig, session.Crypto, payload, _logger);
            if (decoded != null) {
                DispatchDataEvent(channel, decoded);
            }
        }

        private void DispatchDataEvent(byte channel, byte[] payload) {
            if (_dispatcher.IsImmediate) {
                // Direct invoke: no closure allocation on the hot path.
                try {
                    DataReceived?.Invoke(channel, payload);
                }
                catch (Exception ex) {
                    _logger.Error($"Unhandled exception in DataReceived handler: {ex}");
                }
            }
            else {
                _dispatcher.Dispatch(() => DataReceived?.Invoke(channel, payload));
            }
        }

        private bool HasReliableUdpChannels() {
            foreach (var channel in _channels.Values) {
                if (channel.Transport == TransportType.Udp && channel.Reliability != ReliabilityMode.Unreliable) {
                    return true;
                }
            }
            return false;
        }

        private void OnLinkDisconnected(Session session, TransportType type, DisconnectReason reason) {
            if (Volatile.Read(ref session.Closed) == 1) {
                return;
            }
            session.Handshakes[type].TrySetException(
                new System.IO.IOException($"{type} link lost during handshake: {reason}"));
            // One link down takes the whole logical connection down: channels assume
            // every configured transport is available.
            CloseSession(session, reason, raiseEvent: true);
        }

        private void SendByeBestEffort(Session session) {
            // UDP links get an explicit Bye so the server drops the peer instantly
            // instead of waiting out the ping timeout. TCP links skip it: closing the
            // socket delivers FIN, which the server sees immediately anyway.
            if (session.Links.TryGetValue(TransportType.Udp, out var udpLink)) {
                try {
                    udpLink.SendAsync(PeerProtocol.EncodeBye(PeerProtocol.ByeGraceful)).AsTask().Wait(100);
                }
                catch { }
            }
        }

        private void CloseSession(Session session, DisconnectReason reason, bool raiseEvent) {
            if (Interlocked.Exchange(ref session.Closed, 1) == 1) {
                return;
            }
            bool wasEstablished = Volatile.Read(ref session.Established) == 1;
            if (ReferenceEquals(_session, session)) {
                _session = null;
            }

            try {
                session.Cts.Cancel();
            }
            catch { }
            session.Reliable?.Dispose();
            lock (session.CryptoLock) {
                session.Crypto?.Dispose();
                session.Ecdh?.Dispose();
                session.Ecdh = null;
            }
            foreach (var link in session.Links.Values) {
                try {
                    link.Dispose();
                }
                catch (Exception ex) {
                    _logger.Debug($"Link dispose: {ex.Message}");
                }
            }
            session.Cts.Dispose();

            if (raiseEvent && wasEstablished) {
                _dispatcher.Dispatch(() => Disconnected?.Invoke(reason));
            }
        }

        private static long GenerateToken() {
            Span<byte> bytes = stackalloc byte[8];
            long token;
            do {
                RandomNumberGenerator.Fill(bytes);
                token = BinaryPrimitives.ReadInt64BigEndian(bytes);
            } while (token == 0);
            return token;
        }
    }
}
