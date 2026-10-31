using System;
using System.Collections.Generic;
using System.Net;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using NetSync.Diagnostics;
using NetSync.Internal;
using NetSync.Pipeline.Reliability;
using NetSync.Pipeline.Security;
using NetSync.Transports;

namespace NetSync.Peers {
    /// <summary>
    /// High-level client: connects all configured transports to one server and exposes
    /// them as channels. A random session token sent in the Hello message on every link
    /// lets the server group the links into a single logical connection.
    ///
    /// Connecting = handshake on every link (keys, cipher suite, server identity), then
    /// Join: the connect data goes to the server application, which accepts or refuses.
    ///
    /// Threading: with EventDelivery.Polled (default) call <see cref="PollEvents"/>
    /// regularly; with Immediate, handlers run on network threads.
    /// </summary>
    public sealed partial class NetClient : IDisposable, IDataEventSink {
        private const int JoinResendMs = 300;
        private const int TimeSyncBurst = 5;
        private const int TimeSyncBurstIntervalMs = 100;
        private const int TimeSyncIntervalMs = 2000;

        // Per-connect state. A fresh Session per ConnectAsync means events from a dying
        // previous connection can never leak into the current one.
        private sealed class Session {
            public readonly Dictionary<TransportType, ITransport> Links = new Dictionary<TransportType, ITransport>();
            public readonly Dictionary<TransportType, TaskCompletionSource<bool>> Handshakes = new Dictionary<TransportType, TaskCompletionSource<bool>>();
            public readonly TaskCompletionSource<bool> Joined = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            public readonly CancellationTokenSource Cts = new CancellationTokenSource();
            public readonly object CryptoLock = new object();
            public readonly byte[]?[] Hellos = new byte[2][]; // per slot: UDP may carry a server cookie
            public ReliableEndpoint? Reliable;
            /// <summary>Packet authenticator per link slot (encrypted connections only).</summary>
            public readonly LinkSecurity?[] Security = new LinkSecurity?[2];
            public ConnectionCrypto? Crypto;
            public ECDiffieHellman? Ecdh; // ephemeral, gone after key derivation
            public byte[]? ClientPublicKey;
            public byte OfferedSuites;
            public CipherSuite Suite;
            public byte[]? ServerIdentityKey;
            public byte[] ConnectData = Array.Empty<byte>();
            public IPEndPoint Endpoint = null!;
            public long Token;
            public long ConnectionId;
            public int UdpHelloStarted;
            public int JoinedFlag;
            public int Established; // set once the server accepted the Join
            public int Closed;
            public int Degraded;    // UDP given up: UDP channels ride the TCP link
            public bool UdpUnavailable; // the UDP link couldn't even be created/connected (browser, no sockets)
            public readonly TrafficCounters Traffic = new TrafficCounters();
            public readonly ClockSync Clock = new ClockSync();

            public bool IsDegraded => Volatile.Read(ref Degraded) == 1;

            /// <summary>The link that is live for this slot (Udp maps to Tcp when degraded).</summary>
            public bool IsLinkActive(TransportType type) => !(type == TransportType.Udp && IsDegraded);

            public bool AllWelcomed() {
                foreach (var kvp in Handshakes) {
                    if (IsLinkActive(kvp.Key) && !kvp.Value.Task.IsCompletedSuccessfully) {
                        return false;
                    }
                }
                return true;
            }
        }

        private readonly Dictionary<byte, ChannelConfig> _channels;
        private readonly List<TransportType> _transportTypes;
        private readonly NetConfig _config;
        private readonly TransportSettings _settings;
        private readonly uint _configHash;
        private readonly bool _hasReliableUdp;
        private readonly INetLogger _logger;
        private readonly EventDispatcher _dispatcher;
        private Session? _session;
        private int _connectInProgress;
        private IPEndPoint? _lastEndpoint;
        private byte[] _lastConnectData = Array.Empty<byte>();
        private CancellationTokenSource? _reconnectCts;

        /// <summary>Server-assigned connection id, 0 before the handshake completes.</summary>
        public long ConnectionId => _session?.ConnectionId ?? 0;

        public bool IsConnected {
            get {
                var session = _session;
                return session != null && Volatile.Read(ref session.Established) == 1;
            }
        }

        /// <summary>
        /// True when UDP could not be established and UDP channels are carried over TCP
        /// (see <see cref="NetConfig.RequireAllTransports"/>). Everything still works, but
        /// unreliable channels inherit TCP's head-of-line blocking.
        /// </summary>
        public bool IsDegraded => _session?.IsDegraded ?? false;

        /// <summary>Negotiated packet protection of the current connection.</summary>
        public CipherSuite CipherSuite => _session?.Suite ?? CipherSuite.None;

        /// <summary>
        /// The server's identity key, when it signed the handshake and the signature checked
        /// out (ECDH mode). Compare <see cref="NetIdentity.GetFingerprint"/> of it with a known
        /// value, or pin it in <see cref="EncryptionConfig.TrustedServerKeys"/>.
        /// </summary>
        public byte[]? ServerIdentityKey => _session?.ServerIdentityKey;

        /// <summary>
        /// The server's clock (<see cref="NetServer.TimeMs"/>) as estimated right now — a shared
        /// timeline for timestamps, interpolation and scheduling. NaN until the first
        /// synchronization (a few ms after connecting); refreshed every 2 seconds.
        /// </summary>
        public double ServerTimeMs {
            get {
                var session = _session;
                return session == null ? double.NaN : session.Clock.ServerTimeMs(NetTime.PreciseMs);
            }
        }

        /// <summary>Worst-case error of <see cref="ServerTimeMs"/> (half the best measured RTT); NaN when unknown.</summary>
        public double ServerTimeAccuracyMs => _session?.Clock.AccuracyMs ?? double.NaN;

        /// <summary>Traffic snapshot for the current connection (zeros when disconnected).</summary>
        public NetStatistics Statistics {
            get {
                var statistics = new NetStatistics { PingMs = PingMs, DroppedEvents = _dispatcher.DroppedEvents };
                var session = _session;
                if (session != null) {
                    statistics.IsDegraded = session.IsDegraded;
                    session.Traffic.CopyTo(statistics);
                    var reliable = session.Reliable;
                    if (reliable != null && !session.IsDegraded) {
                        statistics.Retransmissions = reliable.Retransmissions;
                        statistics.FastRetransmissions = reliable.FastRetransmissions;
                        statistics.SmoothedRttMs = reliable.SmoothedRttMs;
                    }
                }
                return statistics;
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
                foreach (var kvp in session.Links) {
                    if (!session.IsLinkActive(kvp.Key)) {
                        continue;
                    }
                    int ping = kvp.Value.PingMs;
                    if (ping >= 0 && (best < 0 || ping < best)) {
                        best = ping;
                    }
                }
                return best;
            }
        }

        public event Action? Connected;
        /// <summary>Auto-reconnect is about to make attempt N (see <see cref="NetConfig.Reconnect"/>).</summary>
        public event Action<int>? Reconnecting;
        /// <summary>Auto-reconnect gave up after MaxAttempts.</summary>
        public event Action? ReconnectFailed;
        public event Action<DisconnectReason>? Disconnected;

        /// <summary>
        /// Application data, zero-copy: (channel, data). The data is valid only until the
        /// handler returns — see <see cref="NetDataHandler"/>. The allocation-free way to
        /// receive; prefer it on hot paths.
        /// </summary>
        public event NetDataHandler? MessageReceived;

        /// <summary>
        /// Application data as an owned array: (channel, payload). Convenient, but each
        /// message is copied into a new array — use <see cref="MessageReceived"/> on hot paths.
        /// </summary>
        public event Action<byte, byte[]>? DataReceived;

        public NetClient(NetConfig config) {
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
        }

        public int GetPing(TransportType transport) {
            var session = _session;
            return session != null && session.IsLinkActive(transport) && session.Links.TryGetValue(transport, out var link)
                ? link.PingMs
                : -1;
        }

        public Task ConnectAsync(string host, int port, CancellationToken ct = default) {
            return ConnectAsync(host, port, ReadOnlyMemory<byte>.Empty, ct);
        }

        /// <param name="host">Host name or IP literal.</param>
        /// <param name="port">Server port.</param>
        /// <param name="connectData">
        /// Handed to the server's <see cref="NetServer.ConnectionApprover"/> (login token, game
        /// version…), up to 4 KB; encrypted whenever a key exchange is configured. Reused for
        /// automatic reconnects.
        /// </param>
        /// <param name="ct">Cancels the attempt.</param>
        public async Task ConnectAsync(string host, int port, ReadOnlyMemory<byte> connectData, CancellationToken ct = default) {
            if (IPAddress.TryParse(host, out var literal)) {
                await ConnectAsync(new IPEndPoint(literal, port), connectData, ct).ConfigureAwait(false);
                return;
            }
            // Try addresses in the order the OS prefers (RFC 6724). On IPv6-only networks
            // with NAT64 (common on mobile, required by App Store review) the usable
            // address is a synthesized IPv6 one — never force IPv4 first.
#if NET8_0_OR_GREATER
            var addresses = await Dns.GetHostAddressesAsync(host, ct).ConfigureAwait(false);
#else
            var addresses = await Dns.GetHostAddressesAsync(host).ConfigureAwait(false);
            ct.ThrowIfCancellationRequested();
#endif
            if (addresses.Length == 0) {
                throw new ArgumentException($"Could not resolve host '{host}'", nameof(host));
            }
            Exception? last = null;
            foreach (var address in addresses) {
                try {
                    await ConnectAsync(new IPEndPoint(address, port), connectData, ct).ConfigureAwait(false);
                    return;
                }
                catch (Exception ex) when (!(ex is OperationCanceledException) && !(ex is InvalidOperationException)) {
                    last = ex; // unreachable address family/route: try the next one
                }
            }
            throw last!;
        }

        public Task ConnectAsync(IPEndPoint endpoint, CancellationToken ct = default) {
            return ConnectAsync(endpoint, ReadOnlyMemory<byte>.Empty, ct);
        }

        /// <inheritdoc cref="ConnectAsync(string, int, ReadOnlyMemory{byte}, CancellationToken)"/>
        public Task ConnectAsync(IPEndPoint endpoint, ReadOnlyMemory<byte> connectData, CancellationToken ct = default) {
            if (connectData.Length > PeerProtocol.MaxConnectDataSize) {
                throw new ArgumentException($"Connect data is limited to {PeerProtocol.MaxConnectDataSize} bytes", nameof(connectData));
            }
            StopReconnecting();
            return ConnectCoreAsync(endpoint, connectData.ToArray(), ct);
        }

        /// <summary>
        /// Sends on a channel. <paramref name="data"/> is read only before this returns:
        /// the buffer may be reused (or returned to a pool) right after the call, without
        /// awaiting.
        /// </summary>
        public ValueTask SendAsync(byte channel, ReadOnlyMemory<byte> data, CancellationToken ct = default) {
            var session = _session ?? throw new InvalidOperationException("Not connected");
            var channelConfig = _config.ResolveOutgoing(_channels, channel, data.Length);
            session.Traffic.OnSent(data.Length);
            if (!channelConfig.NeedsTransform) {
                return Route(session, channel, channelConfig, data, ct);
            }
            // Compress/encrypt first; reliability and transports see opaque bytes.
            var encoded = ChannelTransform.Encode(channelConfig, session.Crypto, data.Span, out int length);
            try {
                return Route(session, channel, channelConfig, new ReadOnlyMemory<byte>(encoded, 0, length), ct);
            }
            finally {
                Pooled.Return(encoded); // every send path consumes the bytes before returning
            }
        }

        private ValueTask Route(Session session, byte channel, ChannelConfig channelConfig, ReadOnlyMemory<byte> data, CancellationToken ct) {
            var slot = session.IsLinkActive(channelConfig.Transport) ? channelConfig.Transport : TransportType.Tcp;
            if (slot == TransportType.Udp && channelConfig.Reliability != ReliabilityMode.Unreliable) {
                var reliable = session.Reliable ?? throw new InvalidOperationException("Not connected");
                return reliable.SendAsync(channel, data, ct);
            }
            // Cached 2-byte prefix + gather send: no per-send allocations at this layer.
            return SendOnLinkAsync(session, slot, PeerProtocol.DataPrefix(channel), data,
                                   PriorityMapping.ToSendPriority(channelConfig.Priority), ct);
        }

        /// <summary>
        /// Single exit for post-handshake traffic: wraps it in the authenticated envelope
        /// on encrypted connections, applies priority when the transport supports it.
        /// </summary>
        private ValueTask SendOnLinkAsync(Session session, TransportType slot, ReadOnlyMemory<byte> prefix,
                                          ReadOnlyMemory<byte> payload, Sending.SendPriority priority, CancellationToken ct) {
            var link = session.Links[slot];
            if (session.Crypto == null) {
                return SendRaw(link, prefix, payload, priority, ct);
            }
            var security = Volatile.Read(ref session.Security[(int)slot])
                ?? throw new InvalidOperationException($"{slot} link has no authentication keys yet");
            var envelope = security.WrapPooled(prefix.Span, payload.Span, out int length);
            try {
                return SendRaw(link, ReadOnlyMemory<byte>.Empty, new ReadOnlyMemory<byte>(envelope, 0, length), priority, ct);
            }
            finally {
                Pooled.Return(envelope); // transports consume the bytes before returning
            }
        }

        private static ValueTask SendRaw(ITransport link, ReadOnlyMemory<byte> prefix, ReadOnlyMemory<byte> payload,
                                         Sending.SendPriority priority, CancellationToken ct) {
            return link is IPrioritizedTransport prioritized
                ? prioritized.SendAsync(prefix, payload, priority, ct)
                : link.SendAsync(prefix, payload, ct);
        }

        /// <summary>Drains queued events (Polled mode). Returns the number of events handled.</summary>
        public int PollEvents(int maxEvents = int.MaxValue) => _dispatcher.Poll(maxEvents);

        public void Disconnect() {
            StopReconnecting();
            var session = _session;
            if (session == null) {
                return;
            }
            SendByeBestEffort(session);
            CloseSession(session, DisconnectReason.Local, raiseEvent: true);
        }

        public void Dispose() => Disconnect();

        private ITransport CreateTransport(TransportType type) {
            return _config.GetTransportFactory(type).CreateClient(_settings);
        }

        // buffer may be a pooled/transport-owned array: valid only during this call,
        // so anything kept (the payload) is copied here — exactly once per packet.
        private void HandleLinkData(Session session, TransportType type, byte[] buffer, int offset, int count) {
            if (count == 0 || Volatile.Read(ref session.Closed) == 1 || !session.IsLinkActive(type)) {
                return;
            }

            byte first = buffer[offset];
            bool handshakeMessage = first == PeerProtocol.MsgWelcome || first == PeerProtocol.MsgReject || first == PeerProtocol.MsgRetry;
            if (_config.Encryption.Mode != KeyExchangeMode.None && !handshakeMessage) {
                // Encrypted connection: after the handshake only authenticated envelopes count.
                if (first != PeerProtocol.MsgSecure ||
                    Volatile.Read(ref session.Security[(int)type]) is not { } security ||
                    !security.TryUnwrap(buffer, offset, count, out offset, out count)) {
                    return; // forged, replayed, tampered or unauthenticated: drop silently
                }
                byte inner = buffer[offset];
                if (inner == PeerProtocol.MsgWelcome || inner == PeerProtocol.MsgReject ||
                    inner == PeerProtocol.MsgRetry || inner == PeerProtocol.MsgSecure) {
                    return; // never valid inside an envelope
                }
            }

            switch (buffer[offset]) {
                case PeerProtocol.MsgWelcome:
                    HandleWelcome(session, type, buffer, offset, count);
                    break;

                case PeerProtocol.MsgRetry:
                    if (type == TransportType.Udp && count >= 1 + PeerProtocol.CookieSize && !session.Handshakes[type].Task.IsCompleted) {
                        // UDP-only server asks us to prove our address: echo its cookie.
                        session.Hellos[(int)type] = PeerProtocol.EncodeHello(
                            session.Token, _configHash, _config.Encryption.Mode, session.OfferedSuites, session.ClientPublicKey,
                            new ReadOnlySpan<byte>(buffer, offset + 1, PeerProtocol.CookieSize).ToArray());
                    }
                    break;

                case PeerProtocol.MsgReject:
                    if (Volatile.Read(ref session.Established) == 0) { // plaintext: only believed while connecting
                        FailConnect(session, RejectionFor(count > 1 ? buffer[offset + 1] : (byte)0));
                    }
                    break;

                case PeerProtocol.MsgJoinAccept:
                    MarkJoined(session);
                    break;

                case PeerProtocol.MsgJoinReject:
                    FailConnect(session, new ConnectionRejectedException(ConnectionRejection.Application,
                                                                         PeerProtocol.DecodeJoinReject(buffer, offset, count)));
                    break;

                case PeerProtocol.MsgBye:
                    bool kicked = count > 1 && buffer[offset + 1] == PeerProtocol.ByeKicked;
                    CloseSession(session, kicked ? DisconnectReason.Kicked : DisconnectReason.Remote, raiseEvent: true);
                    break;

                case PeerProtocol.MsgData:
                    if (count >= PeerProtocol.DataHeaderSize && (Volatile.Read(ref session.Established) == 1 || session.AllWelcomed())) {
                        MarkJoined(session); // the server only sends data to accepted clients
                        DeliverIncoming(session, buffer[offset + 1], buffer,
                                        offset + PeerProtocol.DataHeaderSize, count - PeerProtocol.DataHeaderSize);
                    }
                    break;

                case PeerProtocol.MsgRelData:
                    if (Volatile.Read(ref session.Established) == 0 && session.AllWelcomed()) {
                        MarkJoined(session);
                    }
                    session.Reliable?.HandleRelData(buffer, offset, count);
                    break;

                case PeerProtocol.MsgRelAck:
                    session.Reliable?.HandleAck(buffer, offset, count);
                    break;

                case PeerProtocol.MsgTimeResponse:
                    if (count >= PeerProtocol.TimeResponseSize) {
                        session.Clock.AddSample(PeerProtocol.ReadTime(buffer.AsSpan(offset + 1, 8)),
                                                PeerProtocol.ReadTime(buffer.AsSpan(offset + 9, 8)),
                                                NetTime.PreciseMs);
                    }
                    break;
            }
        }

        // buffer[offset..offset+count) is only valid during this call.
        private void DeliverIncoming(Session session, byte channel, byte[] buffer, int offset, int count) {
            if (!_channels.TryGetValue(channel, out var channelConfig)) {
                return; // unknown channel — config mismatch, drop
            }
            if (count > _config.MaxMessageSize + ChannelTransform.MaxOverhead) {
                _logger.Error($"Server sent a {count}-byte message, above MaxMessageSize ({_config.MaxMessageSize}); disconnecting");
                CloseSession(session, DisconnectReason.Error, raiseEvent: true);
                return;
            }
            if (!channelConfig.NeedsTransform) {
                session.Traffic.OnReceived(count);
                DispatchDataEvent(channel, buffer, offset, count);
                return;
            }
            var decoded = ChannelTransform.Decode(channelConfig, session.Crypto, buffer, offset, count,
                                                  _config.MaxMessageSize, _logger, out int length);
            if (decoded == null) {
                return;
            }
            try {
                session.Traffic.OnReceived(length);
                DispatchDataEvent(channel, decoded, 0, length);
            }
            finally {
                Pooled.Return(decoded);
            }
        }

        private void DispatchDataEvent(byte channel, byte[] buffer, int offset, int count) {
            if (_dispatcher.IsImmediate) {
                RaiseData(channel, buffer, offset, count); // straight from the network buffer
            }
            else {
                _dispatcher.QueueData(null, channel, buffer, offset, count);
            }
        }

        void IDataEventSink.RaiseQueuedData(object? source, byte channel, byte[] buffer, int length) {
            RaiseData(channel, buffer, 0, length);
        }

        private void RaiseData(byte channel, byte[] buffer, int offset, int count) {
            var zeroCopy = MessageReceived;
            if (zeroCopy != null) {
                try {
                    zeroCopy(channel, new ReadOnlyMemory<byte>(buffer, offset, count));
                }
                catch (Exception ex) {
                    _logger.Error($"Unhandled exception in MessageReceived handler: {ex}");
                }
            }
            var copying = DataReceived;
            if (copying != null) {
                try {
                    copying(channel, Pooled.ToArray(buffer, offset, count));
                }
                catch (Exception ex) {
                    _logger.Error($"Unhandled exception in DataReceived handler: {ex}");
                }
            }
        }

        private void OnLinkDisconnected(Session session, TransportType type, DisconnectReason reason) {
            if (Volatile.Read(ref session.Closed) == 1 || !session.IsLinkActive(type)) {
                return;
            }
            session.Handshakes[type].TrySetException(
                new System.IO.IOException($"{type} link lost during handshake: {reason}"));
            if (type == TransportType.Udp && Volatile.Read(ref session.Established) == 0 && CanFallback(session)) {
                return; // still connecting: the fallback logic decides whether to degrade
            }
            session.Joined.TrySetException(new System.IO.IOException($"{type} link lost while joining: {reason}"));
            // One link down takes the whole logical connection down: channels assume
            // every configured transport is available.
            CloseSession(session, reason, raiseEvent: true);
        }

        private void SendByeBestEffort(Session session) {
            // UDP links get an explicit Bye so the server drops the peer instantly
            // instead of waiting out the ping timeout. TCP links skip it: closing the
            // socket delivers FIN, which the server sees immediately anyway.
            if (session.IsLinkActive(TransportType.Udp) && session.Links.ContainsKey(TransportType.Udp)) {
                try {
                    SendOnLinkAsync(session, TransportType.Udp, ReadOnlyMemory<byte>.Empty, PeerProtocol.EncodeBye(PeerProtocol.ByeGraceful),
                                    Sending.SendPriority.High, CancellationToken.None).AsTask().Wait(100);
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
            foreach (var security in session.Security) {
                security?.Dispose();
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
                var policy = _config.Reconnect;
                var endpoint = _lastEndpoint;
                if (policy != null && endpoint != null && reason != DisconnectReason.Local && reason != DisconnectReason.Kicked) {
                    StartReconnecting(endpoint, policy);
                }
            }
        }
    }
}
