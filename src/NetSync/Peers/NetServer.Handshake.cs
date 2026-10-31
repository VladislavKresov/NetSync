using System;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using NetSync.Diagnostics;
using NetSync.Internal;
using NetSync.Pipeline.Reliability;
using NetSync.Pipeline.Security;
using NetSync.Transports;

namespace NetSync.Peers {
    // Opening connections: Hello on every link (who may create a connection, cookies,
    // keys, the signed Welcome), the UDP→TCP degrade, then Join and the application's
    // approval within MaxConnections.
    public sealed partial class NetServer {
        /// <summary>
        /// A packet on a bound link failed authentication. Before the connection is open this
        /// almost always means the client keyed differently (wrong PSK): say so in plaintext
        /// instead of letting it time out — once, then drop the attempt. After opening, it's
        /// an attack or corruption and is ignored.
        /// </summary>
        private void OnUnverifiablePacket(TransportType type, TransportPeer peer, NetConnection connection) {
            if (Volatile.Read(ref connection.Announced) == 1 || Interlocked.Exchange(ref connection.AuthRejectSent, 1) == 1) {
                return;
            }
            _logger.Warning($"{connection}: packets fail authentication — client and server keys differ (pre-shared key?)");
            _ = SafeSendRawAsync(peer, PeerProtocol.EncodeReject(PeerProtocol.RejectAuthenticationFailed));
            _ = CloseAfterFlushAsync(connection, DisconnectReason.Error);
        }

        private void HandleHello(TransportType type, TransportPeer peer, byte[] buffer, int offset, int count) {
            if (!PeerProtocol.TryReadHelloVersion(new ReadOnlySpan<byte>(buffer, offset, count), out byte version)) {
                return;
            }
            if (version != PeerProtocol.ProtocolVersion) {
                _logger.Warning($"Rejecting {peer.EndPoint}: protocol version {version}, expected {PeerProtocol.ProtocolVersion}");
                _ = SafeSendRawAsync(peer, PeerProtocol.EncodeReject(PeerProtocol.RejectVersionMismatch));
                return;
            }
            if (!PeerProtocol.TryParseHello(buffer, offset, count, out var hello) || hello.Token == 0) {
                return;
            }
            if (hello.ConfigHash != _configHash) {
                _logger.Warning($"Rejecting {peer.EndPoint}: configuration mismatch (channel table or MaxMessageSize differ)");
                _ = SafeSendRawAsync(peer, PeerProtocol.EncodeReject(PeerProtocol.RejectConfigMismatch));
                return;
            }

            var mode = _config.Encryption.Mode;
            var suite = mode == KeyExchangeMode.None ? CipherSuite.None : CipherSuites.Choose(hello.Suites);
            bool keyExchangeOk = hello.KeyExchange == (byte)mode &&
                                 (mode == KeyExchangeMode.None || suite != CipherSuite.None) &&
                                 (mode != KeyExchangeMode.Ecdh || hello.PublicKeyLength == EcdhKeyExchange.PublicKeySize);
            if (!keyExchangeOk) {
                _logger.Warning($"Rejecting {peer.EndPoint}: encryption mismatch (client key exchange {hello.KeyExchange}, " +
                                $"server {(byte)mode}, offered suites 0x{hello.Suites:X2})");
                _ = SafeSendRawAsync(peer, PeerProtocol.EncodeReject(PeerProtocol.RejectEncryptionMismatch));
                return;
            }

            if (!_connectionsByToken.TryGetValue(hello.Token, out var connection)) {
                if (!MayCreateConnection(type, peer, hello, buffer)) {
                    return;
                }
                lock (_createLock) {
                    if (!_connectionsByToken.TryGetValue(hello.Token, out connection)) {
                        connection = CreateConnection(hello, suite, buffer);
                        if (connection == null) {
                            _ = SafeSendRawAsync(peer, PeerProtocol.EncodeReject(PeerProtocol.RejectEncryptionMismatch));
                            return;
                        }
                        _connectionsByToken[hello.Token] = connection;
                    }
                }
            }

            if (Volatile.Read(ref connection.Closed) == 1 || (type == TransportType.Udp && connection.IsDegraded)) {
                return;
            }
            if (connection.ClientPublicKey != null &&
                !new ReadOnlySpan<byte>(buffer, hello.PublicKeyOffset, hello.PublicKeyLength).SequenceEqual(connection.ClientPublicKey)) {
                return; // same token, different key: not the client that opened this connection
            }

            if (connection.Links.TryGetValue(type, out var boundPeer) && !ReferenceEquals(boundPeer, peer)) {
                // The session token travels in plaintext; anyone who saw it could otherwise
                // redirect this link to their own address. A live link is never re-bound.
                _logger.Warning($"Ignoring Hello for {connection} from {peer.EndPoint}: its {type} link is already bound to {boundPeer.EndPoint}");
                return;
            }
            connection.Links[type] = peer; // duplicate Hello from the same peer (UDP resend) is idempotent
            if (connection.Crypto != null && connection.Security[(int)type] == null) {
                connection.Security[(int)type] = connection.Crypto.CreateLinkSecurity(type);
            }
            peer.PeerLayerState = connection;
            _pendingPeers.TryRemove(peer, out _);
            MarkTrusted(peer);

            if (type == TransportType.Udp && connection.Reliable == null && _hasReliableUdp) {
                var capturedConnection = connection;
                connection.Reliable = new ReliableEndpoint(
                    _channels,
                    message => SendToLinkAsync(capturedConnection, TransportType.Udp, ReadOnlyMemory<byte>.Empty, message,
                                               Sending.SendPriority.Normal, CancellationToken.None),
                    (channel, segment, segmentOffset, segmentCount) => DeliverIncoming(capturedConnection, channel, segment, segmentOffset, segmentCount),
                    () => capturedConnection.GetPing(TransportType.Udp),
                    reason => CloseConnection(capturedConnection, reason, notifyLinks: true),
                    _logger,
                    maxMessageSize: _config.WireMessageLimit);
                // The peer's own transport reports datagram boundaries (see CreateServerTransport).
                connection.Reliable.DeferAcks = peer.Owner is IDatagramBoundaryServerTransport;
                connection.Reliable.Start();
            }

            _ = SafeSendRawAsync(peer, connection.WelcomeMessage);
        }

        /// <summary>
        /// Who may start a connection: links whose source address can't be forged. Stream
        /// slots (TCP) qualify by their own handshake. A datagram Hello only joins a
        /// connection a stream link created — or, on UDP-only servers, proves its address
        /// by echoing a cookie (no keys are generated for spoofed packets).
        /// </summary>
        private bool MayCreateConnection(TransportType type, TransportPeer peer, PeerProtocol.Hello hello, byte[] buffer) {
            if (_connectionsByToken.Count - _connections.Count >= MaxPendingConnections) {
                _logger.Warning($"Ignoring Hello from {peer.EndPoint}: {MaxPendingConnections} handshakes already pending");
                return false;
            }
            if (type != TransportType.Udp) {
                return true;
            }
            if (_transportTypes.Contains(TransportType.Tcp)) {
                return false; // the client's TCP Hello creates the connection; UDP joins it
            }
            long epoch = NetTime.NowMs / CookieEpochMs;
            if (hello.CookieLength == PeerProtocol.CookieSize) {
                var presented = new ReadOnlySpan<byte>(buffer, hello.CookieOffset, hello.CookieLength);
                if (ConnectionCrypto.FixedTimeEquals(presented, ComputeCookie(peer, hello.Token, epoch)) ||
                    ConnectionCrypto.FixedTimeEquals(presented, ComputeCookie(peer, hello.Token, epoch - 1))) {
                    return true;
                }
            }
            _ = SafeSendRawAsync(peer, PeerProtocol.EncodeRetry(ComputeCookie(peer, hello.Token, epoch)));
            return false;
        }

        private byte[] ComputeCookie(TransportPeer peer, long token, long epoch) {
            var address = peer.EndPoint.Address.GetAddressBytes();
            var input = new byte[8 + 8 + 2 + address.Length];
            System.Buffers.Binary.BinaryPrimitives.WriteInt64BigEndian(input.AsSpan(0), epoch);
            System.Buffers.Binary.BinaryPrimitives.WriteInt64BigEndian(input.AsSpan(8), token);
            System.Buffers.Binary.BinaryPrimitives.WriteUInt16BigEndian(input.AsSpan(16), (ushort)peer.EndPoint.Port);
            address.CopyTo(input, 18);
            using var hmac = new HMACSHA256(_cookieKey);
            var mac = hmac.ComputeHash(input);
            return mac.AsSpan(0, PeerProtocol.CookieSize).ToArray();
        }

        /// <summary>
        /// Builds a pending connection from its first Hello: keys (PSK or ECDH), the chosen
        /// suite, and the Welcome every link will get — signed when the server has an identity.
        /// Returns null when the client's key material is unusable.
        /// </summary>
        private NetConnection? CreateConnection(PeerProtocol.Hello hello, CipherSuite suite, byte[] buffer) {
            long id = Interlocked.Increment(ref _nextConnectionId);
            var connection = new NetConnection(this, id, hello.Token, NetTime.NowMs) { CipherSuite = suite };
            byte[]? serverPublicKey = null;
            byte[]? identityKey = null;
            byte[]? signature = null;
            try {
                switch (_config.Encryption.Mode) {
                    case KeyExchangeMode.PresharedKey:
                        connection.Crypto = ConnectionCrypto.FromPresharedKey(_config.Encryption.PresharedKey!, hello.Token, suite, isClient: false);
                        break;

                    case KeyExchangeMode.Ecdh: {
                        var clientPublicKey = new ReadOnlySpan<byte>(buffer, hello.PublicKeyOffset, hello.PublicKeyLength).ToArray();
                        using var ephemeral = EcdhKeyExchange.Create();
                        serverPublicKey = EcdhKeyExchange.ExportPublicKey(ephemeral);
                        var secret = EcdhKeyExchange.DeriveSessionSecret(ephemeral, clientPublicKey, hello.Token);
                        try {
                            connection.Crypto = ConnectionCrypto.FromSecret(secret, suite, isClient: false);
                        }
                        finally {
                            Array.Clear(secret, 0, secret.Length);
                        }
                        connection.ClientPublicKey = clientPublicKey;
                        var identity = _config.Encryption.ServerIdentity;
                        if (identity != null) {
                            var transcript = PeerProtocol.IdentityTranscript(hello.Token, hello.ConfigHash, hello.KeyExchange, hello.Suites,
                                                                             suite, clientPublicKey, serverPublicKey);
                            signature = identity.Sign(transcript);
                            identityKey = identity.PublicKey;
                        }
                        break;
                    }
                }
            }
            catch (Exception ex) when (ex is CryptographicException || ex is ArgumentException) {
                _logger.Warning($"Handshake key setup failed: {ex.Message}");
                connection.Crypto?.Dispose();
                return null;
            }
            connection.WelcomeMessage = PeerProtocol.EncodeWelcome(id, suite, serverPublicKey, identityKey, signature);
            return connection;
        }

        /// <summary>The client gave up on UDP: drop any half-bound UDP side, carry everything over TCP.</summary>
        private void HandleDegrade(NetConnection connection) {
            if (_config.RequireAllTransports || Interlocked.Exchange(ref connection.Degraded, 1) == 1) {
                if (_config.RequireAllTransports) {
                    _logger.Warning($"{connection} asked to run without UDP, but RequireAllTransports is set; closing");
                    CloseConnection(connection, DisconnectReason.Error, notifyLinks: true);
                }
                return;
            }
            _logger.Info($"{connection} could not establish UDP; its UDP channels are carried over TCP");

            connection.Reliable?.Dispose();
            if (connection.Links.TryRemove(TransportType.Udp, out var udpPeer)) {
                udpPeer.PeerLayerState = null;
                udpPeer.Owner?.DisconnectPeer(udpPeer);
            }
        }

        private bool AllLinksBound(NetConnection connection) {
            return connection.IsDegraded
                ? connection.Links.ContainsKey(TransportType.Tcp)
                : connection.Links.Count == _transportTypes.Count;
        }

        private void HandleJoin(TransportType type, NetConnection connection, byte[] buffer, int offset, int count) {
            if (Volatile.Read(ref connection.Closed) == 1 || !AllLinksBound(connection)) {
                return; // links still binding; the client resends Join
            }
            switch (Volatile.Read(ref connection.JoinState)) {
                case NetConnection.JoinAccepted:
                    _ = SafeSendAsync(connection, type, PeerProtocol.JoinAccept); // our accept was lost: repeat it
                    return;
                case NetConnection.JoinNone:
                    break;
                default:
                    return; // approval running, or already rejected
            }
            if (Interlocked.CompareExchange(ref connection.JoinState, NetConnection.JoinPending, NetConnection.JoinNone) != NetConnection.JoinNone) {
                return;
            }

            var body = new ReadOnlySpan<byte>(buffer, offset + 1, count - 1);
            byte[] connectData;
            if (connection.Crypto != null) {
                if (body.Length > PeerProtocol.MaxConnectDataSize + connection.Crypto.MaxOverhead) {
                    RejectJoin(connection, type, "connect data too large");
                    return;
                }
                var plain = Pooled.Rent(body.Length);
                try {
                    int length = connection.Crypto.Decrypt(body, plain);
                    if (length < 0) {
                        RejectJoin(connection, type, "connect data could not be decrypted");
                        return;
                    }
                    connectData = plain.AsSpan(0, length).ToArray();
                }
                finally {
                    Pooled.Return(plain);
                }
            }
            else {
                if (body.Length > PeerProtocol.MaxConnectDataSize) {
                    RejectJoin(connection, type, "connect data too large");
                    return;
                }
                connectData = body.ToArray();
            }
            _ = ApproveAsync(connection, type, connectData);
        }

        private async Task ApproveAsync(NetConnection connection, TransportType type, byte[] connectData) {
            ConnectionDecision decision;
            try {
                if (!TryTakeSlot(connection)) {
                    decision = ConnectionDecision.Reject("server full");
                }
                else if (ConnectionApprover is { } approver) {
                    decision = await approver(new ConnectionRequest(connection, connectData)).ConfigureAwait(false);
                }
                else {
                    decision = ConnectionDecision.Accept();
                }
            }
            catch (Exception ex) {
                _logger.Error($"ConnectionApprover threw for {connection}: {ex}");
                decision = ConnectionDecision.Reject("server error while approving the connection");
            }

            if (Volatile.Read(ref connection.Closed) == 1) {
                return; // timed out or the client left while we were deciding
            }
            if (!decision.IsAccepted) {
                RejectJoin(connection, type, decision.RejectReason ?? "rejected");
                return;
            }

            Volatile.Write(ref connection.JoinState, NetConnection.JoinAccepted);
            Volatile.Write(ref connection.Announced, 1);
            _connections[connection.Id] = connection;
            // Opened is raised (or queued) before the client learns it's in, so nothing the
            // client sends can be delivered ahead of it.
            _dispatcher.Dispatch(() => ConnectionOpened?.Invoke(connection));
            await SafeSendAsync(connection, type, PeerProtocol.JoinAccept).ConfigureAwait(false);
        }

        /// <summary>
        /// Reserves a MaxConnections slot before the approval runs: simultaneous joins can't all
        /// pass the limit check and then all open. Released when the connection closes.
        /// </summary>
        private bool TryTakeSlot(NetConnection connection) {
            if (_config.MaxConnections <= 0) {
                return true;
            }
            if (Interlocked.Increment(ref _slotsTaken) > _config.MaxConnections) {
                Interlocked.Decrement(ref _slotsTaken);
                return false;
            }
            Volatile.Write(ref connection.HoldsSlot, 1);
            if (Volatile.Read(ref connection.Closed) == 1) {
                ReleaseSlot(connection); // closed meanwhile: CloseConnection may have missed the slot
            }
            return true;
        }

        private void ReleaseSlot(NetConnection connection) {
            if (Interlocked.Exchange(ref connection.HoldsSlot, 0) == 1) {
                Interlocked.Decrement(ref _slotsTaken);
            }
        }

        private void RejectJoin(NetConnection connection, TransportType type, string reason) {
            Volatile.Write(ref connection.JoinState, NetConnection.JoinRejected);
            _logger.Info($"Refusing {connection}: {reason}");
            _ = SafeSendAsync(connection, type, PeerProtocol.EncodeJoinReject(reason));
            _ = CloseAfterFlushAsync(connection, DisconnectReason.Local);
        }

        /// <summary>Gives a final notice (reject) time to leave the send queue before the links close.</summary>
        private async Task CloseAfterFlushAsync(NetConnection connection, DisconnectReason reason) {
            await Task.Delay(RejectFlushDelayMs).ConfigureAwait(false);
            CloseConnection(connection, reason, notifyLinks: true);
        }

        private static void MarkTrusted(TransportPeer peer) {
            if (peer.Owner != null && Unwrap(peer.Owner) is ITrustAwareServerTransport trustAware) {
                trustAware.MarkTrusted(peer); // keeps e.g. the untrusted-peer counter exact
            }
            else {
                peer.MarkTrusted();
            }
        }
    }
}
