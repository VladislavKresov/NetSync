using System;
using System.Buffers.Binary;
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
    // Connecting: links, Hello/Welcome on every link (keys, suite, server identity),
    // UDP fallback, then Join — the connect data the server application vets.
    public sealed partial class NetClient {
        private async Task ConnectCoreAsync(IPEndPoint endpoint, byte[] connectData, CancellationToken ct) {
            if (Interlocked.Exchange(ref _connectInProgress, 1) == 1) {
                throw new InvalidOperationException("Connect already in progress");
            }
            try {
                if (_session != null) {
                    throw new InvalidOperationException("Already connected; call Disconnect() first");
                }

                long deadline = NetTime.NowMs + _config.ConnectTimeoutMs;
                var session = new Session { Token = GenerateToken(), Endpoint = endpoint, ConnectData = connectData };
                var mode = _config.Encryption.Mode;
                if (mode != KeyExchangeMode.None) {
                    session.OfferedSuites = CipherSuites.LocalMask;
                }
                if (mode == KeyExchangeMode.Ecdh) {
                    session.Ecdh = EcdhKeyExchange.Create();
                    session.ClientPublicKey = EcdhKeyExchange.ExportPublicKey(session.Ecdh);
                }
                var hello = PeerProtocol.EncodeHello(session.Token, _configHash, mode, session.OfferedSuites, session.ClientPublicKey, cookie: null);
                session.Hellos[(int)TransportType.Tcp] = hello;
                session.Hellos[(int)TransportType.Udp] = hello;

                foreach (var type in _transportTypes) {
                    ITransport link;
                    try {
                        link = CreateTransport(type);
                    }
                    catch (Exception ex) when (type == TransportType.Udp && FallbackAllowed()) {
                        // e.g. WebAssembly: no sockets at all. Everything goes over the stream link.
                        _logger.Warning($"UDP is unavailable here ({ex.Message}); UDP channels will be carried over TCP");
                        session.UdpUnavailable = true;
                        continue;
                    }
                    var capturedType = type;
                    // Zero-copy hook on our own transports (buffer valid only during the
                    // call — HandleLinkData copies what it keeps).
                    if (link is IPooledReceiveTransport pooled) {
                        pooled.PooledReceive = (buffer, offset, count) => HandleLinkData(session, capturedType, buffer, offset, count);
                    }
                    else {
                        link.DataReceived += data => HandleLinkData(session, capturedType, data, 0, data.Length);
                    }
                    link.Disconnected += reason => OnLinkDisconnected(session, capturedType, reason);
                    session.Links[type] = link;
                    session.Handshakes[type] = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
                }

                if (session.Links.TryGetValue(TransportType.Udp, out var udpLink) && _hasReliableUdp) {
                    session.Reliable = new ReliableEndpoint(
                        _channels,
                        message => SendOnLinkAsync(session, TransportType.Udp, ReadOnlyMemory<byte>.Empty, message,
                                                   Sending.SendPriority.Normal, CancellationToken.None),
                        (channel, buffer, offset, count) => DeliverIncoming(session, channel, buffer, offset, count),
                        () => udpLink.PingMs,
                        reason => CloseSession(session, reason, raiseEvent: true),
                        _logger,
                        maxMessageSize: _config.WireMessageLimit);
                    if (udpLink is IDatagramBoundaryTransport bounded) {
                        var reliable = session.Reliable;
                        reliable.DeferAcks = true;
                        bounded.DatagramCompleted = reliable.FlushAcks;
                    }
                    session.Reliable.Start();
                }

                try {
                    // ConnectTimeoutMs covers everything, the links' own connects included.
                    using (var budget = CancellationTokenSource.CreateLinkedTokenSource(ct)) {
                        budget.CancelAfter(_config.ConnectTimeoutMs);
                        foreach (var kvp in session.Links) {
                            try {
                                await kvp.Value.ConnectAsync(endpoint, budget.Token).ConfigureAwait(false);
                            }
                            catch (OperationCanceledException) when (!ct.IsCancellationRequested) {
                                throw new TimeoutException($"Could not reach {endpoint} ({kvp.Key}) within {_config.ConnectTimeoutMs} ms");
                            }
                            catch (Exception ex) when (kvp.Key == TransportType.Udp && FallbackAllowed() && !(ex is OperationCanceledException)) {
                                _logger.Warning($"UDP link failed to start ({ex.Message}); UDP channels will be carried over TCP");
                                session.UdpUnavailable = true;
                            }
                        }
                    }

                    // TCP opens the connection; UDP joins once TCP is welcomed (the server only
                    // lets unspoofable links create connections). Without TCP, UDP goes alone.
                    if (session.Links.ContainsKey(TransportType.Tcp)) {
                        StartHelloLoop(session, TransportType.Tcp);
                    }
                    else {
                        StartHelloLoop(session, TransportType.Udp);
                    }

                    if (CanFallback(session)) {
                        await WaitForHandshakeWithFallbackAsync(session, deadline, ct).ConfigureAwait(false);
                    }
                    else {
                        var allWelcomed = Task.WhenAll(GetHandshakeTasks(session));
                        var finished = await Task.WhenAny(allWelcomed, Task.Delay(RemainingMs(deadline), ct)).ConfigureAwait(false);
                        if (finished != allWelcomed) {
                            ct.ThrowIfCancellationRequested();
                            throw new TimeoutException($"NetSync handshake timed out after {_config.ConnectTimeoutMs} ms");
                        }
                        await allWelcomed.ConfigureAwait(false); // propagate Reject / link failures
                    }

                    await JoinAsync(session, deadline, ct).ConfigureAwait(false);
                }
                catch {
                    CloseSession(session, DisconnectReason.Error, raiseEvent: false);
                    // Swallow late handshake faults so they don't surface as unobserved.
                    Task.WhenAll(GetHandshakeTasks(session)).ObserveFaults();
                    session.Joined.Task.ObserveFaults();
                    throw;
                }
            }
            finally {
                Volatile.Write(ref _connectInProgress, 0);
            }
        }

        /// <summary>
        /// Sends Join (connect data, encrypted when keyed) on the primary link until the server
        /// accepts or refuses. Duplicates are harmless: the server answers each with its decision.
        /// </summary>
        private async Task JoinAsync(Session session, long deadlineMs, CancellationToken ct) {
            var slot = session.Links.ContainsKey(TransportType.Tcp) ? TransportType.Tcp : TransportType.Udp;
            var join = BuildJoin(session);
            while (!session.Joined.Task.IsCompleted) {
                try {
                    await SendOnLinkAsync(session, slot, ReadOnlyMemory<byte>.Empty, join, Sending.SendPriority.High, ct).ConfigureAwait(false);
                }
                catch (Exception ex) when (!(ex is OperationCanceledException)) {
                    _logger.Debug($"Join send failed: {ex.Message}");
                }
                long remaining = deadlineMs - NetTime.NowMs;
                if (remaining <= 0) {
                    throw new TimeoutException($"The server did not accept the connection within {_config.ConnectTimeoutMs} ms");
                }
                await Task.WhenAny(session.Joined.Task, Task.Delay((int)Math.Min(JoinResendMs, remaining), ct)).ConfigureAwait(false);
                ct.ThrowIfCancellationRequested();
            }
            await session.Joined.Task.ConfigureAwait(false); // propagate a refusal
        }

        private static byte[] BuildJoin(Session session) {
            var data = session.ConnectData;
            if (session.Crypto == null) {
                var plain = new byte[1 + data.Length];
                plain[0] = PeerProtocol.MsgJoin;
                data.CopyTo(plain, 1);
                return plain;
            }
            var encrypted = new byte[1 + data.Length + session.Crypto.MaxOverhead];
            encrypted[0] = PeerProtocol.MsgJoin;
            int length = session.Crypto.Encrypt(data, encrypted.AsSpan(1));
            return encrypted.AsSpan(0, 1 + length).ToArray();
        }

        private bool CanFallback(Session session) {
            return FallbackAllowed() && session.Links.ContainsKey(TransportType.Tcp);
        }

        /// <summary>The configuration lets UDP channels ride TCP when UDP fails.</summary>
        private bool FallbackAllowed() {
            return !_config.RequireAllTransports &&
                   _transportTypes.Contains(TransportType.Tcp) &&
                   _transportTypes.Contains(TransportType.Udp);
        }

        private static int RemainingMs(long deadlineMs) => (int)Math.Max(0, deadlineMs - NetTime.NowMs);

        private async Task WaitForHandshakeWithFallbackAsync(Session session, long deadlineMs, CancellationToken ct) {
            // TCP is mandatory: it carries everything if UDP turns out to be blocked.
            var tcpHandshake = session.Handshakes[TransportType.Tcp].Task;
            var deadline = Task.Delay(RemainingMs(deadlineMs), ct);
            if (await Task.WhenAny(tcpHandshake, deadline).ConfigureAwait(false) != tcpHandshake) {
                ct.ThrowIfCancellationRequested();
                throw new TimeoutException($"NetSync handshake timed out after {_config.ConnectTimeoutMs} ms");
            }
            await tcpHandshake.ConfigureAwait(false); // propagate Reject

            // Stay well inside the server's half-open timeout (ConnectTimeoutMs).
            int fallbackMs = Math.Min(_config.TransportFallbackTimeoutMs, _config.ConnectTimeoutMs / 2);
            session.Handshakes.TryGetValue(TransportType.Udp, out var udpTcs);
            if (!session.UdpUnavailable && udpTcs != null) {
                var udpHandshake = udpTcs.Task;
                var fallback = Task.Delay(fallbackMs, ct);
                if (await Task.WhenAny(udpHandshake, fallback).ConfigureAwait(false) == udpHandshake &&
                    udpHandshake.Status == TaskStatus.RanToCompletion) {
                    return; // both links up — the normal case
                }
                ct.ThrowIfCancellationRequested();
            }

            // Degrade: mark first so the UDP link's teardown is not mistaken for a
            // connection loss, then tell the server over TCP and drop the UDP side.
            Volatile.Write(ref session.Degraded, 1);
            if (udpTcs != null) {
                udpTcs.TrySetCanceled(); // stops the UDP Hello loop
                udpTcs.Task.ObserveFaults();
            }
            await SendOnLinkAsync(session, TransportType.Tcp, ReadOnlyMemory<byte>.Empty, PeerProtocol.EncodeDegrade(),
                                  Sending.SendPriority.High, ct).ConfigureAwait(false);
            session.Reliable?.Dispose();
            if (session.Links.TryGetValue(TransportType.Udp, out var udpLink)) {
                try {
                    udpLink.Dispose();
                }
                catch (Exception ex) {
                    _logger.Debug($"UDP link dispose: {ex.Message}");
                }
            }
            if (!session.UdpUnavailable) {
                _logger.Warning($"UDP did not connect within {fallbackMs} ms; carrying UDP channels over TCP " +
                                "(set NetConfig.RequireAllTransports to fail instead)");
            }
        }

        private static Task[] GetHandshakeTasks(Session session) {
            var tasks = new Task[session.Handshakes.Count];
            int i = 0;
            foreach (var handshake in session.Handshakes.Values) {
                tasks[i++] = handshake.Task;
            }
            return tasks;
        }

        private void StartHelloLoop(Session session, TransportType type) {
            if (type == TransportType.Udp && (session.UdpUnavailable || Interlocked.Exchange(ref session.UdpHelloStarted, 1) == 1)) {
                return;
            }
            if (!session.Links.TryGetValue(type, out var link)) {
                return;
            }
            _ = Task.Run(() => SendHelloLoopAsync(session, type, link, session.Handshakes[type]));
        }

        private async Task SendHelloLoopAsync(Session session, TransportType type, ITransport link, TaskCompletionSource<bool> handshake) {
            try {
                // Hello is re-sent until Welcome arrives: mandatory on UDP (the datagram may be
                // lost), harmless on TCP (duplicates are idempotent). Re-read each round: a
                // server Retry may have given the UDP Hello a cookie.
                while (!handshake.Task.IsCompleted && !session.Cts.IsCancellationRequested) {
                    try {
                        await link.SendAsync(session.Hellos[(int)type]!).ConfigureAwait(false);
                    }
                    catch (Exception ex) {
                        _logger.Debug($"Hello send failed: {ex.Message}");
                    }
                    await Task.Delay(200, session.Cts.Token).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) { }
        }

        private void HandleWelcome(Session session, TransportType type, byte[] buffer, int offset, int count) {
            if (!PeerProtocol.TryParseWelcome(buffer, offset, count, out var welcome)) {
                return;
            }
            var mode = _config.Encryption.Mode;
            lock (session.CryptoLock) {
                if (session.ConnectionId == 0) {
                    session.ConnectionId = welcome.ConnectionId;
                }
                if (mode != KeyExchangeMode.None && session.Crypto == null) {
                    var failure = EstablishKeys(session, welcome);
                    if (failure != null) {
                        FailConnect(session, failure);
                        return;
                    }
                }
                else if (mode != KeyExchangeMode.None && welcome.Suite != session.Suite) {
                    return; // inconsistent with the first Welcome: not from our server
                }
            }
            if (session.Crypto != null && Volatile.Read(ref session.Security[(int)type]) == null) {
                Volatile.Write(ref session.Security[(int)type], session.Crypto.CreateLinkSecurity(type));
            }
            session.Handshakes[type].TrySetResult(true);
            if (type == TransportType.Tcp) {
                StartHelloLoop(session, TransportType.Udp); // the connection exists now: UDP may join it
            }
        }

        /// <summary>
        /// First Welcome: check the server's choice of suite, verify its identity, derive the
        /// session keys. Returns the reason to refuse the server, or null. Caller holds CryptoLock.
        /// </summary>
        private ConnectionRejectedException? EstablishKeys(Session session, PeerProtocol.Welcome welcome) {
            if (!CipherSuites.IsOffered(session.OfferedSuites, welcome.Suite)) {
                return new ConnectionRejectedException(ConnectionRejection.Encryption,
                    $"the server chose cipher suite {welcome.Suite}, which this client didn't offer");
            }
            if (_config.Encryption.Mode == KeyExchangeMode.PresharedKey) {
                session.Crypto = ConnectionCrypto.FromPresharedKey(_config.Encryption.PresharedKey!, session.Token, welcome.Suite, isClient: true);
                session.Suite = welcome.Suite;
                return null;
            }

            // ECDH
            if (welcome.PublicKey.Length != EcdhKeyExchange.PublicKeySize || session.Ecdh == null) {
                return new ConnectionRejectedException(ConnectionRejection.Encryption, "the server's Welcome did not carry a valid ECDH key");
            }
            if (welcome.IdentityKey.Length > 0 || welcome.Signature.Length > 0) {
                var transcript = PeerProtocol.IdentityTranscript(session.Token, _configHash, (byte)KeyExchangeMode.Ecdh, session.OfferedSuites,
                                                                 welcome.Suite, session.ClientPublicKey, welcome.PublicKey);
                if (!NetIdentity.IsSupported || !NetIdentity.Verify(welcome.IdentityKey, transcript, welcome.Signature)) {
                    return new ConnectionRejectedException(ConnectionRejection.ServerIdentity,
                        "the server's identity signature is invalid (possible man-in-the-middle)");
                }
                session.ServerIdentityKey = welcome.IdentityKey;
            }
            var trusted = _config.Encryption.TrustedServerKeys;
            if (trusted.Count > 0) {
                bool pinned = false;
                var presented = session.ServerIdentityKey;
                if (presented != null) {
                    foreach (var key in trusted) {
                        if (key.AsSpan().SequenceEqual(presented)) {
                            pinned = true;
                            break;
                        }
                    }
                }
                if (!pinned) {
                    return new ConnectionRejectedException(ConnectionRejection.ServerIdentity,
                        presented == null
                            ? "the server did not prove its identity, but TrustedServerKeys requires it (possible man-in-the-middle)"
                            : $"the server's identity {NetIdentity.GetFingerprint(presented)} is not in TrustedServerKeys (possible man-in-the-middle)");
                }
            }
            try {
                var secret = EcdhKeyExchange.DeriveSessionSecret(session.Ecdh, welcome.PublicKey, session.Token);
                session.Crypto = ConnectionCrypto.FromSecret(secret, welcome.Suite, isClient: true);
                Array.Clear(secret, 0, secret.Length);
            }
            catch (Exception ex) when (ex is CryptographicException || ex is ArgumentException) {
                return new ConnectionRejectedException(ConnectionRejection.Encryption, $"ECDH key derivation failed: {ex.Message}");
            }
            session.Ecdh.Dispose();
            session.Ecdh = null;
            session.Suite = welcome.Suite;
            return null;
        }

        private static ConnectionRejectedException RejectionFor(byte reason) {
            return reason switch {
                PeerProtocol.RejectVersionMismatch => new ConnectionRejectedException(ConnectionRejection.ProtocolVersion,
                    "protocol version mismatch (update client or server)"),
                PeerProtocol.RejectEncryptionMismatch => new ConnectionRejectedException(ConnectionRejection.Encryption,
                    "encryption mode mismatch (client and server NetConfig.Encryption must agree)"),
                PeerProtocol.RejectConfigMismatch => new ConnectionRejectedException(ConnectionRejection.Configuration,
                    "configuration mismatch: client and server must use identical channel tables " +
                    "(ids, transports, reliability, compression, encryption flags) and MaxMessageSize"),
                PeerProtocol.RejectAuthenticationFailed => new ConnectionRejectedException(ConnectionRejection.Authentication,
                    "authentication failed: the server could not verify our packets (different pre-shared keys?)"),
                _ => new ConnectionRejectedException(ConnectionRejection.Application, $"reason {reason}")
            };
        }

        /// <summary>The connect attempt fails with <paramref name="error"/> (every pending wait observes it).</summary>
        private static void FailConnect(Session session, Exception error) {
            foreach (var handshake in session.Handshakes.Values) {
                handshake.TrySetException(error);
            }
            session.Joined.TrySetException(error);
        }

        /// <summary>The server accepted us: the connection is open from here on.</summary>
        private void MarkJoined(Session session) {
            if (Volatile.Read(ref session.JoinedFlag) == 1 || Volatile.Read(ref session.Closed) == 1 ||
                Interlocked.Exchange(ref session.JoinedFlag, 1) == 1) {
                return;
            }
            Volatile.Write(ref session.Established, 1);
            _session = session;
            _lastEndpoint = session.Endpoint;
            _lastConnectData = session.ConnectData;
            _dispatcher.Dispatch(() => Connected?.Invoke());
            session.Joined.TrySetResult(true);
            StartSessionServices(session);
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
