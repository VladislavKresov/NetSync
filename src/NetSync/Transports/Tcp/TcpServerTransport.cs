using System;
using System.Collections.Concurrent;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using NetSync.Diagnostics;
using NetSync.Internal;
using NetSync.Pipeline.Fragmentation;
using NetSync.Sending;

namespace NetSync.Transports.Tcp {
    /// <summary>
    /// Server-side TCP transport: accepts clients, frames packets, pings peers and
    /// drops the ones that stop answering.
    ///
    /// Every peer gets its own priority <see cref="SendQueue"/>: sends never block the
    /// caller on a slow client, keepalive jumps ahead of bulk data, and a client that
    /// stops reading is detected by the write watchdog instead of hanging the server.
    /// </summary>
    public sealed class TcpServerTransport : IServerTransport, IPeerFlushingServerTransport, IPrioritizedServerTransport, IPooledReceiveServerTransport, ITrustAwareServerTransport {
        private sealed class PeerState {
            public readonly TcpClient Client;
            public readonly NetworkStream Stream;
            public readonly SendQueue SendQueue;
            public readonly FragmentBuffer FragmentBuffer;

            public PeerState(TcpClient client, TransportSettings settings) {
                Client = client;
                Stream = client.GetStream();
                SendQueue = new SendQueue(WriteRawAsync, TcpTransport.CoalesceBytes, settings.Logger);
                FragmentBuffer = TcpTransport.CreateFragmentBuffer(settings.MaxMessageSize);
            }

            private ValueTask WriteRawAsync(byte[] buffer, int length, CancellationToken ct) {
                return Stream.WriteAsync(buffer.AsMemory(0, length), ct);
            }
        }

        private readonly TransportSettings _settings;
        private readonly INetLogger _logger;
        private readonly ConcurrentDictionary<long, TransportPeer> _peers = new ConcurrentDictionary<long, TransportPeer>();
        private TcpListener? _listener;
        private CancellationTokenSource? _cts;
        private int _running;

        public int Port { get; private set; }
        public bool IsRunning => _running == 1;

        public event Action<TransportPeer>? PeerConnected;
        public event Action<TransportPeer, DisconnectReason>? PeerDisconnected;
        public event Action<TransportPeer, byte[]>? DataReceived;

        /// <summary>Zero-copy delivery hook, see <see cref="TcpTransport.PooledReceive"/>.</summary>
        internal Action<TransportPeer, byte[], int, int>? PooledReceive { get; set; }

        Action<TransportPeer, byte[], int, int>? IPooledReceiveServerTransport.PooledReceive {
            get => PooledReceive;
            set => PooledReceive = value;
        }

        bool ITrustAwareServerTransport.RequireTrust {
            get => RequireTrust;
            set => RequireTrust = value;
        }

        void ITrustAwareServerTransport.MarkTrusted(TransportPeer peer) => peer.MarkTrusted();

        /// <summary>
        /// When set (by NetServer), peers that have not completed the handshake may only
        /// send frames up to <see cref="PacketProtocol.MaxUntrustedFrameBody"/> bytes.
        /// </summary>
        internal bool RequireTrust { get; set; }

        public TcpServerTransport(int pingIntervalMs = 1000, int pingTimeoutMs = 5000, INetLogger? logger = null)
            : this(TransportSettings.From(pingIntervalMs, pingTimeoutMs, logger)) { }

        public TcpServerTransport(TransportSettings settings) {
            _settings = settings ?? throw new ArgumentNullException(nameof(settings));
            _logger = settings.Logger;
        }

        public Task<int> StartAsync(int port, CancellationToken ct = default) {
            if (Interlocked.Exchange(ref _running, 1) == 1) {
                throw new InvalidOperationException("Server transport already running");
            }

            try {
                _listener = DualStack.StartTcpListener(port);
            }
            catch {
                Volatile.Write(ref _running, 0);
                throw;
            }
            _cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            Port = ((IPEndPoint)_listener.LocalEndpoint).Port;

            var token = _cts.Token;
            var listener = _listener;
            _ = Task.Run(() => TcpAcceptLoop.RunAsync(listener, client => ServeAsync(client, token), _logger, nameof(TcpServerTransport), token));
            _ = Task.Run(() => PingLoopAsync(token));

            _logger.Info($"TcpServerTransport listening on port {Port}");
            return Task.FromResult(Port);
        }

        private Task ServeAsync(TcpClient tcpClient, CancellationToken ct) {
            tcpClient.NoDelay = true;
            var endPoint = DualStack.Normalize((IPEndPoint)tcpClient.Client.RemoteEndPoint!); // throws if already reset: the loop drops it
            var state = new PeerState(tcpClient, _settings);
            var peer = new TransportPeer(endPoint, NetTime.NowMs) { TransportState = state };
            state.SendQueue.Start();
            _peers[peer.Id] = peer;
            PeerConnected?.Invoke(peer);
            return ReceiveLoopAsync(peer, state, ct);
        }

        private async Task ReceiveLoopAsync(TransportPeer peer, PeerState state, CancellationToken ct) {
            var reason = DisconnectReason.Remote;
            var reader = new TcpFrameReader();
            try {
                while (!ct.IsCancellationRequested) {
                    int read = await state.Stream.ReadAsync(reader.GetReadMemory(), ct).ConfigureAwait(false);
                    if (read == 0) {
                        break;
                    }
                    peer.Touch(NetTime.NowMs);
                    reader.Advance(read);
                    if (!ProcessFrames(peer, state, reader, ref reason)) {
                        break;
                    }
                }
            }
            catch (OperationCanceledException) {
                reason = DisconnectReason.Local;
            }
            catch (Exception ex) when (ex is IOException || ex is SocketException || ex is ObjectDisposedException) {
                reason = ct.IsCancellationRequested ? DisconnectReason.Local : DisconnectReason.Remote;
            }
            catch (Exception ex) {
                _logger.Error($"TcpServerTransport receive loop ({peer}): {ex}");
                reason = DisconnectReason.Error;
            }
            finally {
                RemovePeer(peer, reason);
                reader.Dispose();
            }
        }

        /// <summary>Handles every complete buffered frame. False = protocol violation, drop the peer.</summary>
        private bool ProcessFrames(TransportPeer peer, PeerState state, TcpFrameReader reader, ref DisconnectReason reason) {
            while (true) {
                // Re-evaluated per frame: the Hello inside this very batch may make the peer trusted.
                int limit = RequireTrust && !peer.IsTrusted
                    ? PacketProtocol.MaxUntrustedFrameBody
                    : PacketProtocol.MaxTcpFrameBody;
                var kind = reader.TryRead(limit, out int offset, out int length, out long timestamp);
                switch (kind) {
                    case TcpFrameReader.FrameKind.None:
                        return true;

                    case TcpFrameReader.FrameKind.Ping: {
                        var pong = PacketProtocol.RentPongPacket(timestamp);
                        if (!state.SendQueue.TryEnqueue(pong, PacketProtocol.PingPacketSize, pooled: true, SendPriority.Critical)) {
                            PacketProtocol.Return(pong);
                        }
                        break;
                    }

                    case TcpFrameReader.FrameKind.Pong:
                        peer.SetPing((int)Math.Max(0, NetTime.NowMs - timestamp));
                        break;

                    case TcpFrameReader.FrameKind.Data:
                        if (!DeliverPayload(peer, state, reader.Buffer, offset, length)) {
                            _logger.Warning($"TcpServerTransport: fragment limits violated by {peer}; dropping peer");
                            reason = DisconnectReason.Error;
                            return false;
                        }
                        break;

                    default:
                        _logger.Warning($"TcpServerTransport: invalid frame (type or length {offset}, limit {limit}) from {peer}; dropping peer");
                        reason = DisconnectReason.Error;
                        return false;
                }
            }
        }

        // buffer is pooled and only valid during this call. Returns false on a limit violation.
        private bool DeliverPayload(TransportPeer peer, PeerState state, byte[] buffer, int offset, int length) {
            var span = new ReadOnlySpan<byte>(buffer, offset, length);
            if (PacketFragmenter.TryParse(span, out uint seqId, out uint index, out uint total, out var chunk)) {
                if (!state.FragmentBuffer.TryAddFragmentPooled(seqId, index, total, chunk, out var reassembled, out int reassembledLength)) {
                    return false;
                }
                if (reassembled != null) {
                    try {
                        Deliver(peer, reassembled, 0, reassembledLength);
                    }
                    finally {
                        Pooled.Return(reassembled);
                    }
                }
                return true;
            }
            Deliver(peer, buffer, offset, length);
            return true;
        }

        private void Deliver(TransportPeer peer, byte[] buffer, int offset, int length) {
            ReceivedPacket.Deliver(PooledReceive, DataReceived, peer, buffer, offset, length); // zero-copy: valid only during the call
        }

        private async Task PingLoopAsync(CancellationToken ct) {
            try {
                while (!ct.IsCancellationRequested) {
                    long now = NetTime.NowMs;
                    foreach (var kvp in _peers) {
                        var peer = kvp.Value;
                        if (peer.TransportState is not PeerState state) {
                            continue;
                        }

                        // Enqueue, never write: one slow client must not stall everyone's keepalive.
                        if (!RequireTrust || peer.IsTrusted) {
                            var ping = PacketProtocol.RentPingPacket(now);
                            if (!state.SendQueue.TryEnqueue(ping, PacketProtocol.PingPacketSize, pooled: true, SendPriority.Critical)) {
                                PacketProtocol.Return(ping);
                            }
                        }

                        if (_settings.PingTimeoutMs > 0 && now - peer.LastSeenMs > _settings.PingTimeoutMs) {
                            RemovePeer(peer, DisconnectReason.Timeout);
                            continue;
                        }
                        long writeStarted = state.SendQueue.WriteStartedAtMs;
                        if (_settings.WriteTimeoutMs > 0 && writeStarted != 0 && now - writeStarted > _settings.WriteTimeoutMs) {
                            _logger.Warning($"TcpServerTransport: {peer} stopped reading (write stalled); dropping peer");
                            RemovePeer(peer, DisconnectReason.Timeout);
                        }
                    }
                    await Task.Delay(_settings.PingIntervalMs, ct).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) { }
        }

        public ValueTask SendAsync(TransportPeer peer, ReadOnlyMemory<byte> data, CancellationToken ct = default) {
            return SendAsync(peer, ReadOnlyMemory<byte>.Empty, data, ct);
        }

        public ValueTask SendAsync(TransportPeer peer, ReadOnlyMemory<byte> prefix, ReadOnlyMemory<byte> payload, CancellationToken ct = default) {
            return SendAsync(peer, prefix, payload, SendPriority.Normal, ct);
        }

        public ValueTask SendAsync(TransportPeer peer, ReadOnlyMemory<byte> prefix, ReadOnlyMemory<byte> payload, SendPriority priority, CancellationToken ct = default) {
            if (peer.TransportState is not PeerState state) {
                throw new ArgumentException("Peer does not belong to this transport", nameof(peer));
            }
            if (priority == SendPriority.Critical) {
                priority = SendPriority.High; // Critical is reserved for keepalive
            }

            int totalLength = prefix.Length + payload.Length;
            if (totalLength >= PacketFragmenter.MaxUnfragmentedSize) {
                return BulkSender.SendAsync(state.SendQueue, prefix, payload, PriorityMapping.ForFragments(priority), null, lengthPrefixed: true, ct);
            }

            var frame = PacketProtocol.RentTcpDataFrame(prefix.Span, payload.Span, out int length);
            if (!state.SendQueue.TryEnqueue(frame, length, pooled: true, priority)) {
                PacketProtocol.Return(frame);
                throw new InvalidOperationException($"Send queue overflow for {peer} (the client is not keeping up)");
            }
            return default;
        }

        System.Threading.Tasks.Task IPeerFlushingServerTransport.FlushPeerAsync(TransportPeer peer, int timeoutMs) {
            return peer.TransportState is PeerState state ? state.SendQueue.WaitUntilIdleAsync(timeoutMs) : System.Threading.Tasks.Task.CompletedTask;
        }

        public void DisconnectPeer(TransportPeer peer) => RemovePeer(peer, DisconnectReason.Local);

        private void RemovePeer(TransportPeer peer, DisconnectReason reason) {
            if (!_peers.TryRemove(peer.Id, out _)) {
                return; // already removed — event fires once
            }
            if (peer.TransportState is PeerState state) {
                try {
                    try {
                        if (state.Client.Connected) {
                            state.Client.Client.Shutdown(SocketShutdown.Both);
                        }
                    }
                    catch { }
                    // Socket first: unblocks a write stuck on a client that stopped reading.
                    state.Client.Close();
                    state.SendQueue.Dispose();
                    state.FragmentBuffer.Clear();
                }
                catch (Exception ex) {
                    _logger.Debug($"TcpServerTransport peer close: {ex.Message}");
                }
            }
            PeerDisconnected?.Invoke(peer, reason);
        }

        public async Task StopAsync() {
            if (Interlocked.Exchange(ref _running, 0) == 0) {
                return;
            }
            _cts?.Cancel();

            foreach (var kvp in _peers) {
                RemovePeer(kvp.Value, DisconnectReason.Local);
            }

            try {
                _listener?.Stop();
            }
            catch { }
            _listener = null;
            Port = 0;
            _cts?.Dispose();
            _cts = null;
            await Task.CompletedTask;
        }

        public void Dispose() => StopAsync().GetAwaiter().GetResult();
    }
}
