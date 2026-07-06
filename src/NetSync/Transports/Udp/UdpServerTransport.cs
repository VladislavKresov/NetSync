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
    /// Server-side UDP transport. Tracks peers by source endpoint, pings them actively
    /// and expires the silent ones.
    ///
    /// Fixes over 1.x: the server now *sends* pings (before, a peer's LastPongTime was
    /// set once at creation and never updated, so with a timeout configured every UDP
    /// client would eventually be dropped as dead); liveness uses any received packet.
    /// </summary>
    public sealed class UdpServerTransport : IServerTransport {
        private readonly int _pingIntervalMs;
        private readonly int _pingTimeoutMs;
        private readonly INetLogger _logger;
        private readonly ConcurrentDictionary<string, TransportPeer> _peersByEndpoint = new ConcurrentDictionary<string, TransportPeer>();
        private UdpClient? _udpClient;
        private CancellationTokenSource? _cts;
        private int _running;

        public int Port { get; private set; }
        public bool IsRunning => _running == 1;

        public event Action<TransportPeer>? PeerConnected;
        public event Action<TransportPeer, DisconnectReason>? PeerDisconnected;
        public event Action<TransportPeer, byte[]>? DataReceived;

        /// <summary>Zero-copy delivery hook, see <see cref="Tcp.TcpTransport.PooledReceive"/>.</summary>
        internal Action<TransportPeer, byte[], int, int>? PooledReceive;

        public UdpServerTransport(int pingIntervalMs = 1000, int pingTimeoutMs = 5000, INetLogger? logger = null) {
            _pingIntervalMs = pingIntervalMs;
            _pingTimeoutMs = pingTimeoutMs;
            _logger = logger ?? NullNetLogger.Instance;
        }

        public Task<int> StartAsync(int port, CancellationToken ct = default) {
            if (Interlocked.Exchange(ref _running, 1) == 1) {
                throw new InvalidOperationException("Server transport already running");
            }

            _cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            _udpClient = new UdpClient(port);
            // Default OS buffers (~64 KB) drop datagrams under bursts; a server fans
            // in traffic from many clients and needs the headroom even more.
            _udpClient.Client.ReceiveBufferSize = 1 << 20;
            _udpClient.Client.SendBufferSize = 1 << 20;
            Port = ((IPEndPoint)_udpClient.Client.LocalEndPoint!).Port;

            var token = _cts.Token;
            _ = Task.Run(() => ReceiveLoopAsync(token));
            _ = Task.Run(() => PingLoopAsync(token));

            _logger.Info($"UdpServerTransport listening on port {Port}");
            return Task.FromResult(Port);
        }

        private async Task ReceiveLoopAsync(CancellationToken ct) {
            var udp = _udpClient!;
            while (!ct.IsCancellationRequested) {
                try {
                    var result = await udp.ReceiveAsync().ConfigureAwait(false);
                    var buf = result.Buffer;
                    if (buf.Length == 0) {
                        continue;
                    }

                    var remote = result.RemoteEndPoint;
                    string key = remote.ToString();
                    long now = NetTime.NowMs;

                    var peer = _peersByEndpoint.GetOrAdd(key, _ => {
                        var newPeer = new TransportPeer(remote, now);
                        PeerConnected?.Invoke(newPeer);
                        return newPeer;
                    });
                    peer.Touch(now);

                    switch ((PacketType)buf[0]) {
                        case PacketType.Ping:
                            if (buf.Length >= PacketProtocol.PingPacketSize) {
                                long ts = PacketProtocol.ReadTimestamp(buf);
                                var pong = PacketProtocol.RentPongPacket(ts);
                                try {
                                    await udp.SendAsync(pong, PacketProtocol.PingPacketSize, remote).ConfigureAwait(false);
                                }
                                finally {
                                    PacketProtocol.Return(pong);
                                }
                            }
                            break;

                        case PacketType.Pong:
                            if (buf.Length >= PacketProtocol.PingPacketSize) {
                                long sentAt = PacketProtocol.ReadTimestamp(buf);
                                peer.SetPing((int)Math.Max(0, now - sentAt));
                            }
                            break;

                        case PacketType.Data: {
                            if (PooledReceive != null) {
                                // buf is a fresh array from UdpClient: safe to hand out
                                // sliced, no extra copy needed.
                                PooledReceive(peer, buf, 1, buf.Length - 1);
                            }
                            else {
                                var payload = new byte[buf.Length - 1];
                                Array.Copy(buf, 1, payload, 0, payload.Length);
                                DataReceived?.Invoke(peer, payload);
                            }
                            break;
                        }
                    }
                }
                catch (Exception) when (ct.IsCancellationRequested) {
                    return;
                }
                catch (SocketException) {
                    // ICMP "port unreachable" from a vanished client surfaces here; ignore.
                }
                catch (ObjectDisposedException) {
                    return;
                }
                catch (Exception ex) {
                    _logger.Error($"UdpServerTransport receive loop: {ex}");
                }
            }
        }

        private async Task PingLoopAsync(CancellationToken ct) {
            var udp = _udpClient!;
            try {
                while (!ct.IsCancellationRequested) {
                    foreach (var kvp in _peersByEndpoint) {
                        var peer = kvp.Value;
                        var ping = PacketProtocol.RentPingPacket(NetTime.NowMs);
                        try {
                            await udp.SendAsync(ping, PacketProtocol.PingPacketSize, peer.EndPoint).ConfigureAwait(false);
                        }
                        catch (SocketException) { }
                        finally {
                            PacketProtocol.Return(ping);
                        }

                        if (_pingTimeoutMs > 0 && NetTime.NowMs - peer.LastSeenMs > _pingTimeoutMs) {
                            RemovePeer(peer, DisconnectReason.Timeout);
                        }
                    }
                    await Task.Delay(_pingIntervalMs, ct).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) { }
            catch (ObjectDisposedException) { }
        }

        public ValueTask SendAsync(TransportPeer peer, ReadOnlyMemory<byte> data, CancellationToken ct = default) {
            return SendAsync(peer, ReadOnlyMemory<byte>.Empty, data, ct);
        }

        public async ValueTask SendAsync(TransportPeer peer, ReadOnlyMemory<byte> prefix, ReadOnlyMemory<byte> payload, CancellationToken ct = default) {
            var udp = _udpClient ?? throw new InvalidOperationException("Server transport not started");
            int totalLength = prefix.Length + payload.Length;
            if (totalLength > UdpTransport.MaxDatagramPayload) {
                throw new ArgumentException(
                    $"UDP payload of {totalLength} bytes exceeds the {UdpTransport.MaxDatagramPayload}-byte datagram limit.", nameof(payload));
            }
            var packet = PacketProtocol.RentUdpDataPacket(prefix.Span, payload.Span, out int length);
            try {
                await udp.SendAsync(packet, length, peer.EndPoint).ConfigureAwait(false);
            }
            finally {
                PacketProtocol.Return(packet);
            }
        }

        public void DisconnectPeer(TransportPeer peer) => RemovePeer(peer, DisconnectReason.Local);

        private void RemovePeer(TransportPeer peer, DisconnectReason reason) {
            if (_peersByEndpoint.TryRemove(peer.EndPoint.ToString(), out _)) {
                PeerDisconnected?.Invoke(peer, reason);
            }
        }

        public async Task StopAsync() {
            if (Interlocked.Exchange(ref _running, 0) == 0) {
                return;
            }
            _cts?.Cancel();
            foreach (var kvp in _peersByEndpoint) {
                RemovePeer(kvp.Value, DisconnectReason.Local);
            }
            try {
                _udpClient?.Close();
            }
            catch { }
            _udpClient = null;
            Port = 0;
            _cts?.Dispose();
            _cts = null;
            await Task.CompletedTask;
        }

        public void Dispose() => StopAsync().GetAwaiter().GetResult();
    }
}
