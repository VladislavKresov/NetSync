using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using NetSync.Diagnostics;
using NetSync.Internal;
using NetSync.Pipeline.Fragmentation;

namespace NetSync.Transports.Tcp {
    /// <summary>
    /// Server-side TCP transport: accepts clients, frames packets, pings peers and
    /// drops the ones that stop answering. Send/receive paths use pooled buffers.
    /// </summary>
    public sealed class TcpServerTransport : IServerTransport {
        private readonly int _pingIntervalMs;
        private readonly int _pingTimeoutMs;
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
        internal Action<TransportPeer, byte[], int, int>? PooledReceive;

        public TcpServerTransport(int pingIntervalMs = 1000, int pingTimeoutMs = 5000, INetLogger? logger = null) {
            _pingIntervalMs = pingIntervalMs;
            _pingTimeoutMs = pingTimeoutMs;
            _logger = logger ?? NullNetLogger.Instance;
        }

        public Task<int> StartAsync(int port, CancellationToken ct = default) {
            if (Interlocked.Exchange(ref _running, 1) == 1) {
                throw new InvalidOperationException("Server transport already running");
            }

            _cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            _listener = new TcpListener(IPAddress.Any, port);
            _listener.Start();
            Port = ((IPEndPoint)_listener.LocalEndpoint).Port;

            var token = _cts.Token;
            _ = Task.Run(() => AcceptLoopAsync(token));
            _ = Task.Run(() => PingLoopAsync(token));

            _logger.Info($"TcpServerTransport listening on port {Port}");
            return Task.FromResult(Port);
        }

        private async Task AcceptLoopAsync(CancellationToken ct) {
            var listener = _listener!;
            while (!ct.IsCancellationRequested) {
                TcpClient tcpClient;
                try {
                    tcpClient = await listener.AcceptTcpClientAsync().ConfigureAwait(false);
                }
                catch (Exception) when (ct.IsCancellationRequested) {
                    return;
                }
                catch (Exception ex) {
                    _logger.Error($"TcpServerTransport accept: {ex.Message}");
                    continue;
                }

                tcpClient.NoDelay = true;
                var endPoint = (IPEndPoint)tcpClient.Client.RemoteEndPoint!;
                var peer = new TransportPeer(endPoint, NetTime.NowMs) { TransportState = tcpClient };
                _peers[peer.Id] = peer;
                PeerConnected?.Invoke(peer);
                _ = Task.Run(() => ReceiveLoopAsync(peer, tcpClient, ct));
            }
        }

        private async Task ReceiveLoopAsync(TransportPeer peer, TcpClient tcpClient, CancellationToken ct) {
            var reason = DisconnectReason.Remote;
            try {
                var stream = tcpClient.GetStream();
                var header = new byte[PacketProtocol.PingPacketSize];

                while (!ct.IsCancellationRequested) {
                    if (!await ReadExactAsync(stream, header, 0, 1, ct).ConfigureAwait(false)) {
                        break;
                    }
                    peer.Touch(NetTime.NowMs);

                    var type = (PacketType)header[0];
                    switch (type) {
                        case PacketType.Ping:
                        case PacketType.Pong: {
                            if (!await ReadExactAsync(stream, header, 1, 8, ct).ConfigureAwait(false)) {
                                goto done;
                            }
                            long timestamp = PacketProtocol.ReadTimestamp(header);
                            if (type == PacketType.Ping) {
                                var pong = PacketProtocol.RentPongPacket(timestamp);
                                try {
                                    await WriteRawAsync(peer, tcpClient, pong, PacketProtocol.PingPacketSize, ct).ConfigureAwait(false);
                                }
                                finally {
                                    PacketProtocol.Return(pong);
                                }
                            }
                            else {
                                peer.SetPing((int)Math.Max(0, NetTime.NowMs - timestamp));
                            }
                            break;
                        }

                        case PacketType.Data: {
                            if (!await ReadExactAsync(stream, header, 1, 4, ct).ConfigureAwait(false)) {
                                goto done;
                            }
                            int length = PacketProtocol.ReadTcpFrameLength(header);
                            if (length < 0) {
                                _logger.Error($"TcpServerTransport: invalid frame length {length} from {peer}");
                                reason = DisconnectReason.Error;
                                goto done;
                            }
                            if (length == 0) {
                                DeliverPayload(peer, Array.Empty<byte>(), 0);
                                break;
                            }

                            var payload = ArrayPool<byte>.Shared.Rent(length);
                            try {
                                if (!await ReadExactAsync(stream, payload, 0, length, ct).ConfigureAwait(false)) {
                                    goto done;
                                }
                                DeliverPayload(peer, payload, length);
                            }
                            finally {
                                ArrayPool<byte>.Shared.Return(payload);
                            }
                            break;
                        }

                        default:
                            _logger.Error($"TcpServerTransport: unknown packet type 0x{header[0]:X2} from {peer}");
                            reason = DisconnectReason.Error;
                            goto done;
                    }
                }
                done: ;
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
            }
        }

        // buffer is pooled and only valid during this call.
        private void DeliverPayload(TransportPeer peer, byte[] buffer, int length) {
            var span = buffer.AsSpan(0, length);
            if (PacketFragmenter.TryUnwrap(span, out uint seqId, out uint index, out uint total, out var fragmentData)) {
                var reassembled = peer.FragmentBuffer.AddFragment(seqId, index, total, fragmentData);
                if (reassembled != null) {
                    if (PooledReceive != null) {
                        PooledReceive(peer, reassembled, 0, reassembled.Length);
                    }
                    else {
                        DataReceived?.Invoke(peer, reassembled);
                    }
                }
                return;
            }

            if (PooledReceive != null) {
                PooledReceive(peer, buffer, 0, length);
            }
            else {
                DataReceived?.Invoke(peer, span.ToArray());
            }
        }

        private static async Task<bool> ReadExactAsync(Stream stream, byte[] buffer, int offset, int count, CancellationToken ct) {
            int total = 0;
            while (total < count) {
                int read = await stream.ReadAsync(buffer, offset + total, count - total, ct).ConfigureAwait(false);
                if (read == 0) {
                    return false;
                }
                total += read;
            }
            return true;
        }

        private async Task PingLoopAsync(CancellationToken ct) {
            try {
                while (!ct.IsCancellationRequested) {
                    foreach (var kvp in _peers) {
                        var peer = kvp.Value;
                        if (peer.TransportState is not TcpClient tcpClient) {
                            continue;
                        }
                        var ping = PacketProtocol.RentPingPacket(NetTime.NowMs);
                        try {
                            await WriteRawAsync(peer, tcpClient, ping, PacketProtocol.PingPacketSize, ct).ConfigureAwait(false);
                        }
                        catch {
                            RemovePeer(peer, DisconnectReason.Error);
                            continue;
                        }
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
        }

        public ValueTask SendAsync(TransportPeer peer, ReadOnlyMemory<byte> data, CancellationToken ct = default) {
            return SendAsync(peer, ReadOnlyMemory<byte>.Empty, data, ct);
        }

        public async ValueTask SendAsync(TransportPeer peer, ReadOnlyMemory<byte> prefix, ReadOnlyMemory<byte> payload, CancellationToken ct = default) {
            if (peer.TransportState is not TcpClient tcpClient) {
                throw new ArgumentException("Peer does not belong to this transport", nameof(peer));
            }

            try {
                int totalLength = prefix.Length + payload.Length;
                if (totalLength >= PacketFragmenter.MaxUnfragmentedSize) {
                    await SendFragmentedAsync(peer, tcpClient, prefix, payload, totalLength, ct).ConfigureAwait(false);
                }
                else {
                    var frame = PacketProtocol.RentTcpDataFrame(prefix.Span, payload.Span, out int length);
                    try {
                        await WriteRawAsync(peer, tcpClient, frame, length, ct).ConfigureAwait(false);
                    }
                    finally {
                        PacketProtocol.Return(frame);
                    }
                }
            }
            catch (Exception ex) when (ex is IOException || ex is SocketException || ex is ObjectDisposedException) {
                RemovePeer(peer, DisconnectReason.Error);
                throw;
            }
        }

        private async Task SendFragmentedAsync(TransportPeer peer, TcpClient tcpClient, ReadOnlyMemory<byte> prefix, ReadOnlyMemory<byte> payload, int totalLength, CancellationToken ct) {
            var whole = ArrayPool<byte>.Shared.Rent(totalLength);
            try {
                prefix.Span.CopyTo(whole);
                payload.Span.CopyTo(whole.AsSpan(prefix.Length));

                uint seqId = PacketFragmenter.NextSequenceId();
                uint totalFragments = (uint)((totalLength + PacketFragmenter.FragmentSizeTcp - 1) / PacketFragmenter.FragmentSizeTcp);
                for (uint i = 0; i < totalFragments; i++) {
                    int offset = (int)(i * PacketFragmenter.FragmentSizeTcp);
                    int chunkLength = Math.Min(PacketFragmenter.FragmentSizeTcp, totalLength - offset);
                    var frame = PacketProtocol.RentTcpFragmentFrame(seqId, i, totalFragments, whole.AsSpan(offset, chunkLength), out int frameLength);
                    try {
                        await WriteRawAsync(peer, tcpClient, frame, frameLength, ct).ConfigureAwait(false);
                    }
                    finally {
                        PacketProtocol.Return(frame);
                    }
                }
            }
            finally {
                ArrayPool<byte>.Shared.Return(whole);
            }
        }

        private static async Task WriteRawAsync(TransportPeer peer, TcpClient tcpClient, byte[] buffer, int length, CancellationToken ct) {
            await peer.WriteLock.WaitAsync(ct).ConfigureAwait(false);
            try {
                var stream = tcpClient.GetStream();
                await stream.WriteAsync(buffer, 0, length, ct).ConfigureAwait(false);
            }
            finally {
                peer.WriteLock.Release();
            }
        }

        public void DisconnectPeer(TransportPeer peer) => RemovePeer(peer, DisconnectReason.Local);

        private void RemovePeer(TransportPeer peer, DisconnectReason reason) {
            if (!_peers.TryRemove(peer.Id, out _)) {
                return; // already removed — event fires once
            }
            try {
                if (peer.TransportState is TcpClient tcpClient) {
                    try {
                        if (tcpClient.Connected) {
                            tcpClient.Client.Shutdown(SocketShutdown.Both);
                        }
                    }
                    catch { }
                    tcpClient.Close();
                }
            }
            catch (Exception ex) {
                _logger.Debug($"TcpServerTransport peer close: {ex.Message}");
            }
            peer.FragmentBuffer.Clear();
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
