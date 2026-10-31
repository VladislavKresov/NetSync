using System;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using NetSync.Diagnostics;
using NetSync.Internal;
using NetSync.Pipeline.Reliability;
using NetSync.Transports.Udp;

namespace NetSync.Peers {
    // Session services that run beside an open connection — clock sync, path MTU —
    // and automatic reconnection after it is lost.
    public sealed partial class NetClient {
        // Kept out of MarkJoined on purpose: a lambda capturing a parameter makes the compiler
        // allocate its closure on every entry to the method — and MarkJoined runs per message.
        private void StartSessionServices(Session session) {
            _ = Task.Run(() => TimeSyncLoopAsync(session));
            if (_config.MtuDiscovery && session.Reliable != null && session.IsLinkActive(TransportType.Udp) &&
                session.Links.TryGetValue(TransportType.Udp, out var udp) && udp is UdpTransport) {
                _ = Task.Run(() => DiscoverPathMtuAsync(session));
            }
        }

        /// <summary>
        /// Keeps <see cref="ServerTimeMs"/> fresh: a quick burst right after connecting (the
        /// best of several round trips), then one exchange every couple of seconds. Prefers
        /// the UDP link — no head-of-line blocking behind bulk TCP traffic to skew the RTT.
        /// </summary>
        private async Task TimeSyncLoopAsync(Session session) {
            var token = session.Cts.Token;
            try {
                for (int round = 0; !token.IsCancellationRequested; round++) {
                    var slot = session.IsLinkActive(TransportType.Udp) && session.Links.ContainsKey(TransportType.Udp)
                        ? TransportType.Udp
                        : TransportType.Tcp;
                    var request = Pooled.Rent(PeerProtocol.TimeRequestSize);
                    try {
                        PeerProtocol.WriteTimeRequest(request, NetTime.PreciseMs);
                        SendOnLinkAsync(session, slot, ReadOnlyMemory<byte>.Empty,
                                        new ReadOnlyMemory<byte>(request, 0, PeerProtocol.TimeRequestSize),
                                        Sending.SendPriority.High, CancellationToken.None).Forget(_logger);
                    }
                    catch (Exception ex) {
                        _logger.Debug($"Time sync send failed: {ex.Message}");
                    }
                    finally {
                        Pooled.Return(request);
                    }
                    await Task.Delay(round < TimeSyncBurst ? TimeSyncBurstIntervalMs : TimeSyncIntervalMs, token).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) { }
            catch (ObjectDisposedException) { }
        }

        /// <summary>Raises the reliable-UDP fragment size to what the path carries, here and (via PathMtu) on the server.</summary>
        private async Task DiscoverPathMtuAsync(Session session) {
            try {
                int datagramSize = await PathMtuProbe.ProbeAsync(session.Endpoint, timeoutMs: 300, session.Cts.Token).ConfigureAwait(false);
                var reliable = session.Reliable;
                if (datagramSize <= 0 || reliable == null || Volatile.Read(ref session.Closed) == 1 || !session.IsLinkActive(TransportType.Udp)) {
                    return;
                }
                reliable.FragmentSize = ReliableEndpoint.FragmentSizeForDatagram(datagramSize, enveloped: session.Crypto != null);
                var slot = session.Links.ContainsKey(TransportType.Tcp) ? TransportType.Tcp : TransportType.Udp;
                await SendOnLinkAsync(session, slot, ReadOnlyMemory<byte>.Empty, PeerProtocol.EncodePathMtu(datagramSize),
                                      Sending.SendPriority.High, CancellationToken.None).ConfigureAwait(false);
                _logger.Debug($"Path MTU to {session.Endpoint}: {datagramSize}-byte datagrams, {reliable.FragmentSize}-byte fragments");
            }
            catch (OperationCanceledException) { }
            catch (ObjectDisposedException) { }
            catch (Exception ex) {
                _logger.Debug($"Path MTU discovery failed: {ex.Message}");
            }
        }

        private void StartReconnecting(IPEndPoint endpoint, ReconnectPolicy policy) {
            var cts = new CancellationTokenSource();
            var previous = Interlocked.Exchange(ref _reconnectCts, cts);
            previous?.Cancel();
            var connectData = _lastConnectData;
            _ = Task.Run(() => ReconnectLoopAsync(endpoint, connectData, policy, cts.Token));
        }

        private void StopReconnecting() {
            Interlocked.Exchange(ref _reconnectCts, null)?.Cancel();
        }

        private async Task ReconnectLoopAsync(IPEndPoint endpoint, byte[] connectData, ReconnectPolicy policy, CancellationToken ct) {
            var random = new Random();
            for (int attempt = 1; policy.MaxAttempts <= 0 || attempt <= policy.MaxAttempts; attempt++) {
                double backoff = Math.Min(policy.MaxDelayMs, policy.InitialDelayMs * Math.Pow(2, Math.Min(attempt - 1, 20)));
                int delay = (int)(backoff * (0.8 + random.NextDouble() * 0.4)); // jitter: don't stampede a restarting server
                try {
                    await Task.Delay(delay, ct).ConfigureAwait(false);
                }
                catch (OperationCanceledException) {
                    return;
                }

                int current = attempt;
                _dispatcher.Dispatch(() => Reconnecting?.Invoke(current));
                try {
                    await ConnectCoreAsync(endpoint, connectData, ct).ConfigureAwait(false);
                    _logger.Info($"Reconnected to {endpoint} on attempt {current}");
                    return;
                }
                catch (Exception) when (ct.IsCancellationRequested) {
                    return;
                }
                catch (ConnectionRejectedException ex) when (ex.Rejection != ConnectionRejection.Application) {
                    // A configuration or security problem won't fix itself by retrying.
                    _logger.Warning($"Giving up reconnecting to {endpoint}: {ex.Message}");
                    break;
                }
                catch (Exception ex) {
                    _logger.Debug($"Reconnect attempt {current} to {endpoint} failed: {ex.Message}");
                }
            }
            if (!ct.IsCancellationRequested) {
                _logger.Warning($"Stopped reconnecting to {endpoint}");
                _dispatcher.Dispatch(() => ReconnectFailed?.Invoke());
            }
        }
    }
}
