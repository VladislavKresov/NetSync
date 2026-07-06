using System;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using NetSync.Diagnostics;

namespace NetSync.Discovery {
    /// <summary>
    /// Listens for <see cref="LanServerBroadcaster"/> announcements and raises
    /// <see cref="ServerFound"/> with the server's connectable endpoint.
    /// The event fires on a background thread and keeps firing for every
    /// announcement received — stop the listener once connected.
    /// </summary>
    public sealed class LanServerListener : IDisposable {
        private readonly string _appId;
        private readonly int _discoveryPort;
        private readonly INetLogger _logger;
        private UdpClient? _udp;
        private CancellationTokenSource? _cts;

        /// <summary>Actual bound port (useful when constructed with port 0 in tests).</summary>
        public int Port { get; private set; }

        /// <summary>A matching server announced itself at this endpoint.</summary>
        public event Action<IPEndPoint>? ServerFound;

        public LanServerListener(string appId, int discoveryPort = LanServerBroadcaster.DefaultDiscoveryPort, INetLogger? logger = null) {
            _appId = appId ?? throw new ArgumentNullException(nameof(appId));
            _discoveryPort = discoveryPort;
            _logger = logger ?? NullNetLogger.Instance;
        }

        public void Start() {
            if (_udp != null) {
                return;
            }
            _udp = new UdpClient();
            // Several listeners (e.g. multiple game instances on one machine) share the port.
            _udp.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
            _udp.Client.Bind(new IPEndPoint(IPAddress.Any, _discoveryPort));
            Port = ((IPEndPoint)_udp.Client.LocalEndPoint!).Port;
            _cts = new CancellationTokenSource();
            var token = _cts.Token;
            _ = Task.Run(() => ListenLoopAsync(token));
        }

        private async Task ListenLoopAsync(CancellationToken ct) {
            var udp = _udp!;
            while (!ct.IsCancellationRequested) {
                try {
                    var result = await udp.ReceiveAsync().ConfigureAwait(false);
                    if (!DiscoveryMessage.TryDecode(result.Buffer, out var appId, out var serverPort) || appId != _appId) {
                        continue;
                    }
                    var endpoint = new IPEndPoint(result.RemoteEndPoint.Address, serverPort);
                    _logger.Info($"Discovered server for '{appId}' at {endpoint}");
                    try {
                        ServerFound?.Invoke(endpoint);
                    }
                    catch (Exception ex) {
                        _logger.Error($"Unhandled exception in ServerFound handler: {ex}");
                    }
                }
                catch (Exception) when (ct.IsCancellationRequested) {
                    return;
                }
                catch (SocketException) {
                    // e.g. ICMP unreachable noise; keep listening
                }
                catch (ObjectDisposedException) {
                    return;
                }
            }
        }

        public void Stop() {
            _cts?.Cancel();
            try {
                _udp?.Close();
            }
            catch { }
            _udp = null;
            _cts?.Dispose();
            _cts = null;
            Port = 0;
        }

        public void Dispose() => Stop();
    }
}
