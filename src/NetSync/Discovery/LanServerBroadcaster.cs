using System;
using System.Collections.Generic;
using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using NetSync.Diagnostics;

namespace NetSync.Discovery {
    /// <summary>
    /// Announces a server on the local network: periodically broadcasts a discovery
    /// datagram on every IPv4 LAN interface (Ethernet/Wi-Fi). The Unity-free
    /// replacement for the 1.x LocalNetworkBroadcaster.
    /// </summary>
    public sealed class LanServerBroadcaster : IDisposable {
        /// <summary>Default UDP port the discovery datagrams are sent to.</summary>
        public const int DefaultDiscoveryPort = 47777;

        private readonly string _appId;
        private readonly int _serverPort;
        private readonly int _discoveryPort;
        private readonly int _intervalMs;
        private readonly INetLogger _logger;
        private CancellationTokenSource? _cts;

        /// <summary>Test hook: when set, datagrams go only to this endpoint instead of broadcast.</summary>
        internal IPEndPoint? TargetOverride { get; set; }

        /// <param name="appId">Application identity; listeners only react to a matching id.</param>
        /// <param name="serverPort">The port clients should connect to.</param>
        /// <param name="discoveryPort">UDP port listeners are bound to.</param>
        /// <param name="intervalMs">Broadcast period.</param>
        /// <param name="logger">Optional logger.</param>
        public LanServerBroadcaster(string appId, int serverPort, int discoveryPort = DefaultDiscoveryPort,
                                    int intervalMs = 2000, INetLogger? logger = null) {
            _appId = appId ?? throw new ArgumentNullException(nameof(appId));
            _serverPort = serverPort;
            _discoveryPort = discoveryPort;
            _intervalMs = intervalMs;
            _logger = logger ?? NullNetLogger.Instance;
        }

        public void Start() {
            if (_cts != null) {
                return;
            }
            _cts = new CancellationTokenSource();
            var token = _cts.Token;
            _ = Task.Run(() => BroadcastLoopAsync(token));
        }

        public void Stop() {
            _cts?.Cancel();
            _cts?.Dispose();
            _cts = null;
        }

        private async Task BroadcastLoopAsync(CancellationToken ct) {
            var message = DiscoveryMessage.Encode(_appId, _serverPort);
            try {
                while (!ct.IsCancellationRequested) {
                    SendOnce(message);
                    await Task.Delay(_intervalMs, ct).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) { }
        }

        private void SendOnce(byte[] message) {
            if (TargetOverride != null) {
                try {
                    using var socket = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
                    socket.SendTo(message, TargetOverride);
                }
                catch (SocketException ex) {
                    _logger.Debug($"Discovery send failed: {ex.Message}");
                }
                return;
            }

            // Broadcast from every LAN-capable IPv4 address: a single socket would only
            // reach the default-route interface, missing e.g. a second Wi-Fi adapter.
            foreach (var localAddress in GetLanAddresses()) {
                try {
                    using var socket = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
                    socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.Broadcast, true);
                    socket.Bind(new IPEndPoint(localAddress, 0));
                    socket.SendTo(message, new IPEndPoint(IPAddress.Broadcast, _discoveryPort));
                }
                catch (SocketException ex) {
                    _logger.Debug($"Discovery broadcast on {localAddress} failed: {ex.Message}");
                }
            }
        }

        private static IEnumerable<IPAddress> GetLanAddresses() {
            NetworkInterface[] interfaces;
            try {
                interfaces = NetworkInterface.GetAllNetworkInterfaces();
            }
            catch (NetworkInformationException) {
                yield break;
            }

            foreach (var adapter in interfaces) {
                if (adapter.OperationalStatus != OperationalStatus.Up ||
                    (adapter.NetworkInterfaceType != NetworkInterfaceType.Ethernet &&
                     adapter.NetworkInterfaceType != NetworkInterfaceType.Wireless80211 &&
                     adapter.NetworkInterfaceType != NetworkInterfaceType.GigabitEthernet) ||
                    !adapter.Supports(NetworkInterfaceComponent.IPv4)) {
                    continue;
                }
                foreach (var unicast in adapter.GetIPProperties().UnicastAddresses) {
                    if (unicast.Address.AddressFamily == AddressFamily.InterNetwork) {
                        yield return unicast.Address;
                    }
                }
            }
        }

        public void Dispose() => Stop();
    }
}
