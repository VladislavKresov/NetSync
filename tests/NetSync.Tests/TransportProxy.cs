using System;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace NetSync.Tests {
    /// <summary>
    /// Sits between a client and a server on one port, forwarding TCP and UDP to the server —
    /// a stand-in for the network. <see cref="DropUdp"/> makes every datagram vanish in both
    /// directions while TCP keeps working: what a firewall change or a lost NAT mapping does.
    /// Unlike <c>NetworkSimulation</c>, this sits under the transports, so their keepalive
    /// pings are lost too. One client at a time.
    /// </summary>
    internal sealed class TransportProxy : IDisposable {
        private readonly IPEndPoint _server;
        private readonly TcpListener _tcp;
        private readonly Socket _udp;
        private readonly CancellationTokenSource _stop = new CancellationTokenSource();
        private Socket? _upstreamUdp;
        private EndPoint? _client;
        private volatile bool _dropUdp;

        public int Port { get; }

        /// <summary>When set, datagrams in either direction are dropped.</summary>
        public bool DropUdp {
            get => _dropUdp;
            set => _dropUdp = value;
        }

        public TransportProxy(int serverPort) {
            _server = new IPEndPoint(IPAddress.Loopback, serverPort);
            // One port number for both protocols: the system picks a free TCP port, which may be
            // taken for UDP by someone else (other test processes run in parallel) — then another.
            for (int attempt = 0; ; attempt++) {
                var tcp = new TcpListener(IPAddress.Loopback, 0);
                tcp.Start();
                int port = ((IPEndPoint)tcp.LocalEndpoint).Port;
                var udp = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
                try {
                    udp.Bind(new IPEndPoint(IPAddress.Loopback, port));
                }
                catch (SocketException) when (attempt < 50) {
                    udp.Dispose();
                    tcp.Stop();
                    continue;
                }
                _tcp = tcp;
                _udp = udp;
                Port = port;
                break;
            }
            _ = Task.Run(AcceptLoopAsync);
            _ = Task.Run(ClientToServerLoop);
        }

        private async Task AcceptLoopAsync() {
            try {
                while (!_stop.IsCancellationRequested) {
                    var client = await _tcp.AcceptTcpClientAsync().ConfigureAwait(false);
                    var upstream = new TcpClient { NoDelay = true };
                    client.NoDelay = true;
                    await upstream.ConnectAsync(_server.Address, _server.Port).ConfigureAwait(false);
                    _ = PipeAsync(client, upstream);
                    _ = PipeAsync(upstream, client);
                }
            }
            catch (Exception) when (_stop.IsCancellationRequested) { }
            catch (ObjectDisposedException) { }
        }

        private async Task PipeAsync(TcpClient from, TcpClient to) {
            try {
                await from.GetStream().CopyToAsync(to.GetStream(), 81920, _stop.Token).ConfigureAwait(false);
            }
            catch (Exception) {
                // either side closed
            }
            finally {
                from.Close();
                to.Close();
            }
        }

        private void ClientToServerLoop() {
            var buffer = new byte[65536];
            try {
                while (!_stop.IsCancellationRequested) {
                    EndPoint from = new IPEndPoint(IPAddress.Any, 0);
                    int received = _udp.ReceiveFrom(buffer, ref from);
                    if (_upstreamUdp == null) {
                        _client = from;
                        _upstreamUdp = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
                        _upstreamUdp.Connect(_server);
                        var upstream = _upstreamUdp;
                        _ = Task.Run(() => ServerToClientLoop(upstream));
                    }
                    if (!_dropUdp) {
                        _upstreamUdp.Send(buffer, 0, received, SocketFlags.None);
                    }
                }
            }
            catch (Exception) {
                // stopped
            }
        }

        private void ServerToClientLoop(Socket upstream) {
            var buffer = new byte[65536];
            try {
                while (!_stop.IsCancellationRequested) {
                    int received = upstream.Receive(buffer);
                    if (!_dropUdp && _client != null) {
                        _udp.SendTo(buffer, 0, received, SocketFlags.None, _client);
                    }
                }
            }
            catch (Exception) {
                // stopped
            }
        }

        public void Dispose() {
            _stop.Cancel();
            _tcp.Stop();
            // Wakes the blocked receives first: on macOS a plain close would wait for them.
            NetSync.Transports.Udp.UdpSockets.Close(_udp);
            if (_upstreamUdp != null) {
                NetSync.Transports.Udp.UdpSockets.Close(_upstreamUdp);
            }
        }
    }
}
