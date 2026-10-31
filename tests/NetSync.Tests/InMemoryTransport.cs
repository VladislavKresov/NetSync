using System;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using NetSync.Transports;

namespace NetSync.Tests {
    /// <summary>
    /// A complete in-process transport implementation built only on the public
    /// ITransport / IServerTransport / ITransportFactory contracts — proof that custom
    /// transports plug into NetClient/NetServer, and a deterministic test network.
    /// One network = one transport slot; loss applies to every message in both directions.
    /// </summary>
    internal sealed class InMemoryNetwork {
        private readonly ConcurrentDictionary<int, InMemoryServerTransport> _servers = new ConcurrentDictionary<int, InMemoryServerTransport>();
        private readonly Random _random;
        private readonly double _lossRate;
        private int _nextPort = 40000;
        private int _nextClientPort = 1;
        private int _dropped;

        public int Dropped => Volatile.Read(ref _dropped);
        public ITransportFactory Factory { get; }

        /// <summary>Attack-test hooks: every client→server packet (a wiretap), and every client transport created.</summary>
        public Action<byte[]>? ClientToServerTap;
        /// <summary>A client→server packet the network just lost, with the client transport that sent it.</summary>
        public Action<InMemoryClientTransport, byte[]>? ClientToServerDropped;
        public readonly ConcurrentQueue<InMemoryClientTransport> Clients = new ConcurrentQueue<InMemoryClientTransport>();

        public InMemoryNetwork(double lossRate = 0, int seed = 1) {
            _lossRate = lossRate;
            _random = new Random(seed);
            Factory = new InMemoryFactory(this);
        }

        internal bool ShouldDrop() {
            if (_lossRate <= 0) {
                return false;
            }
            bool drop;
            lock (_random) {
                drop = _random.NextDouble() < _lossRate;
            }
            if (drop) {
                Interlocked.Increment(ref _dropped);
            }
            return drop;
        }

        internal int Register(InMemoryServerTransport server, int port) {
            if (port == 0) {
                port = Interlocked.Increment(ref _nextPort);
            }
            if (!_servers.TryAdd(port, server)) {
                throw new SocketException((int)SocketError.AddressAlreadyInUse);
            }
            return port;
        }

        internal void Unregister(int port) => _servers.TryRemove(port, out _);
        internal bool TryGetServer(int port, out InMemoryServerTransport server) => _servers.TryGetValue(port, out server!);
        internal int NextClientPort() => Interlocked.Increment(ref _nextClientPort);

        private sealed class InMemoryFactory : ITransportFactory {
            private readonly InMemoryNetwork _network;
            public InMemoryFactory(InMemoryNetwork network) => _network = network;
            public ITransport CreateClient(TransportSettings settings) {
                var client = new InMemoryClientTransport(_network);
                _network.Clients.Enqueue(client);
                return client;
            }
            public IServerTransport CreateServer(TransportSettings settings) => new InMemoryServerTransport(_network);
        }
    }

    /// <summary>Single-reader mailbox: events for one receiver run on one logical thread, in order.</summary>
    internal sealed class SerialInbox {
        private readonly Channel<Action> _channel = Channel.CreateUnbounded<Action>(new UnboundedChannelOptions { SingleReader = true });

        public SerialInbox() {
            _ = Task.Run(async () => {
                await foreach (var action in _channel.Reader.ReadAllAsync()) {
                    try {
                        action();
                    }
                    catch {
                        // a throwing handler must not kill the pump
                    }
                }
            });
        }

        public void Post(Action action) => _channel.Writer.TryWrite(action);
        public void Complete() => _channel.Writer.TryComplete();
    }

    internal sealed class InMemoryClientTransport : ITransport {
        private readonly InMemoryNetwork _network;
        private InMemoryServerTransport? _server;
        private TransportPeer? _serverSidePeer;
        private SerialInbox? _inbox;
        private int _connected;

        public bool IsConnected => Volatile.Read(ref _connected) == 1;
        public int PingMs => IsConnected ? 0 : -1;

        public event Action? Connected;
        public event Action<DisconnectReason>? Disconnected;
        public event Action<byte[]>? DataReceived;

        public InMemoryClientTransport(InMemoryNetwork network) => _network = network;

        public Task ConnectAsync(IPEndPoint endpoint, CancellationToken ct = default) {
            if (!_network.TryGetServer(endpoint.Port, out var server)) {
                throw new SocketException((int)SocketError.ConnectionRefused);
            }
            _inbox = new SerialInbox();
            _server = server;
            _serverSidePeer = server.Accept(this);
            Volatile.Write(ref _connected, 1);
            Connected?.Invoke();
            return Task.CompletedTask;
        }

        public ValueTask SendAsync(ReadOnlyMemory<byte> data, CancellationToken ct = default) {
            return SendAsync(ReadOnlyMemory<byte>.Empty, data, ct);
        }

        public ValueTask SendAsync(ReadOnlyMemory<byte> prefix, ReadOnlyMemory<byte> payload, CancellationToken ct = default) {
            if (!IsConnected) {
                throw new InvalidOperationException("Not connected");
            }
            var bytes = Concat(prefix, payload);
            _network.ClientToServerTap?.Invoke(bytes);
            if (!_network.ShouldDrop()) {
                _server!.DeliverFromClient(_serverSidePeer!, bytes);
            }
            else {
                _network.ClientToServerDropped?.Invoke(this, bytes);
            }
            return default;
        }

        /// <summary>Attack hook: raw bytes arrive at the server as if this client sent them.</summary>
        internal void InjectToServer(byte[] raw) => _server!.DeliverFromClient(_serverSidePeer!, raw);

        /// <summary>Attack hook: raw bytes arrive at this client as if the server sent them.</summary>
        internal void InjectFromServer(byte[] raw) => DeliverFromServer(raw);

        internal void DeliverFromServer(byte[] data) => _inbox?.Post(() => DataReceived?.Invoke(data));

        internal void CloseFromServer() => Close(DisconnectReason.Remote, notifyServer: false);

        public void Disconnect() => Close(DisconnectReason.Local, notifyServer: true);

        private void Close(DisconnectReason reason, bool notifyServer) {
            if (Interlocked.Exchange(ref _connected, 0) == 0) {
                return;
            }
            if (notifyServer) {
                _server?.ClientGone(_serverSidePeer!);
            }
            var inbox = _inbox;
            inbox?.Post(() => Disconnected?.Invoke(reason));
            inbox?.Complete();
        }

        public void Dispose() => Disconnect();

        internal static byte[] Concat(ReadOnlyMemory<byte> prefix, ReadOnlyMemory<byte> payload) {
            var bytes = new byte[prefix.Length + payload.Length];
            prefix.Span.CopyTo(bytes);
            payload.Span.CopyTo(bytes.AsSpan(prefix.Length));
            return bytes;
        }
    }

    internal sealed class InMemoryServerTransport : IServerTransport {
        private readonly InMemoryNetwork _network;
        private readonly ConcurrentDictionary<TransportPeer, InMemoryClientTransport> _clients = new ConcurrentDictionary<TransportPeer, InMemoryClientTransport>();
        private readonly SerialInbox _inbox = new SerialInbox();
        private int _running;

        public int Port { get; private set; }
        public bool IsRunning => Volatile.Read(ref _running) == 1;

        public event Action<TransportPeer>? PeerConnected;
        public event Action<TransportPeer, DisconnectReason>? PeerDisconnected;
        public event Action<TransportPeer, byte[]>? DataReceived;

        public InMemoryServerTransport(InMemoryNetwork network) => _network = network;

        public Task<int> StartAsync(int port, CancellationToken ct = default) {
            Port = _network.Register(this, port);
            Volatile.Write(ref _running, 1);
            return Task.FromResult(Port);
        }

        internal TransportPeer Accept(InMemoryClientTransport client) {
            var peer = new TransportPeer(new IPEndPoint(IPAddress.Loopback, _network.NextClientPort()));
            _clients[peer] = client;
            _inbox.Post(() => PeerConnected?.Invoke(peer));
            return peer;
        }

        internal void DeliverFromClient(TransportPeer peer, byte[] data) {
            if (_clients.ContainsKey(peer)) {
                _inbox.Post(() => DataReceived?.Invoke(peer, data));
            }
        }

        internal void ClientGone(TransportPeer peer) {
            if (_clients.TryRemove(peer, out _)) {
                _inbox.Post(() => PeerDisconnected?.Invoke(peer, DisconnectReason.Remote));
            }
        }

        public ValueTask SendAsync(TransportPeer peer, ReadOnlyMemory<byte> data, CancellationToken ct = default) {
            return SendAsync(peer, ReadOnlyMemory<byte>.Empty, data, ct);
        }

        public ValueTask SendAsync(TransportPeer peer, ReadOnlyMemory<byte> prefix, ReadOnlyMemory<byte> payload, CancellationToken ct = default) {
            if (_clients.TryGetValue(peer, out var client) && !_network.ShouldDrop()) {
                client.DeliverFromServer(InMemoryClientTransport.Concat(prefix, payload));
            }
            return default;
        }

        public void DisconnectPeer(TransportPeer peer) {
            if (_clients.TryRemove(peer, out var client)) {
                client.CloseFromServer();
                _inbox.Post(() => PeerDisconnected?.Invoke(peer, DisconnectReason.Local));
            }
        }

        public Task StopAsync() {
            if (Interlocked.Exchange(ref _running, 0) == 0) {
                return Task.CompletedTask;
            }
            _network.Unregister(Port);
            foreach (var peer in _clients.Keys) {
                DisconnectPeer(peer);
            }
            return Task.CompletedTask;
        }

        public void Dispose() => StopAsync().GetAwaiter().GetResult();
    }

    /// <summary>A datagram transport that "connects" but never delivers anything — simulates blocked UDP.</summary>
    internal sealed class BlackholeTransportFactory : ITransportFactory {
        public ITransport CreateClient(TransportSettings settings) => new BlackholeTransport();
        public IServerTransport CreateServer(TransportSettings settings) => throw new NotSupportedException();

        private sealed class BlackholeTransport : ITransport {
            public bool IsConnected { get; private set; }
            public int PingMs => -1;
#pragma warning disable CS0067 // never raised: nothing ever arrives
            public event Action? Connected;
            public event Action<DisconnectReason>? Disconnected;
            public event Action<byte[]>? DataReceived;
#pragma warning restore CS0067
            public Task ConnectAsync(IPEndPoint endpoint, CancellationToken ct = default) {
                IsConnected = true;
                return Task.CompletedTask;
            }
            public ValueTask SendAsync(ReadOnlyMemory<byte> data, CancellationToken ct = default) => default;
            public ValueTask SendAsync(ReadOnlyMemory<byte> prefix, ReadOnlyMemory<byte> payload, CancellationToken ct = default) => default;
            public void Disconnect() => IsConnected = false;
            public void Dispose() => Disconnect();
        }
    }
}
