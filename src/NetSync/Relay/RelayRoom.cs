using System;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using NetSync.Diagnostics;
using NetSync.Transports;

namespace NetSync.Relay {
    /// <summary>
    /// A room on a <see cref="NetRelayServer"/>: the way to reach a server behind NAT.
    ///
    /// Host — keep accepting LAN clients directly, add the relay as extra listeners:
    /// <code>
    /// var room = new RelayRoom(RelayRoom.NewRoomCode()) { RelayEndPoint = relayEndPoint };
    /// server.AddListener(TransportType.Tcp, room.Tcp, 0);
    /// server.AddListener(TransportType.Udp, room.Udp, 0);
    /// await server.StartAsync(7777);   // tell players room.RoomId
    /// </code>
    /// Client — same channels, the relay's address, the room code:
    /// <code>
    /// var room = new RelayRoom("K7QX2M");
    /// config.TransportFactories[TransportType.Tcp] = room.Tcp;
    /// config.TransportFactories[TransportType.Udp] = room.Udp;
    /// await client.ConnectAsync("relay.example.com", 7000);
    /// </code>
    /// The UDP side tries a direct, hole-punched path first (<see cref="AllowDirect"/>) and
    /// stays on the relay when the NATs don't allow one; the stream side always goes
    /// through the relay (TCP can't be hole-punched reliably). To take all traffic off the
    /// relay, configure only UDP channels — ReliableOrdered UDP replaces a TCP channel — and
    /// register only <see cref="Udp"/>. Encryption and authentication stay end to end — the
    /// relay only sees envelopes.
    ///
    /// A host that loses the relay (relay restart, network outage) keeps reclaiming its room
    /// in the background, with growing pauses up to <see cref="MaxReconnectDelayMs"/>; clients
    /// that were connected through the relay reconnect on their own (<see cref="NetConfig.Reconnect"/>).
    /// </summary>
    public sealed class RelayRoom {
        private const string CodeAlphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"; // no 0/O, 1/I

        /// <summary>Longest pause between a host's attempts to get back onto a lost relay.</summary>
        public const int MaxReconnectDelayMs = 30_000;
        private const int FirstReconnectDelayMs = 500;

        private readonly object _hostLock = new object();
        private Task<RelayHostSession>? _hostSession;
        private int _hostUsers;

        public RelayRoom(string roomId) {
            RelayProtocol.EncodeRoom(roomId); // validates
            RoomId = roomId;
            Tcp = new StreamFactory(this);
            Udp = new DatagramFactory(this);
        }

        /// <summary>A random, readable room code (no ambiguous characters).</summary>
        public static string NewRoomCode(int length = 6) {
            var bytes = new byte[length];
            RandomNumberGenerator.Fill(bytes);
            var chars = new char[length];
            for (int i = 0; i < length; i++) {
                chars[i] = CodeAlphabet[bytes[i] % CodeAlphabet.Length];
            }
            return new string(chars);
        }

        public string RoomId { get; }

        /// <summary>Host only: the relay server's address. Clients pass it to ConnectAsync instead.</summary>
        public IPEndPoint? RelayEndPoint { get; set; }

        /// <summary>Try a direct (hole-punched) UDP path; false = always through the relay.</summary>
        public bool AllowDirect { get; set; } = true;

        /// <summary>Stream slot (<see cref="TransportType.Tcp"/>) through the relay.</summary>
        public ITransportFactory Tcp { get; }

        /// <summary>Datagram slot (<see cref="TransportType.Udp"/>): direct when possible, relayed otherwise.</summary>
        public ITransportFactory Udp { get; }

        /// <summary>Test hook: how datagram sockets are made (NAT simulations wrap them).</summary>
        internal Func<AddressFamily, IDatagramSocket> SocketFactory { get; set; } = family => new DatagramSocket(family);

        internal IPEndPoint RequireRelayEndPoint() {
            return RelayEndPoint ?? throw new InvalidOperationException("RelayRoom.RelayEndPoint must be set on the host");
        }

        /// <summary>
        /// Raised when the host got back onto the relay after losing it — a new session with a
        /// new secret. Host transports re-attach to it; it starts receiving right after.
        /// </summary>
        internal event Action<RelayHostSession>? HostSessionRestored;

        /// <summary>The host's control connection, shared by its stream and datagram transports.</summary>
        internal Task<RelayHostSession> AcquireHostSessionAsync(INetLogger logger, CancellationToken ct) {
            lock (_hostLock) {
                _hostUsers++;
                if (_hostSession == null || _hostSession.IsFaulted || _hostSession.IsCanceled ||
                    (_hostSession.IsCompleted && _hostSession.Result.IsClosed)) {
                    _hostSession = ConnectHostAsync(logger, ct);
                }
                return _hostSession;
            }
        }

        private async Task<RelayHostSession> ConnectHostAsync(INetLogger logger, CancellationToken ct) {
            var session = await RelayHostSession.ConnectAsync(RequireRelayEndPoint(), RoomId, logger, ct).ConfigureAwait(false);
            session.OnClosed(() => OnHostSessionLost(session, logger));
            return session;
        }

        private void OnHostSessionLost(RelayHostSession lost, INetLogger logger) {
            TaskCompletionSource<RelayHostSession> restored;
            lock (_hostLock) {
                var current = _hostSession;
                if (_hostUsers == 0 || current == null || current.Status != TaskStatus.RanToCompletion || !ReferenceEquals(current.Result, lost)) {
                    return; // released on purpose, or already being replaced
                }
                // Transports starting meanwhile wait for the replacement instead of a dead session.
                restored = new TaskCompletionSource<RelayHostSession>(TaskCreationOptions.RunContinuationsAsynchronously);
                _hostSession = restored.Task;
            }
            logger.Warning($"Relay room '{RoomId}': lost the relay, reconnecting");
            _ = RestoreHostSessionAsync(restored, logger);
        }

        private async Task RestoreHostSessionAsync(TaskCompletionSource<RelayHostSession> restored, INetLogger logger) {
            int delayMs = FirstReconnectDelayMs;
            while (true) {
                await Task.Delay(delayMs).ConfigureAwait(false);
                if (!InUse()) {
                    restored.TrySetCanceled();
                    return;
                }
                RelayHostSession session;
                try {
                    session = await RelayHostSession.ConnectAsync(RequireRelayEndPoint(), RoomId, logger, CancellationToken.None).ConfigureAwait(false);
                }
                catch (Exception ex) {
                    delayMs = Math.Min(delayMs * 2, MaxReconnectDelayMs);
                    logger.Warning($"Relay room '{RoomId}': relay still unreachable ({ex.Message}); next attempt in {delayMs} ms");
                    continue;
                }
                if (!InUse()) {
                    session.Dispose();
                    restored.TrySetCanceled();
                    return;
                }
                session.OnClosed(() => OnHostSessionLost(session, logger));
                logger.Info($"Relay room '{RoomId}': back on the relay");
                try {
                    HostSessionRestored?.Invoke(session);
                }
                catch (Exception ex) {
                    logger.Error($"Relay room '{RoomId}': re-attaching a transport failed: {ex}");
                }
                session.StartReceiving();
                restored.TrySetResult(session);
                return;
            }
        }

        private bool InUse() {
            lock (_hostLock) {
                return _hostUsers > 0;
            }
        }

        internal void ReleaseHostSession() {
            Task<RelayHostSession>? toClose = null;
            lock (_hostLock) {
                if (--_hostUsers == 0) {
                    toClose = _hostSession;
                    _hostSession = null;
                }
            }
            if (toClose != null) {
                _ = toClose.ContinueWith(t => {
                    if (t.Status == TaskStatus.RanToCompletion) {
                        t.Result.Dispose();
                    }
                    else {
                        _ = t.Exception; // a failed connect nobody waits for any more
                    }
                }, CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
            }
        }

        private sealed class StreamFactory : ITransportFactory {
            private readonly RelayRoom _room;
            public StreamFactory(RelayRoom room) => _room = room;
            public ITransport CreateClient(TransportSettings settings) => new RelayStreamClientTransport(_room, settings);
            public IServerTransport CreateServer(TransportSettings settings) => new RelayStreamHostTransport(_room, settings);
        }

        private sealed class DatagramFactory : ITransportFactory {
            private readonly RelayRoom _room;
            public DatagramFactory(RelayRoom room) => _room = room;
            public ITransport CreateClient(TransportSettings settings) => new RelayDatagramClientTransport(_room, settings);
            public IServerTransport CreateServer(TransportSettings settings) => new RelayDatagramHostTransport(_room, settings);
        }
    }
}
