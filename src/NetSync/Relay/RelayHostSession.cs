using System;
using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NetSync.Diagnostics;
using NetSync.Sending;

namespace NetSync.Relay {
    /// <summary>
    /// A host's control connection to the relay: claims the room, receives the secret that
    /// authorizes the host's UDP registration, and multiplexes every stream client
    /// (PeerOpen / PeerData / PeerClose) over one TCP connection.
    /// </summary>
    internal sealed class RelayHostSession : IDisposable {
        private const int KeepaliveMs = 10_000;

        private readonly INetLogger _logger;
        private readonly CancellationTokenSource _cts = new CancellationTokenSource();
        private readonly object _closeLock = new object();
        private Action? _onClosed;
        private int _receiving;
        private int _closed;

        public FramedConnection Connection { get; }
        public byte[] Secret { get; }
        public IPEndPoint RelayEndPoint { get; }
        public bool IsClosed => Volatile.Read(ref _closed) == 1;

        /// <summary>Stream-side events; set before <see cref="StartReceiving"/>.</summary>
        public Action<uint, IPEndPoint?>? PeerOpened;
        public Action<uint, byte[], int, int>? PeerData;
        public Action<uint>? PeerClosed;

        /// <summary>Runs <paramref name="callback"/> once the session closes — right away if it already has.</summary>
        public void OnClosed(Action callback) {
            lock (_closeLock) {
                if (Volatile.Read(ref _closed) == 0) {
                    _onClosed += callback;
                    return;
                }
            }
            callback();
        }

        public void RemoveOnClosed(Action callback) {
            lock (_closeLock) {
                _onClosed -= callback;
            }
        }

        private RelayHostSession(FramedConnection connection, byte[] secret, IPEndPoint relay, INetLogger logger) {
            Connection = connection;
            Secret = secret;
            RelayEndPoint = relay;
            _logger = logger;
        }

        public static async Task<RelayHostSession> ConnectAsync(IPEndPoint relay, string room, INetLogger logger, CancellationToken ct) {
            var client = new TcpClient(relay.AddressFamily);
            try {
                await Transports.TcpConnect.ConnectAsync(client, relay, ct).ConfigureAwait(false);
            }
            catch {
                client.Dispose();
                throw;
            }
            var connection = new FramedConnection(client, logger);
            try {
                connection.TrySend(RelayProtocol.HostHello, RelayProtocol.EncodeRoom(room), ReadOnlySpan<byte>.Empty, SendPriority.High);
                using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
                timeout.CancelAfter(10_000);
                using var abort = timeout.Token.Register(connection.Dispose);
                var (ok, kind, body) = await connection.ReadOneAsync(1024, timeout.Token).ConfigureAwait(false);
                if (ok && kind == RelayProtocol.HostWelcome && body.Length == RelayProtocol.SecretSize) {
                    logger.Info($"Relay {relay}: hosting room '{room}'");
                    return new RelayHostSession(connection, body, relay, logger);
                }
                string reason = ok && kind == RelayProtocol.Refused ? Encoding.UTF8.GetString(body) : "no answer";
                throw new InvalidOperationException($"Relay {relay} refused room '{room}': {reason}");
            }
            catch {
                connection.Dispose();
                throw;
            }
        }

        /// <summary>Starts dispatching stream-client frames (idempotent).</summary>
        public void StartReceiving() {
            if (Interlocked.Exchange(ref _receiving, 1) == 1) {
                return;
            }
            var token = _cts.Token;
            _ = Task.Run(async () => {
                await Connection.RunReceiveLoopAsync(OnFrame, token).ConfigureAwait(false);
                Close();
            });
            _ = Task.Run(() => KeepaliveLoopAsync(token));
        }

        private bool OnFrame(byte kind, byte[] buffer, int offset, int length) {
            switch (kind) {
                case RelayProtocol.PeerOpen: {
                    if (length < 4) {
                        return false;
                    }
                    uint id = BinaryPrimitives.ReadUInt32BigEndian(buffer.AsSpan(offset));
                    RelayProtocol.TryReadEndPoint(buffer.AsSpan(offset + 4, length - 4), out var endPoint, out _);
                    PeerOpened?.Invoke(id, endPoint);
                    return true;
                }
                case RelayProtocol.PeerData:
                    if (length < 4) {
                        return false;
                    }
                    PeerData?.Invoke(BinaryPrimitives.ReadUInt32BigEndian(buffer.AsSpan(offset)), buffer, offset + 4, length - 4);
                    return true;
                case RelayProtocol.PeerClose:
                    if (length >= 4) {
                        PeerClosed?.Invoke(BinaryPrimitives.ReadUInt32BigEndian(buffer.AsSpan(offset)));
                    }
                    return true;
                case RelayProtocol.Keepalive:
                    return true;
                default:
                    return false;
            }
        }

        private async Task KeepaliveLoopAsync(CancellationToken ct) {
            try {
                while (!ct.IsCancellationRequested) {
                    await Task.Delay(KeepaliveMs, ct).ConfigureAwait(false);
                    Connection.TrySend(RelayProtocol.Keepalive, ReadOnlySpan<byte>.Empty, ReadOnlySpan<byte>.Empty, SendPriority.Critical);
                }
            }
            catch (OperationCanceledException) { }
        }

        private void Close() {
            Action? onClosed;
            lock (_closeLock) {
                if (Interlocked.Exchange(ref _closed, 1) == 1) {
                    return;
                }
                onClosed = _onClosed;
                _onClosed = null;
            }
            _logger.Warning($"Relay {RelayEndPoint}: host connection closed");
            _cts.Cancel();
            Connection.Dispose();
            onClosed?.Invoke();
        }

        public void Dispose() => Close();
    }
}
