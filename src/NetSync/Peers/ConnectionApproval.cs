using System;
using System.Net;
using System.Threading.Tasks;

namespace NetSync.Peers {
    /// <summary>
    /// A client asking to join (see <see cref="NetServer.ConnectionApprover"/>). The
    /// handshake is complete — keys established, server identity proven — but the
    /// connection isn't open yet: the application decides.
    /// </summary>
    public sealed class ConnectionRequest {
        /// <summary>The pending connection. Set <see cref="NetConnection.Tag"/> here to attach your session object.</summary>
        public NetConnection Connection { get; }

        /// <summary>
        /// What the client passed to ConnectAsync (login token, version, nickname…).
        /// Encrypted on the wire whenever a key exchange is configured.
        /// </summary>
        public ReadOnlyMemory<byte> ConnectData { get; }

        /// <summary>Address of the client's first link.</summary>
        public IPEndPoint? RemoteEndPoint =>
            Connection.GetEndPoint(TransportType.Tcp) ?? Connection.GetEndPoint(TransportType.Udp);

        internal ConnectionRequest(NetConnection connection, ReadOnlyMemory<byte> connectData) {
            Connection = connection;
            ConnectData = connectData;
        }
    }

    /// <summary>The application's answer to a <see cref="ConnectionRequest"/>.</summary>
    public readonly struct ConnectionDecision {
        /// <summary>Longest reject reason sent to the client, in UTF-8 bytes.</summary>
        public const int MaxReasonBytes = 200;

        public bool IsAccepted { get; }

        /// <summary>Shown to the client in <see cref="ConnectionRejectedException.Reason"/>.</summary>
        public string? RejectReason { get; }

        private ConnectionDecision(bool accepted, string? reason) {
            IsAccepted = accepted;
            RejectReason = reason;
        }

        public static ConnectionDecision Accept() => new ConnectionDecision(true, null);

        public static ConnectionDecision Reject(string reason) => new ConnectionDecision(false, reason ?? "rejected");

        /// <summary>Lets synchronous approvers return a decision directly.</summary>
        public static implicit operator ValueTask<ConnectionDecision>(ConnectionDecision decision) {
            return new ValueTask<ConnectionDecision>(decision);
        }
    }

    /// <summary>
    /// Decides whether a client may join. Runs on a network thread: keep it quick, or
    /// make it async (e.g. look the token up in a database) — the client waits up to
    /// <see cref="NetConfig.ConnectTimeoutMs"/>. Throwing rejects the client.
    /// </summary>
    public delegate ValueTask<ConnectionDecision> ConnectionApprover(ConnectionRequest request);

    /// <summary>Why a connect attempt was refused.</summary>
    public enum ConnectionRejection {
        /// <summary>Client and server speak different protocol versions.</summary>
        ProtocolVersion,
        /// <summary>Key exchange modes differ, or no common cipher suite.</summary>
        Encryption,
        /// <summary>Channel tables or MaxMessageSize differ.</summary>
        Configuration,
        /// <summary>The server couldn't verify our packets — different pre-shared keys.</summary>
        Authentication,
        /// <summary>The server's identity didn't match a pinned key (possible man-in-the-middle).</summary>
        ServerIdentity,
        /// <summary>The server application refused (ConnectionApprover, or server full).</summary>
        Application
    }

    /// <summary>The server refused the connection, or the client refused the server.</summary>
    public sealed class ConnectionRejectedException : InvalidOperationException {
        public ConnectionRejection Rejection { get; }

        /// <summary>Human-readable reason (for Application: the server's reject reason).</summary>
        public string Reason { get; }

        public ConnectionRejectedException(ConnectionRejection rejection, string reason)
            : base(rejection == ConnectionRejection.Application
                ? $"Server rejected the connection: {reason}"
                : $"Connection refused: {reason}") {
            Rejection = rejection;
            Reason = reason;
        }
    }
}
