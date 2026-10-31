using System;
using System.Collections.Generic;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;

namespace NetSync.Transports.WebSockets {
    /// <summary>
    /// WebSocket transport for the <see cref="TransportType.Tcp"/> slot — the way browsers
    /// (WebAssembly, Blazor) and networks that only let HTTP(S) through reach a NetSync server.
    /// The whole stack runs on top unchanged: channels, handshake, encryption, approval.
    ///
    /// Server: <c>server.AddListener(TransportType.Tcp, new WebSocketTransportFactory(), 8080)</c>
    /// next to the native TCP/UDP listeners — both kinds of clients share one server. Give it
    /// a <see cref="ServerCertificate"/> for wss:// (required by pages served over https).
    ///
    /// Client: <c>config.TransportFactories[TransportType.Tcp] = new WebSocketTransportFactory { UseTls = true, HostName = "game.example.com" }</c>.
    /// Where UDP is unavailable (browsers), UDP channels automatically ride the WebSocket
    /// (<see cref="NetConfig.RequireAllTransports"/> = false).
    ///
    /// Wire format: one binary WebSocket message per packet, [type:1][payload] — the same
    /// packets as UDP; messages of 64 KB and more are sent as fragments so small traffic
    /// never waits behind a file.
    /// </summary>
    public sealed class WebSocketTransportFactory : ITransportFactory {
        /// <summary>Request path of the WebSocket endpoint (both sides).</summary>
        public string Path { get; set; } = "/netsync";

        /// <summary>Client: connect with wss:// (TLS) instead of ws://.</summary>
        public bool UseTls { get; set; }

        /// <summary>
        /// Client: the server's host name, for the Host header, TLS server name and certificate
        /// check. When set, the client connects to this name (resolved by the runtime) and the
        /// port it was given; when null, to the IP address passed to ConnectAsync.
        /// </summary>
        public string? HostName { get; set; }

        /// <summary>Client: custom server certificate validation (e.g. a self-signed certificate during development).</summary>
        public RemoteCertificateValidationCallback? RemoteCertificateValidation { get; set; }

        /// <summary>Server: certificate (with private key) — enables wss://.</summary>
        public X509Certificate2? ServerCertificate { get; set; }

        /// <summary>
        /// Server: accepted values of the browser's Origin header (e.g. "https://game.example.com").
        /// Empty = any origin. Non-browser clients send no Origin and are refused when the list
        /// is not empty.
        /// </summary>
        public List<string> AllowedOrigins { get; } = new List<string>();

        public ITransport CreateClient(TransportSettings settings) {
            if (!Path.StartsWith("/", StringComparison.Ordinal)) {
                throw new InvalidOperationException("WebSocketTransportFactory.Path must start with '/'");
            }
            return new WebSocketClientTransport(this, settings);
        }

        public IServerTransport CreateServer(TransportSettings settings) {
            if (!Path.StartsWith("/", StringComparison.Ordinal)) {
                throw new InvalidOperationException("WebSocketTransportFactory.Path must start with '/'");
            }
            return new WebSocketServerTransport(this, settings);
        }
    }
}
