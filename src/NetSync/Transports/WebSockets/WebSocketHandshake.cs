using System;
using System.Collections.Generic;
using System.IO;
using System.Net.WebSockets;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace NetSync.Transports.WebSockets {
    /// <summary>
    /// Server side of the RFC 6455 opening handshake, on a raw (or TLS) stream. Deliberately
    /// strict and small: bounded request size, GET on the configured path, the mandatory
    /// headers, an optional Origin allow-list. Anything else gets a plain HTTP error.
    /// </summary>
    internal static class WebSocketHandshake {
        private const int MaxRequestBytes = 8 * 1024;
        private const string AcceptGuid = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

        public enum Result { Accepted, BadRequest, Forbidden, NotFound }

        public static async Task<Result> AcceptAsync(Stream stream, string path, IReadOnlyCollection<string> allowedOrigins, CancellationToken ct) {
            var request = await ReadRequestAsync(stream, ct).ConfigureAwait(false);
            if (request == null) {
                return await RespondAsync(stream, Result.BadRequest, ct).ConfigureAwait(false);
            }

            var lines = request.Split(new[] { "\r\n" }, StringSplitOptions.None);
            var requestLine = lines[0].Split(' ');
            if (requestLine.Length != 3 || requestLine[0] != "GET" || !requestLine[2].StartsWith("HTTP/1.1", StringComparison.Ordinal)) {
                return await RespondAsync(stream, Result.BadRequest, ct).ConfigureAwait(false);
            }
            string target = requestLine[1];
            int query = target.IndexOf('?');
            if (!string.Equals(query >= 0 ? target.Substring(0, query) : target, path, StringComparison.Ordinal)) {
                return await RespondAsync(stream, Result.NotFound, ct).ConfigureAwait(false);
            }

            var headers = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            for (int i = 1; i < lines.Length; i++) {
                int colon = lines[i].IndexOf(':');
                if (colon > 0) {
                    headers[lines[i].Substring(0, colon).Trim()] = lines[i].Substring(colon + 1).Trim();
                }
            }
            if (!HasToken(headers, "Upgrade", "websocket") || !HasToken(headers, "Connection", "upgrade") ||
                !headers.TryGetValue("Sec-WebSocket-Version", out var version) || version != "13" ||
                !headers.TryGetValue("Sec-WebSocket-Key", out var key) || !IsValidKey(key)) {
                return await RespondAsync(stream, Result.BadRequest, ct).ConfigureAwait(false);
            }
            if (allowedOrigins.Count > 0 &&
                (!headers.TryGetValue("Origin", out var origin) || !Contains(allowedOrigins, origin))) {
                return await RespondAsync(stream, Result.Forbidden, ct).ConfigureAwait(false);
            }

            string accept;
            using (var sha1 = SHA1.Create()) {
                accept = Convert.ToBase64String(sha1.ComputeHash(Encoding.ASCII.GetBytes(key + AcceptGuid)));
            }
            var response = Encoding.ASCII.GetBytes(
                "HTTP/1.1 101 Switching Protocols\r\n" +
                "Upgrade: websocket\r\n" +
                "Connection: Upgrade\r\n" +
                $"Sec-WebSocket-Accept: {accept}\r\n\r\n");
            await stream.WriteAsync(response, 0, response.Length, ct).ConfigureAwait(false);
            await stream.FlushAsync(ct).ConfigureAwait(false);
            return Result.Accepted;
        }

        /// <summary>Reads up to the blank line ending the headers; null if too long or the client hung up.</summary>
        private static async Task<string?> ReadRequestAsync(Stream stream, CancellationToken ct) {
            var buffer = new byte[MaxRequestBytes];
            int filled = 0;
            while (filled < buffer.Length) {
                // One byte at a time past the headers would be wasteful; but reading ahead could
                // swallow WebSocket frames, so read only what the client has sent — it sends no
                // frames before our 101.
                int read = await stream.ReadAsync(buffer, filled, buffer.Length - filled, ct).ConfigureAwait(false);
                if (read == 0) {
                    return null;
                }
                filled += read;
                int end = IndexOfHeaderEnd(buffer, filled);
                if (end >= 0) {
                    return Encoding.ASCII.GetString(buffer, 0, end);
                }
            }
            return null;
        }

        private static int IndexOfHeaderEnd(byte[] buffer, int length) {
            for (int i = 3; i < length; i++) {
                if (buffer[i - 3] == '\r' && buffer[i - 2] == '\n' && buffer[i - 1] == '\r' && buffer[i] == '\n') {
                    return i - 3;
                }
            }
            return -1;
        }

        private static async Task<Result> RespondAsync(Stream stream, Result result, CancellationToken ct) {
            string status = result switch {
                Result.Forbidden => "403 Forbidden",
                Result.NotFound => "404 Not Found",
                _ => "400 Bad Request"
            };
            var response = Encoding.ASCII.GetBytes($"HTTP/1.1 {status}\r\nConnection: close\r\nContent-Length: 0\r\n\r\n");
            try {
                await stream.WriteAsync(response, 0, response.Length, ct).ConfigureAwait(false);
            }
            catch (IOException) { }
            return result;
        }

        private static bool HasToken(Dictionary<string, string> headers, string name, string token) {
            if (!headers.TryGetValue(name, out var value)) {
                return false;
            }
            foreach (var part in value.Split(',')) {
                if (string.Equals(part.Trim(), token, StringComparison.OrdinalIgnoreCase)) {
                    return true;
                }
            }
            return false;
        }

        private static bool IsValidKey(string key) {
            try {
                return Convert.FromBase64String(key).Length == 16;
            }
            catch (FormatException) {
                return false;
            }
        }

        private static bool Contains(IReadOnlyCollection<string> origins, string origin) {
            foreach (var allowed in origins) {
                if (string.Equals(allowed, origin, StringComparison.OrdinalIgnoreCase)) {
                    return true;
                }
            }
            return false;
        }
    }

    /// <summary>Reading whole binary messages into a bounded buffer.</summary>
    internal static class WebSocketMessages {
        public const int Closed = -2;
        public const int Violation = -1;

        /// <summary>
        /// Reads one complete message into <paramref name="buffer"/> (which grows only as far as
        /// the message needs). Returns its length — the message is at buffer.Array[buffer.Start..];
        /// consume it before the next call — or <see cref="Closed"/> when the remote closed, or
        /// <see cref="Violation"/> for a text message or one longer than <paramref name="limit"/>.
        /// </summary>
        public static async ValueTask<int> ReceiveAsync(WebSocket socket, ReceiveBuffer buffer, int limit, CancellationToken ct) {
            int filled = 0;
            while (true) {
                if (filled >= limit) {
                    return Violation; // the message continues past the limit
                }
                // Ask for room beyond what we have: the buffer grows only while a message is this big.
                var memory = buffer.GetReadMemory(Math.Min(limit, filled + ReceiveBuffer.SmallSize));
                int room = Math.Min(memory.Length, limit - filled);
                var result = await socket.ReceiveAsync(memory.Slice(0, room), ct).ConfigureAwait(false);
                if (result.MessageType == WebSocketMessageType.Close) {
                    return Closed;
                }
                if (result.MessageType != WebSocketMessageType.Binary) {
                    return Violation;
                }
                buffer.Advance(result.Count);
                filled += result.Count;
                if (result.EndOfMessage) {
                    return filled;
                }
            }
        }
    }
}
