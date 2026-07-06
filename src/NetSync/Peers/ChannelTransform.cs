using System;
using NetSync.Diagnostics;
using NetSync.Pipeline.Compression;
using NetSync.Pipeline.Security;

namespace NetSync.Peers {
    /// <summary>
    /// Per-channel payload pipeline: compress, then encrypt (compressing ciphertext is
    /// pointless). Runs before reliability/fragmentation, which treat the result as
    /// opaque bytes. Channels with neither feature skip this entirely — the extra
    /// flags byte only exists on channels that opted in.
    ///
    /// Transformed layout: [flags:1][body]
    /// </summary>
    internal static class ChannelTransform {
        private const byte FlagCompressed = 0x01;
        private const byte FlagEncrypted = 0x02;

        /// <summary>Payloads smaller than this are never worth deflating.</summary>
        public const int CompressionThreshold = 128;

        public static ReadOnlyMemory<byte> Encode(ChannelConfig channel, ConnectionCrypto? crypto, ReadOnlyMemory<byte> data) {
            if (!channel.NeedsTransform) {
                return data;
            }

            byte flags = 0;
            ReadOnlyMemory<byte> body = data;

            if (channel.Compression && data.Length >= CompressionThreshold) {
                var compressed = Compressor.Compress(data.Span);
                if (compressed.Length < data.Length) {
                    body = compressed;
                    flags |= FlagCompressed;
                } // else: incompressible — send as-is, flag stays clear
            }

            if (channel.Encryption) {
                if (crypto == null) {
                    throw new InvalidOperationException("Encryption keys are not established yet");
                }
                body = crypto.Encrypt(body.Span);
                flags |= FlagEncrypted;
            }

            var result = new byte[1 + body.Length];
            result[0] = flags;
            body.Span.CopyTo(result.AsSpan(1));
            return result;
        }

        /// <summary>Returns null when the packet must be dropped (tampered, malformed, downgraded).</summary>
        public static byte[]? Decode(ChannelConfig channel, ConnectionCrypto? crypto, byte[] payload, INetLogger logger) {
            if (!channel.NeedsTransform) {
                return payload;
            }
            if (payload.Length < 1) {
                return null;
            }

            byte flags = payload[0];
            ReadOnlySpan<byte> body = payload.AsSpan(1);
            byte[]? decrypted = null;

            if (channel.Encryption) {
                // Downgrade protection: an encrypted channel never accepts plaintext.
                if ((flags & FlagEncrypted) == 0 || crypto == null) {
                    logger.Warning("Dropping packet: encrypted channel received unencrypted data");
                    return null;
                }
                decrypted = crypto.Decrypt(body);
                if (decrypted == null) {
                    logger.Warning("Dropping packet: authentication failed (tampered or key mismatch)");
                    return null;
                }
                body = decrypted;
            }
            else if ((flags & FlagEncrypted) != 0) {
                return null; // encrypted data on a plaintext channel — config mismatch
            }

            if ((flags & FlagCompressed) != 0) {
                return Compressor.Decompress(body, logger);
            }
            return decrypted ?? body.ToArray();
        }
    }
}
