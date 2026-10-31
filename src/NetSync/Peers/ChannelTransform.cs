using System;
using NetSync.Diagnostics;
using NetSync.Internal;
using NetSync.Pipeline.Compression;
using NetSync.Pipeline.Security;

namespace NetSync.Peers {
    /// <summary>
    /// Per-channel payload pipeline: compress, then encrypt (compressing ciphertext is
    /// pointless). Runs before reliability/fragmentation, which treat the result as
    /// opaque bytes. Channels with neither feature skip this entirely — the extra
    /// flags byte only exists on channels that opted in.
    ///
    /// Results come back in pooled buffers (the caller returns them). Encryption is
    /// allocation-free on net8.0; compression works in pooled buffers too, but the
    /// Deflate stream itself allocates — it is meant for bulk data.
    ///
    /// Transformed layout: [flags:1][body]
    /// </summary>
    internal static class ChannelTransform {
        private const byte FlagCompressed = 0x01;
        private const byte FlagEncrypted = 0x02;

        /// <summary>
        /// Worst-case bytes the transform adds on top of the user payload: flags byte,
        /// compression length header, cipher overhead (IV/nonce, tag, padding).
        /// </summary>
        public const int MaxOverhead = 1 + 4 + 32;

        /// <summary>Payloads smaller than this are never worth deflating.</summary>
        public const int CompressionThreshold = 128;

        /// <summary>
        /// Only for channels with <see cref="ChannelConfig.NeedsTransform"/>. Returns a pooled
        /// buffer holding <paramref name="length"/> bytes.
        /// </summary>
        public static byte[] Encode(ChannelConfig channel, ConnectionCrypto? crypto, ReadOnlySpan<byte> data, out int length) {
            byte flags = 0;
            ReadOnlySpan<byte> body = data;
            byte[]? compressed = null;

            if (channel.Compression && data.Length >= CompressionThreshold) {
                compressed = Compressor.TryCompress(data, out int compressedLength);
                if (compressed != null) {
                    body = compressed.AsSpan(0, compressedLength);
                    flags |= FlagCompressed;
                } // else: incompressible — send as-is, flag stays clear
            }

            try {
                byte[] result;
                if (channel.Encryption) {
                    if (crypto == null) {
                        throw new InvalidOperationException("Encryption keys are not established yet");
                    }
                    result = Pooled.Rent(1 + body.Length + crypto.MaxOverhead);
                    try {
                        length = 1 + crypto.Encrypt(body, result.AsSpan(1));
                    }
                    catch {
                        Pooled.Return(result);
                        throw;
                    }
                    flags |= FlagEncrypted;
                }
                else {
                    result = Pooled.Rent(1 + body.Length);
                    body.CopyTo(result.AsSpan(1));
                    length = 1 + body.Length;
                }
                result[0] = flags;
                return result;
            }
            finally {
                if (compressed != null) {
                    Pooled.Return(compressed);
                }
            }
        }

        /// <summary>
        /// Only for channels with <see cref="ChannelConfig.NeedsTransform"/>. Returns the decoded
        /// payload in a pooled buffer (<paramref name="length"/> bytes), or null when the
        /// packet must be dropped (malformed, downgraded, undecryptable).
        /// </summary>
        public static byte[]? Decode(ChannelConfig channel, ConnectionCrypto? crypto, byte[] buffer, int offset, int count, int maxMessageSize,
                                     INetLogger logger, out int length) {
            length = 0;
            if (count < 1) {
                return null;
            }

            byte flags = buffer[offset];
            // The body: buffer[bodyOffset..bodyOffset+bodyCount), replaced by the plaintext once decrypted.
            byte[] body = buffer;
            int bodyOffset = offset + 1;
            int bodyCount = count - 1;
            byte[]? decrypted = null;

            if (channel.Encryption) {
                // Downgrade protection: an encrypted channel never accepts plaintext.
                if ((flags & FlagEncrypted) == 0 || crypto == null) {
                    logger.Warning("Dropping packet: encrypted channel received unencrypted data");
                    return null;
                }
                decrypted = Pooled.Rent(bodyCount);
                int plainLength = crypto.Decrypt(new ReadOnlySpan<byte>(buffer, bodyOffset, bodyCount), decrypted);
                if (plainLength < 0) {
                    Pooled.Return(decrypted);
                    logger.Warning("Dropping packet: decryption failed (key mismatch?)");
                    return null;
                }
                body = decrypted;
                bodyOffset = 0;
                bodyCount = plainLength;
            }
            else if ((flags & FlagEncrypted) != 0) {
                return null; // encrypted data on a plaintext channel — config mismatch
            }

            if ((flags & FlagCompressed) != 0) {
                var inflated = Compressor.Decompress(body, bodyOffset, bodyCount, maxMessageSize, logger, out length);
                if (decrypted != null) {
                    Pooled.Return(decrypted);
                }
                return inflated;
            }
            length = bodyCount;
            return decrypted ?? Pooled.Copy(new ReadOnlySpan<byte>(body, bodyOffset, bodyCount));
        }
    }
}
