using System;
using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Threading;
using NetSync.Internal;
using NetSync.Peers;

namespace NetSync.Pipeline.Security {
    /// <summary>
    /// Authenticates every post-handshake packet on one link of an encrypted connection.
    ///
    /// Envelope: [MsgSecure:1][counter:8 BE][tag:16][inner peer message…]
    /// - AesCbcHmacSha256: tag = HMAC-SHA256(directionKey, slot ‖ counter ‖ inner)[0..16]
    /// - AesGcm: tag = GMAC(directionKey, nonce = slot ‖ 0 0 0 ‖ counter, aad = inner)
    ///
    /// This closes what payload encryption alone leaves open: the inner message's
    /// header — channel, flags, sequence number, and whole ACK/Bye/Degrade messages —
    /// is covered by the tag, so it can't be forged (e.g. fake ACKs that make a
    /// "reliable" channel lose data) or moved between links/channels. The 64-bit
    /// counter plus a 1024-packet sliding window rejects replays, while still accepting
    /// the reordering UDP produces. Confidentiality stays per channel (Encryption flag).
    ///
    /// Thread-safety: Wrap may be called from any thread (counter and MAC under a lock);
    /// Unwrap is called only from the link's receive thread.
    /// Allocation-free: incremental MACs into stack buffers, envelopes in pooled buffers.
    /// </summary>
    internal sealed class LinkSecurity : IDisposable {
        public const int TagSize = 16;
        public const int HeaderSize = 1 + 8 + TagSize;
        public const int ReplayWindowSize = 1024;

        public enum UnwrapResult {
            Ok,
            /// <summary>Not an envelope, or too short.</summary>
            Malformed,
            /// <summary>Authentic packet seen before (or older than the window): harmless duplicate.</summary>
            Replay,
            /// <summary>Tag mismatch: forged, tampered, or keyed differently (wrong PSK).</summary>
            BadTag
        }

        private const int MacSize = 32;
        private const int NonceSize = 12;

        private readonly byte[] _slotBytes;
        private readonly IncrementalHash? _sendHmac;
        private readonly IncrementalHash? _receiveHmac;
#if NET8_0_OR_GREATER
        private readonly AesGcm? _sendGcm;
        private readonly AesGcm? _receiveGcm;
#endif
        private readonly object _sendLock = new object();
        private readonly ulong[] _seen = new ulong[ReplayWindowSize / 64];
        private long _sendCounter;
        private ulong _highestReceived;
        private int _disposed;

        public LinkSecurity(byte slot, CipherSuite suite, byte[] sendKey, byte[] receiveKey) {
            _slotBytes = new[] { slot };
            if (suite == CipherSuite.AesGcm) {
#if NET8_0_OR_GREATER
                _sendGcm = new AesGcm(sendKey, TagSize);
                _receiveGcm = new AesGcm(receiveKey, TagSize);
#else
                throw new PlatformNotSupportedException("AES-GCM is not available on this target");
#endif
            }
            else {
                _sendHmac = IncrementalHash.CreateHMAC(HashAlgorithmName.SHA256, sendKey);
                _receiveHmac = IncrementalHash.CreateHMAC(HashAlgorithmName.SHA256, receiveKey);
            }
        }

        /// <summary>Builds the authenticated envelope around prefix‖payload (exact-size array).</summary>
        public byte[] Wrap(ReadOnlySpan<byte> prefix, ReadOnlySpan<byte> payload) {
            var pooled = WrapPooled(prefix, payload, out int length);
            try {
                return Pooled.ToArray(pooled, 0, length);
            }
            finally {
                Pooled.Return(pooled);
            }
        }

        /// <summary>
        /// Builds the envelope in a pooled buffer (<paramref name="length"/> valid bytes);
        /// the caller returns it once the transport has consumed it.
        /// </summary>
        public byte[] WrapPooled(ReadOnlySpan<byte> prefix, ReadOnlySpan<byte> payload, out int length) {
            length = HeaderSize + prefix.Length + payload.Length;
            var envelope = Pooled.Rent(length);
            envelope[0] = PeerProtocol.MsgSecure;
            prefix.CopyTo(envelope.AsSpan(HeaderSize));
            payload.CopyTo(envelope.AsSpan(HeaderSize + prefix.Length));

            Span<byte> tag = stackalloc byte[MacSize];
            lock (_sendLock) {
                if (Volatile.Read(ref _disposed) == 1) {
                    Pooled.Return(envelope);
                    throw new ObjectDisposedException(nameof(LinkSecurity));
                }
                ulong counter = (ulong)Interlocked.Increment(ref _sendCounter);
                BinaryPrimitives.WriteUInt64BigEndian(envelope.AsSpan(1, 8), counter);
                ComputeTag(send: true, envelope, 0, length, tag);
            }
            tag.Slice(0, TagSize).CopyTo(envelope.AsSpan(9, TagSize));
            return envelope;
        }

        /// <summary>Convenience form of <see cref="Unwrap"/>.</summary>
        public bool TryUnwrap(byte[] buffer, int offset, int count, out int innerOffset, out int innerCount) {
            return Unwrap(buffer, offset, count, out innerOffset, out innerCount) == UnwrapResult.Ok;
        }

        /// <summary>
        /// Verifies an envelope in place. On success outputs the inner message's position
        /// within <paramref name="buffer"/>. The replay window only advances after the tag
        /// checks out, so forged counters can't push legitimate packets out of it.
        /// </summary>
        public UnwrapResult Unwrap(byte[] buffer, int offset, int count, out int innerOffset, out int innerCount) {
            innerOffset = 0;
            innerCount = 0;
            if (count <= HeaderSize || buffer[offset] != PeerProtocol.MsgSecure || Volatile.Read(ref _disposed) == 1) {
                return UnwrapResult.Malformed;
            }

            ulong counter = BinaryPrimitives.ReadUInt64BigEndian(buffer.AsSpan(offset + 1, 8));
            if (counter == 0) {
                return UnwrapResult.Malformed;
            }
            if (IsReplayOrTooOld(counter)) {
                return UnwrapResult.Replay;
            }
            Span<byte> expected = stackalloc byte[MacSize];
            try {
                ComputeTag(send: false, buffer, offset, count, expected);
            }
            catch (ObjectDisposedException) {
                return UnwrapResult.Malformed; // the connection closed while this packet was in flight
            }
            if (!ConnectionCrypto.FixedTimeEquals(expected.Slice(0, TagSize), buffer.AsSpan(offset + 9, TagSize))) {
                return UnwrapResult.BadTag;
            }
            MarkReceived(counter);

            innerOffset = offset + HeaderSize;
            innerCount = count - HeaderSize;
            return UnwrapResult.Ok;
        }

        /// <summary>Tag over slot ‖ counter ‖ inner of the envelope at buffer[offset..offset+count).</summary>
        private void ComputeTag(bool send, byte[] envelope, int offset, int count, Span<byte> destination) {
#if NET8_0_OR_GREATER
            var gcm = send ? _sendGcm : _receiveGcm;
            if (gcm != null) {
                // GMAC: slot and counter go into the nonce (unique per key and link), the
                // inner message is the associated data. Computing the expected tag by
                // "encrypting" nothing lets us compare in constant time, without exceptions.
                Span<byte> nonce = stackalloc byte[NonceSize];
                nonce.Clear();
                nonce[0] = _slotBytes[0];
                envelope.AsSpan(offset + 1, 8).CopyTo(nonce.Slice(4));
                var inner = new ReadOnlySpan<byte>(envelope, offset + HeaderSize, count - HeaderSize);
                gcm.Encrypt(nonce, ReadOnlySpan<byte>.Empty, Span<byte>.Empty, destination.Slice(0, TagSize), inner);
                return;
            }
#endif
            var mac = send ? _sendHmac! : _receiveHmac!;
            mac.AppendData(_slotBytes, 0, 1);
            mac.AppendData(envelope, offset + 1, 8);
            mac.AppendData(envelope, offset + HeaderSize, count - HeaderSize);
            mac.TryGetHashAndReset(destination, out _);
        }

        private bool IsReplayOrTooOld(ulong counter) {
            if (counter > _highestReceived) {
                return false;
            }
            if (_highestReceived - counter >= ReplayWindowSize) {
                return true;
            }
            int bit = (int)(counter % ReplayWindowSize);
            return (_seen[bit >> 6] & (1UL << (bit & 63))) != 0;
        }

        private void MarkReceived(ulong counter) {
            if (counter > _highestReceived) {
                ulong advance = counter - _highestReceived;
                if (advance >= ReplayWindowSize) {
                    Array.Clear(_seen, 0, _seen.Length);
                }
                else {
                    for (ulong c = _highestReceived + 1; c < counter; c++) {
                        int skipped = (int)(c % ReplayWindowSize);
                        _seen[skipped >> 6] &= ~(1UL << (skipped & 63));
                    }
                }
                _highestReceived = counter;
            }
            int bit = (int)(counter % ReplayWindowSize);
            _seen[bit >> 6] |= 1UL << (bit & 63);
        }

        public void Dispose() {
            lock (_sendLock) {
                if (Interlocked.Exchange(ref _disposed, 1) == 1) {
                    return;
                }
                _sendHmac?.Dispose();
                _receiveHmac?.Dispose();
#if NET8_0_OR_GREATER
                _sendGcm?.Dispose();
                _receiveGcm?.Dispose();
#endif
            }
        }
    }
}
