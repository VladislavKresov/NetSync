using System;
using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;

namespace NetSync.Pipeline.Security {
    /// <summary>
    /// Session keys of protocol v4. Every keyed connection runs an ephemeral X25519 exchange,
    /// so both sides contribute fresh randomness to every session; what authenticates it is
    /// mixed into the same derivation:
    /// - PresharedKey: the shared key — only holders of it arrive at the same session keys;
    /// - a server identity: a second exchange between the client's ephemeral key and the
    ///   server's long-term key — only the holder of that private key arrives at them.
    ///
    /// secret = HKDF-SHA256(salt = SHA-256(transcript), ikm = ee ‖ [es] ‖ [psk], info = "NetSync/4 session")
    ///
    /// The transcript covers every negotiated value, so none of them can be altered in
    /// transit without the two sides ending up with different keys.
    /// </summary>
    internal static class HandshakeKeys {
        public const int SecretSize = 32;

        private static readonly byte[] TranscriptLabel = Encoding.ASCII.GetBytes("NetSync/4 handshake");
        private static readonly byte[] SessionInfo = Encoding.ASCII.GetBytes("NetSync/4 session");

        /// <summary>Everything both sides agreed on, in one canonical byte string.</summary>
        public static byte[] Transcript(long token, uint configHash, byte keyExchange, byte offeredSuites, ulong appId, ushort appVersion,
                                        uint offeredFeatures, ReadOnlySpan<byte> clientPublicKey, CipherSuite chosenSuite,
                                        uint agreedFeatures, ReadOnlySpan<byte> serverPublicKey, ReadOnlySpan<byte> identityKey) {
            var transcript = new byte[TranscriptLabel.Length + 8 + 4 + 1 + 1 + 8 + 2 + 4 + 1 + clientPublicKey.Length + 1 + 4 +
                                      1 + serverPublicKey.Length + 1 + identityKey.Length];
            int at = 0;
            TranscriptLabel.CopyTo(transcript, at);
            at += TranscriptLabel.Length;
            BinaryPrimitives.WriteInt64BigEndian(transcript.AsSpan(at), token);
            at += 8;
            BinaryPrimitives.WriteUInt32BigEndian(transcript.AsSpan(at), configHash);
            at += 4;
            transcript[at++] = keyExchange;
            transcript[at++] = offeredSuites;
            BinaryPrimitives.WriteUInt64BigEndian(transcript.AsSpan(at), appId);
            at += 8;
            BinaryPrimitives.WriteUInt16BigEndian(transcript.AsSpan(at), appVersion);
            at += 2;
            BinaryPrimitives.WriteUInt32BigEndian(transcript.AsSpan(at), offeredFeatures);
            at += 4;
            at = WriteBlock(transcript, at, clientPublicKey);
            transcript[at++] = (byte)chosenSuite;
            BinaryPrimitives.WriteUInt32BigEndian(transcript.AsSpan(at), agreedFeatures);
            at += 4;
            at = WriteBlock(transcript, at, serverPublicKey);
            WriteBlock(transcript, at, identityKey);
            return transcript;
        }

        private static int WriteBlock(byte[] destination, int at, ReadOnlySpan<byte> block) {
            destination[at++] = (byte)block.Length;
            block.CopyTo(destination.AsSpan(at));
            return at + block.Length;
        }

        /// <summary>
        /// The session secret. <paramref name="identityShared"/> and <paramref name="presharedKey"/>
        /// are empty when the connection doesn't use them.
        /// </summary>
        public static byte[] SessionSecret(ReadOnlySpan<byte> transcript, ReadOnlySpan<byte> ephemeralShared,
                                           ReadOnlySpan<byte> identityShared, ReadOnlySpan<byte> presharedKey) {
            var material = new byte[ephemeralShared.Length + identityShared.Length + presharedKey.Length];
            ephemeralShared.CopyTo(material);
            identityShared.CopyTo(material.AsSpan(ephemeralShared.Length));
            presharedKey.CopyTo(material.AsSpan(ephemeralShared.Length + identityShared.Length));
            byte[] salt;
            using (var sha = SHA256.Create()) {
                salt = sha.ComputeHash(transcript.ToArray());
            }
            try {
                return Hkdf.DeriveKey(material, salt, SessionInfo, SecretSize);
            }
            finally {
                Array.Clear(material, 0, material.Length);
            }
        }
    }
}
