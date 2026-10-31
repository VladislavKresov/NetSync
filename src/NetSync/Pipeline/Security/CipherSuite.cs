namespace NetSync {
    /// <summary>
    /// How an encrypted connection protects packets. Negotiated in the handshake: the
    /// client offers what its runtime supports, the server picks the best common suite.
    /// </summary>
    public enum CipherSuite : byte {
        /// <summary>No key exchange: nothing is authenticated or encrypted.</summary>
        None = 0,
        /// <summary>
        /// HMAC-SHA256 packet authentication + AES-256-CBC payload encryption. Available
        /// on every target (netstandard2.1 / Unity included) — the interoperable baseline.
        /// </summary>
        AesCbcHmacSha256 = 1,
        /// <summary>
        /// AES-256-GCM for both packet authentication (GMAC) and payload encryption —
        /// hardware-accelerated and allocation-free. Chosen when both peers run .NET 8+
        /// on a platform with AES-GCM support.
        /// </summary>
        AesGcm = 2
    }

    internal static class CipherSuites {
        public const byte MaskCbc = 1 << 0;
        public const byte MaskGcm = 1 << 1;

        public static bool GcmSupported {
            get {
#if NET8_0_OR_GREATER
                return System.Security.Cryptography.AesGcm.IsSupported;
#else
                return false;
#endif
            }
        }

        /// <summary>Suites this runtime can run, as an offer bitmask.</summary>
        public static byte LocalMask => (byte)(MaskCbc | (GcmSupported ? MaskGcm : 0));

        /// <summary>The best suite both sides support; None when there is none.</summary>
        public static CipherSuite Choose(byte offered) {
            byte common = (byte)(offered & LocalMask);
            if ((common & MaskGcm) != 0) {
                return CipherSuite.AesGcm;
            }
            return (common & MaskCbc) != 0 ? CipherSuite.AesCbcHmacSha256 : CipherSuite.None;
        }

        public static bool IsOffered(byte offered, CipherSuite suite) {
            return suite switch {
                CipherSuite.AesCbcHmacSha256 => (offered & MaskCbc) != 0,
                CipherSuite.AesGcm => (offered & MaskGcm) != 0 && GcmSupported,
                _ => false
            };
        }
    }
}
