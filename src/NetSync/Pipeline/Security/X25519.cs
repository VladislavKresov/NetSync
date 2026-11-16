using System;
using System.Security.Cryptography;

namespace NetSync.Pipeline.Security {
    /// <summary>
    /// X25519 Diffie-Hellman (RFC 7748) in portable managed code: the same key exchange on
    /// every runtime, including the ones whose class library has no elliptic curves (Unity).
    ///
    /// Field elements are 16 limbs of 16 bits in 64-bit integers (the TweetNaCl layout):
    /// compact, and every operation runs in constant time — no branch or array index
    /// depends on secret data.
    /// </summary>
    internal static class X25519 {
        public const int KeySize = 32;

        private const int Limbs = 16;

        /// <summary>Fills <paramref name="privateKey"/> with a new random private key.</summary>
        public static void GeneratePrivateKey(Span<byte> privateKey) {
            RandomNumberGenerator.Fill(privateKey.Slice(0, KeySize));
        }

        /// <summary>The public key for a private key: scalar multiplication of the base point (u = 9).</summary>
        public static void PublicKey(ReadOnlySpan<byte> privateKey, Span<byte> publicKey) {
            Span<byte> basePoint = stackalloc byte[KeySize];
            basePoint.Clear();
            basePoint[0] = 9;
            ScalarMultiply(privateKey, basePoint, publicKey);
        }

        /// <summary>
        /// The shared secret of our private key and their public key. False when the result is
        /// all zero (their key is a low-order point): such a secret must not be used.
        /// </summary>
        public static bool TrySharedSecret(ReadOnlySpan<byte> privateKey, ReadOnlySpan<byte> theirPublicKey, Span<byte> sharedSecret) {
            ScalarMultiply(privateKey, theirPublicKey, sharedSecret);
            int accumulated = 0;
            for (int i = 0; i < KeySize; i++) {
                accumulated |= sharedSecret[i];
            }
            return accumulated != 0;
        }

        /// <summary>RFC 7748 X25519(k, u): the scalar is clamped, the top bit of u is ignored.</summary>
        public static void ScalarMultiply(ReadOnlySpan<byte> scalar, ReadOnlySpan<byte> point, Span<byte> result) {
            if (scalar.Length < KeySize || point.Length < KeySize || result.Length < KeySize) {
                throw new ArgumentException($"X25519 keys are {KeySize} bytes");
            }
            Span<byte> z = stackalloc byte[KeySize];
            scalar.Slice(0, KeySize).CopyTo(z);
            z[31] = (byte)((z[31] & 127) | 64);
            z[0] &= 248;

            Span<long> x = stackalloc long[Limbs];
            Span<long> a = stackalloc long[Limbs];
            Span<long> b = stackalloc long[Limbs];
            Span<long> c = stackalloc long[Limbs];
            Span<long> d = stackalloc long[Limbs];
            Span<long> e = stackalloc long[Limbs];
            Span<long> f = stackalloc long[Limbs];
            Span<long> constant = stackalloc long[Limbs]; // 121665
            constant.Clear();
            constant[0] = 0xDB41;
            constant[1] = 1;

            Unpack(x, point);
            x.CopyTo(b);
            a.Clear();
            c.Clear();
            d.Clear();
            a[0] = 1;
            d[0] = 1;

            for (int i = 254; i >= 0; i--) {
                long bit = (z[i >> 3] >> (i & 7)) & 1;
                Swap(a, b, bit);
                Swap(c, d, bit);
                Add(e, a, c);
                Subtract(a, a, c);
                Add(c, b, d);
                Subtract(b, b, d);
                Multiply(d, e, e);
                Multiply(f, a, a);
                Multiply(a, c, a);
                Multiply(c, b, e);
                Add(e, a, c);
                Subtract(a, a, c);
                Multiply(b, a, a);
                Subtract(c, d, f);
                Multiply(a, c, constant);
                Add(a, a, d);
                Multiply(c, c, a);
                Multiply(a, d, f);
                Multiply(d, b, x);
                Multiply(b, e, e);
                Swap(a, b, bit);
                Swap(c, d, bit);
            }

            Invert(c, c);
            Multiply(a, a, c);
            Pack(result, a);

            z.Clear();
            x.Clear();
            a.Clear();
            b.Clear();
            c.Clear();
            d.Clear();
            e.Clear();
            f.Clear();
        }

        private static void Carry(Span<long> o) {
            for (int i = 0; i < Limbs; i++) {
                o[i] += 1L << 16;
                long carry = o[i] >> 16;
                if (i < 15) {
                    o[i + 1] += carry - 1;
                }
                else {
                    o[0] += 38 * (carry - 1); // 2^256 = 38 (mod p)
                }
                o[i] -= carry << 16;
            }
        }

        /// <summary>Swaps p and q when bit is 1, in constant time.</summary>
        private static void Swap(Span<long> p, Span<long> q, long bit) {
            long mask = ~(bit - 1);
            for (int i = 0; i < Limbs; i++) {
                long t = mask & (p[i] ^ q[i]);
                p[i] ^= t;
                q[i] ^= t;
            }
        }

        private static void Pack(Span<byte> o, ReadOnlySpan<long> n) {
            Span<long> m = stackalloc long[Limbs];
            Span<long> t = stackalloc long[Limbs];
            n.CopyTo(t);
            Carry(t);
            Carry(t);
            Carry(t);
            for (int round = 0; round < 2; round++) {
                m[0] = t[0] - 0xffed;
                for (int i = 1; i < 15; i++) {
                    m[i] = t[i] - 0xffff - ((m[i - 1] >> 16) & 1);
                    m[i - 1] &= 0xffff;
                }
                m[15] = t[15] - 0x7fff - ((m[14] >> 16) & 1);
                long borrow = (m[15] >> 16) & 1;
                m[14] &= 0xffff;
                Swap(t, m, 1 - borrow);
            }
            for (int i = 0; i < Limbs; i++) {
                o[2 * i] = (byte)(t[i] & 0xff);
                o[2 * i + 1] = (byte)(t[i] >> 8);
            }
            m.Clear();
            t.Clear();
        }

        private static void Unpack(Span<long> o, ReadOnlySpan<byte> n) {
            for (int i = 0; i < Limbs; i++) {
                o[i] = n[2 * i] + ((long)n[2 * i + 1] << 8);
            }
            o[15] &= 0x7fff;
        }

        private static void Add(Span<long> o, ReadOnlySpan<long> a, ReadOnlySpan<long> b) {
            for (int i = 0; i < Limbs; i++) {
                o[i] = a[i] + b[i];
            }
        }

        private static void Subtract(Span<long> o, ReadOnlySpan<long> a, ReadOnlySpan<long> b) {
            for (int i = 0; i < Limbs; i++) {
                o[i] = a[i] - b[i];
            }
        }

        /// <summary>o = a·b; o may be the same span as a or b.</summary>
        private static void Multiply(Span<long> o, ReadOnlySpan<long> a, ReadOnlySpan<long> b) {
            Span<long> t = stackalloc long[31];
            t.Clear();
            for (int i = 0; i < Limbs; i++) {
                for (int j = 0; j < Limbs; j++) {
                    t[i + j] += a[i] * b[j];
                }
            }
            for (int i = 0; i < 15; i++) {
                t[i] += 38 * t[i + 16];
            }
            t.Slice(0, Limbs).CopyTo(o);
            Carry(o);
            Carry(o);
            t.Clear();
        }

        /// <summary>o = 1/i (i^(p-2)).</summary>
        private static void Invert(Span<long> o, ReadOnlySpan<long> i) {
            Span<long> c = stackalloc long[Limbs];
            Span<long> input = stackalloc long[Limbs];
            i.CopyTo(input);
            i.CopyTo(c);
            for (int a = 253; a >= 0; a--) {
                Multiply(c, c, c);
                if (a != 2 && a != 4) {
                    Multiply(c, c, input);
                }
            }
            c.CopyTo(o);
            c.Clear();
            input.Clear();
        }
    }
}
