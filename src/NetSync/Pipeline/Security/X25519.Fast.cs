#if NET8_0_OR_GREATER
using System;
using System.Buffers.Binary;

namespace NetSync.Pipeline.Security {
    // The same function with 5 limbs of 51 bits and 128-bit products: about ten times
    // faster than the portable form, which matters to servers that accept many connections.
    // Constant time, like the portable form. Both are checked against the same RFC vectors.
    internal static partial class X25519 {
        private const ulong Mask51 = (1UL << 51) - 1;

        private struct Fe {
            public ulong V0, V1, V2, V3, V4;
        }

        private static void ScalarMultiplyFast(ReadOnlySpan<byte> scalar, ReadOnlySpan<byte> point, Span<byte> result) {
            Span<byte> z = stackalloc byte[KeySize];
            scalar.Slice(0, KeySize).CopyTo(z);
            z[31] = (byte)((z[31] & 127) | 64);
            z[0] &= 248;

            Fe x = Expand(point);
            Fe a = default, b = x, c = default, d = default, e, f;
            a.V0 = 1;
            d.V0 = 1;
            Fe constant = default;
            constant.V0 = 121665;

            for (int i = 254; i >= 0; i--) {
                ulong bit = (ulong)((z[i >> 3] >> (i & 7)) & 1);
                Swap(ref a, ref b, bit);
                Swap(ref c, ref d, bit);
                e = Add(a, c);
                a = Subtract(a, c);
                c = Add(b, d);
                b = Subtract(b, d);
                d = Multiply(e, e);
                f = Multiply(a, a);
                a = Multiply(c, a);
                c = Multiply(b, e);
                e = Add(a, c);
                a = Subtract(a, c);
                b = Multiply(a, a);
                c = Subtract(d, f);
                a = Multiply(c, constant);
                a = Add(a, d);
                c = Multiply(c, a);
                a = Multiply(d, f);
                d = Multiply(b, x);
                b = Multiply(e, e);
                Swap(ref a, ref b, bit);
                Swap(ref c, ref d, bit);
            }

            c = Invert(c);
            a = Multiply(a, c);
            Contract(a, result);
            z.Clear();
        }

        private static Fe Expand(ReadOnlySpan<byte> n) {
            Fe o;
            o.V0 = BinaryPrimitives.ReadUInt64LittleEndian(n) & Mask51;
            o.V1 = (BinaryPrimitives.ReadUInt64LittleEndian(n.Slice(6)) >> 3) & Mask51;
            o.V2 = (BinaryPrimitives.ReadUInt64LittleEndian(n.Slice(12)) >> 6) & Mask51;
            o.V3 = (BinaryPrimitives.ReadUInt64LittleEndian(n.Slice(19)) >> 1) & Mask51;
            o.V4 = (BinaryPrimitives.ReadUInt64LittleEndian(n.Slice(24)) >> 12) & Mask51;
            return o;
        }

        private static void Contract(Fe t, Span<byte> o) {
            for (int round = 0; round < 2; round++) {
                Carry(ref t);
            }
            // Now t is in [0, 2^255 + small): adding 19 and carrying tells whether t >= p.
            t.V0 += 19;
            Carry(ref t);
            // t - 19 + p... expressed without negative numbers: add 2^255 - 19 limb by limb and drop bit 255.
            t.V0 += (1UL << 51) - 19;
            t.V1 += (1UL << 51) - 1;
            t.V2 += (1UL << 51) - 1;
            t.V3 += (1UL << 51) - 1;
            t.V4 += (1UL << 51) - 1;
            t.V1 += t.V0 >> 51;
            t.V0 &= Mask51;
            t.V2 += t.V1 >> 51;
            t.V1 &= Mask51;
            t.V3 += t.V2 >> 51;
            t.V2 &= Mask51;
            t.V4 += t.V3 >> 51;
            t.V3 &= Mask51;
            t.V4 &= Mask51;

            BinaryPrimitives.WriteUInt64LittleEndian(o, t.V0 | (t.V1 << 51));
            BinaryPrimitives.WriteUInt64LittleEndian(o.Slice(8), (t.V1 >> 13) | (t.V2 << 38));
            BinaryPrimitives.WriteUInt64LittleEndian(o.Slice(16), (t.V2 >> 26) | (t.V3 << 25));
            BinaryPrimitives.WriteUInt64LittleEndian(o.Slice(24), (t.V3 >> 39) | (t.V4 << 12));
        }

        private static void Carry(ref Fe t) {
            t.V1 += t.V0 >> 51;
            t.V0 &= Mask51;
            t.V2 += t.V1 >> 51;
            t.V1 &= Mask51;
            t.V3 += t.V2 >> 51;
            t.V2 &= Mask51;
            t.V4 += t.V3 >> 51;
            t.V3 &= Mask51;
            t.V0 += 19 * (t.V4 >> 51);
            t.V4 &= Mask51;
        }

        private static void Swap(ref Fe p, ref Fe q, ulong bit) {
            ulong mask = 0UL - bit;
            ulong t;
            t = mask & (p.V0 ^ q.V0);
            p.V0 ^= t;
            q.V0 ^= t;
            t = mask & (p.V1 ^ q.V1);
            p.V1 ^= t;
            q.V1 ^= t;
            t = mask & (p.V2 ^ q.V2);
            p.V2 ^= t;
            q.V2 ^= t;
            t = mask & (p.V3 ^ q.V3);
            p.V3 ^= t;
            q.V3 ^= t;
            t = mask & (p.V4 ^ q.V4);
            p.V4 ^= t;
            q.V4 ^= t;
        }

        private static Fe Add(in Fe a, in Fe b) {
            Fe o;
            o.V0 = a.V0 + b.V0;
            o.V1 = a.V1 + b.V1;
            o.V2 = a.V2 + b.V2;
            o.V3 = a.V3 + b.V3;
            o.V4 = a.V4 + b.V4;
            return o;
        }

        /// <summary>a − b, kept non-negative by adding a multiple of p first; both inputs below 2^54.</summary>
        private static Fe Subtract(in Fe a, in Fe b) {
            const ulong first = (1UL << 54) - 152; // 8·(2^51 − 19)
            const ulong rest = (1UL << 54) - 8;    // 8·(2^51 − 1)
            Fe o;
            o.V0 = a.V0 + first - b.V0;
            o.V1 = a.V1 + rest - b.V1;
            o.V2 = a.V2 + rest - b.V2;
            o.V3 = a.V3 + rest - b.V3;
            o.V4 = a.V4 + rest - b.V4;
            return o;
        }

        private static Fe Multiply(in Fe a, in Fe b) {
            ulong r0 = a.V0, r1 = a.V1, r2 = a.V2, r3 = a.V3, r4 = a.V4;
            ulong s0 = b.V0, s1 = b.V1, s2 = b.V2, s3 = b.V3, s4 = b.V4;
            // Limbs 1..4 of a, times 19: the part of each product that wraps around 2^255.
            ulong w1 = r1 * 19, w2 = r2 * 19, w3 = r3 * 19, w4 = r4 * 19;

            // Five 128-bit column sums (high, low), built from 64×64→128 products.
            ulong h0 = 0, l0 = 0, h1 = 0, l1 = 0, h2 = 0, l2 = 0, h3 = 0, l3 = 0, h4 = 0, l4 = 0;
            Accumulate(ref h0, ref l0, r0, s0);
            Accumulate(ref h0, ref l0, w4, s1);
            Accumulate(ref h0, ref l0, w1, s4);
            Accumulate(ref h0, ref l0, w2, s3);
            Accumulate(ref h0, ref l0, w3, s2);

            Accumulate(ref h1, ref l1, r0, s1);
            Accumulate(ref h1, ref l1, r1, s0);
            Accumulate(ref h1, ref l1, w4, s2);
            Accumulate(ref h1, ref l1, w2, s4);
            Accumulate(ref h1, ref l1, w3, s3);

            Accumulate(ref h2, ref l2, r0, s2);
            Accumulate(ref h2, ref l2, r2, s0);
            Accumulate(ref h2, ref l2, r1, s1);
            Accumulate(ref h2, ref l2, w4, s3);
            Accumulate(ref h2, ref l2, w3, s4);

            Accumulate(ref h3, ref l3, r0, s3);
            Accumulate(ref h3, ref l3, r3, s0);
            Accumulate(ref h3, ref l3, r1, s2);
            Accumulate(ref h3, ref l3, r2, s1);
            Accumulate(ref h3, ref l3, w4, s4);

            Accumulate(ref h4, ref l4, r0, s4);
            Accumulate(ref h4, ref l4, r4, s0);
            Accumulate(ref h4, ref l4, r3, s1);
            Accumulate(ref h4, ref l4, r1, s3);
            Accumulate(ref h4, ref l4, r2, s2);

            Fe o;
            ulong carry;
            o.V0 = l0 & Mask51;
            carry = (l0 >> 51) | (h0 << 13);
            AddCarry(ref h1, ref l1, carry);
            o.V1 = l1 & Mask51;
            carry = (l1 >> 51) | (h1 << 13);
            AddCarry(ref h2, ref l2, carry);
            o.V2 = l2 & Mask51;
            carry = (l2 >> 51) | (h2 << 13);
            AddCarry(ref h3, ref l3, carry);
            o.V3 = l3 & Mask51;
            carry = (l3 >> 51) | (h3 << 13);
            AddCarry(ref h4, ref l4, carry);
            o.V4 = l4 & Mask51;
            carry = (l4 >> 51) | (h4 << 13);
            o.V0 += carry * 19;
            carry = o.V0 >> 51;
            o.V0 &= Mask51;
            o.V1 += carry;
            return o;
        }

        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
        private static void Accumulate(ref ulong high, ref ulong low, ulong x, ulong y) {
            ulong productHigh = Math.BigMul(x, y, out ulong productLow);
            low += productLow;
            high += productHigh + (low < productLow ? 1UL : 0UL);
        }

        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
        private static void AddCarry(ref ulong high, ref ulong low, ulong value) {
            low += value;
            high += low < value ? 1UL : 0UL;
        }

        private static Fe Invert(in Fe i) {
            Fe c = i;
            for (int a = 253; a >= 0; a--) {
                c = Multiply(c, c);
                if (a != 2 && a != 4) {
                    c = Multiply(c, i);
                }
            }
            return c;
        }
    }
}
#endif
