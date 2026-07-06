using System;
using System.Buffers.Binary;
using System.Diagnostics;
using NetSync.Pipeline.Fragmentation;
using NetSync.Transports;

namespace NetSync.Benchmarks {
    /// <summary>
    /// Micro-benchmarks for the frame-building hot path: 2.0 pooled gather framing vs
    /// the 1.x style (allocate peer message, then allocate the transport frame around it).
    /// Manual harness (Stopwatch + GC counters) — no external packages.
    /// </summary>
    internal static class FramingBenchmarks {
        private const int Iterations = 1_000_000;

        public static void Run() {
            Console.WriteLine($"Framing micro-benchmarks, {Iterations:N0} iterations each");
            Console.WriteLine($"{"case",-52} {"ns/op",10} {"B alloc/op",12}");
            Console.WriteLine(new string('-', 78));

            foreach (int size in new[] { 64, 1200, 16384 }) {
                var payload = new byte[size];
                var prefix = new byte[] { 0x10, 1 };
                new Random(42).NextBytes(payload);

                Measure($"1.x double-alloc TCP frame ({size} B)", () => LegacyDoubleAllocFrame(prefix, payload));
                Measure($"2.0 pooled gather TCP frame ({size} B)", () => PooledGatherFrame(prefix, payload));
                Measure($"2.0 pooled UDP packet ({size} B)", () => PooledUdpPacket(prefix, payload));
                Console.WriteLine();
            }

            MeasureFragmentation();
        }

        private static void Measure(string label, Action action) {
            // Warmup (also promotes pool buffers so steady state is measured).
            for (int i = 0; i < 10_000; i++) {
                action();
            }
            GC.Collect();
            GC.WaitForPendingFinalizers();

            long allocatedBefore = GC.GetAllocatedBytesForCurrentThread();
            var stopwatch = Stopwatch.StartNew();
            for (int i = 0; i < Iterations; i++) {
                action();
            }
            stopwatch.Stop();
            long allocated = GC.GetAllocatedBytesForCurrentThread() - allocatedBefore;

            double nsPerOp = stopwatch.Elapsed.TotalMilliseconds * 1_000_000 / Iterations;
            double bytesPerOp = allocated / (double)Iterations;
            Console.WriteLine($"{label,-52} {nsPerOp,10:N1} {bytesPerOp,12:N1}");
        }

        private static int LegacyDoubleAllocFrame(byte[] prefix, byte[] payload) {
            // Peer layer wraps payload…
            var peerMessage = new byte[prefix.Length + payload.Length];
            prefix.CopyTo(peerMessage, 0);
            Buffer.BlockCopy(payload, 0, peerMessage, prefix.Length, payload.Length);
            // …then the transport wraps it again.
            var frame = new byte[5 + peerMessage.Length];
            frame[0] = 0x10;
            BinaryPrimitives.WriteInt32BigEndian(frame.AsSpan(1, 4), peerMessage.Length);
            Buffer.BlockCopy(peerMessage, 0, frame, 5, peerMessage.Length);
            return frame.Length;
        }

        private static int PooledGatherFrame(byte[] prefix, byte[] payload) {
            var frame = PacketProtocol.RentTcpDataFrame(prefix, payload, out int length);
            PacketProtocol.Return(frame);
            return length;
        }

        private static int PooledUdpPacket(byte[] prefix, byte[] payload) {
            var packet = PacketProtocol.RentUdpDataPacket(prefix, payload, out int length);
            PacketProtocol.Return(packet);
            return length;
        }

        private static void MeasureFragmentation() {
            var big = new byte[1024 * 1024];
            const int rounds = 200;

            GC.Collect();
            long allocatedBefore = GC.GetAllocatedBytesForCurrentThread();
            var stopwatch = Stopwatch.StartNew();
            long written = 0;
            for (int r = 0; r < rounds; r++) {
                uint seq = PacketFragmenter.NextSequenceId();
                int total = (big.Length + PacketFragmenter.FragmentSizeTcp - 1) / PacketFragmenter.FragmentSizeTcp;
                for (uint i = 0; i < total; i++) {
                    int offset = (int)(i * PacketFragmenter.FragmentSizeTcp);
                    int chunk = Math.Min(PacketFragmenter.FragmentSizeTcp, big.Length - offset);
                    var frame = PacketProtocol.RentTcpFragmentFrame(seq, i, (uint)total, big.AsSpan(offset, chunk), out int length);
                    written += length;
                    PacketProtocol.Return(frame);
                }
            }
            stopwatch.Stop();
            long allocated = GC.GetAllocatedBytesForCurrentThread() - allocatedBefore;

            double mbPerSec = rounds / stopwatch.Elapsed.TotalSeconds; // 1 MB per round
            Console.WriteLine($"Fragment 1 MB into pooled TCP frames: {mbPerSec:N0} MB/s framed, {allocated / (double)rounds:N0} B allocated per 1 MB");
            _ = written;
        }
    }
}
