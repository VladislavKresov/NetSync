using System;
using System.Linq;
using NetSync.Pipeline.Fragmentation;
using Xunit;

namespace NetSync.Tests {
    public class FragmentationTests {
        [Theory]
        [InlineData(0)]
        [InlineData(1)]
        [InlineData(1199)]
        [InlineData(1200)]
        [InlineData(1201)]
        [InlineData(100_000)]
        public void Fragment_And_Reassemble_Roundtrip(int size) {
            var data = MakeData(size);
            uint seqId = PacketFragmenter.NextSequenceId();
            var fragments = PacketFragmenter.Fragment(data, seqId, PacketFragmenter.FragmentSizeUdp);
            var buffer = new FragmentBuffer();

            byte[]? result = null;
            foreach (var fragment in fragments) {
                Assert.True(PacketFragmenter.TryUnwrap(fragment, out uint id, out uint index, out uint total, out var payload));
                Assert.Equal(seqId, id);
                result = buffer.AddFragment(id, index, total, payload);
            }

            Assert.NotNull(result);
            Assert.Equal(data, result);
            Assert.Equal(0, buffer.ActiveCount);
        }

        [Fact]
        public void Reassembly_Works_Out_Of_Order() {
            var data = MakeData(50_000);
            uint seqId = 42;
            var fragments = PacketFragmenter.Fragment(data, seqId, 1000);
            var shuffled = fragments.OrderBy(_ => Guid.NewGuid()).ToArray();
            var buffer = new FragmentBuffer();

            byte[]? result = null;
            foreach (var fragment in shuffled) {
                PacketFragmenter.TryUnwrap(fragment, out uint id, out uint index, out uint total, out var payload);
                var r = buffer.AddFragment(id, index, total, payload);
                if (r != null) {
                    result = r;
                }
            }

            Assert.Equal(data, result);
        }

        [Fact]
        public void Duplicate_Fragments_Are_Ignored() {
            var data = MakeData(3000);
            var fragments = PacketFragmenter.Fragment(data, 7, 1000);
            var buffer = new FragmentBuffer();

            PacketFragmenter.TryUnwrap(fragments[0], out var id, out var i0, out var total, out var p0);
            Assert.Null(buffer.AddFragment(id, i0, total, p0));
            Assert.Null(buffer.AddFragment(id, i0, total, p0)); // duplicate

            PacketFragmenter.TryUnwrap(fragments[1], out _, out var i1, out _, out var p1);
            Assert.Null(buffer.AddFragment(id, i1, total, p1));

            PacketFragmenter.TryUnwrap(fragments[2], out _, out var i2, out _, out var p2);
            var result = buffer.AddFragment(id, i2, total, p2);

            Assert.Equal(data, result);
        }

        [Fact]
        public void NonFragment_Packet_Is_Not_Unwrapped() {
            var packet = new byte[20]; // starts with 0x00, not the Fragment marker
            Assert.False(PacketFragmenter.TryUnwrap(packet, out _, out _, out _, out _));
        }

        [Fact]
        public void Invalid_Header_Is_Rejected() {
            // index >= total must be rejected
            var fragment = PacketFragmenter.Fragment(MakeData(10), 1, 100)[0];
            fragment[5] = 0xFF; // corrupt index to a huge value
            Assert.False(PacketFragmenter.TryUnwrap(fragment, out _, out _, out _, out _));
        }

        [Fact]
        public void Concurrent_Packet_Cap_Evicts_Oldest_Not_Crash() {
            var buffer = new FragmentBuffer { MaxConcurrentPackets = 4 };
            for (uint seq = 0; seq < 100; seq++) {
                // first fragment of a 2-fragment packet — never completes
                buffer.AddFragment(seq, 0, 2, new byte[10]);
            }
            Assert.True(buffer.ActiveCount <= 4);
        }

        private static byte[] MakeData(int size) {
            var data = new byte[size];
            new Random(1234).NextBytes(data);
            return data;
        }
    }
}
