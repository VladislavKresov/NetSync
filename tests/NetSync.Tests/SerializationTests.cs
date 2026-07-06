using System;
using System.IO;
using NetSync.Serialization;
using Xunit;

namespace NetSync.Tests {
    public class SerializationTests {
        [Fact]
        public void All_Primitives_Roundtrip() {
            var writer = new NetWriter();
            var guid = Guid.NewGuid();
            writer.WriteByte(0xAB)
                  .WriteBool(true)
                  .WriteInt16(-12345)
                  .WriteUInt16(54321)
                  .WriteInt32(int.MinValue)
                  .WriteUInt32(uint.MaxValue)
                  .WriteInt64(long.MinValue)
                  .WriteUInt64(ulong.MaxValue)
                  .WriteSingle(3.14159f)
                  .WriteDouble(Math.E)
                  .WriteString("привет, NetSync! 🚀")
                  .WriteBytes(new byte[] { 1, 2, 3 })
                  .WriteGuid(guid);

            var reader = new NetReader(writer.WrittenMemory);
            Assert.Equal(0xAB, reader.ReadByte());
            Assert.True(reader.ReadBool());
            Assert.Equal(-12345, reader.ReadInt16());
            Assert.Equal(54321, reader.ReadUInt16());
            Assert.Equal(int.MinValue, reader.ReadInt32());
            Assert.Equal(uint.MaxValue, reader.ReadUInt32());
            Assert.Equal(long.MinValue, reader.ReadInt64());
            Assert.Equal(ulong.MaxValue, reader.ReadUInt64());
            Assert.Equal(3.14159f, reader.ReadSingle());
            Assert.Equal(Math.E, reader.ReadDouble());
            Assert.Equal("привет, NetSync! 🚀", reader.ReadString());
            Assert.Equal(new byte[] { 1, 2, 3 }, reader.ReadBytes());
            Assert.Equal(guid, reader.ReadGuid());
            Assert.Equal(0, reader.Remaining);
        }

        [Theory]
        [InlineData(0u, 1)]
        [InlineData(127u, 1)]
        [InlineData(128u, 2)]
        [InlineData(16_383u, 2)]
        [InlineData(16_384u, 3)]
        [InlineData(uint.MaxValue, 5)]
        public void VarUInt_Uses_Expected_Size(uint value, int expectedBytes) {
            var writer = new NetWriter();
            writer.WriteVarUInt(value);
            Assert.Equal(expectedBytes, writer.Length);
            Assert.Equal(value, new NetReader(writer.WrittenMemory).ReadVarUInt());
        }

        [Theory]
        [InlineData(0)]
        [InlineData(-1)]
        [InlineData(63)]
        [InlineData(-64)]
        [InlineData(int.MaxValue)]
        [InlineData(int.MinValue)]
        public void VarInt_ZigZag_Roundtrips(int value) {
            var writer = new NetWriter();
            writer.WriteVarInt(value);
            Assert.Equal(value, new NetReader(writer.WrittenMemory).ReadVarInt());
        }

        [Fact]
        public void Small_Signed_Values_Are_One_Byte() {
            var writer = new NetWriter();
            writer.WriteVarInt(-64).WriteVarInt(63);
            Assert.Equal(2, writer.Length);
        }

        [Fact]
        public void Truncated_Input_Throws_Instead_Of_Garbage() {
            var writer = new NetWriter();
            writer.WriteInt64(42);
            var truncated = writer.ToArray().AsMemory(0, 5);

            var reader = new NetReader(truncated);
            Assert.Throws<EndOfStreamException>(() => reader.ReadInt64());
        }

        [Fact]
        public void Malicious_Length_Prefix_Throws() {
            var writer = new NetWriter();
            writer.WriteVarUInt(uint.MaxValue); // claims a 4 GB string follows
            var reader = new NetReader(writer.WrittenMemory);
            Assert.ThrowsAny<Exception>(() => reader.ReadString()); // checked overflow or EOS
        }

        [Fact]
        public void Writer_Grows_And_Resets() {
            var writer = new NetWriter(initialCapacity: 16);
            var big = new byte[10_000];
            new Random(5).NextBytes(big);
            writer.WriteBytes(big);
            Assert.Equal(big, new NetReader(writer.WrittenMemory).ReadBytes());

            writer.Reset();
            Assert.Equal(0, writer.Length);
            writer.WriteBool(false);
            var reader = new NetReader(writer.WrittenMemory);
            Assert.False(reader.ReadBool());
            Assert.Equal(0, reader.Remaining);
        }

        [Fact]
        public void ReadBytesMemory_Is_ZeroCopy_Slice() {
            var writer = new NetWriter();
            writer.WriteBytes(new byte[] { 7, 8, 9 });
            var reader = new NetReader(writer.WrittenMemory);
            var memory = reader.ReadBytesMemory();
            Assert.Equal(new byte[] { 7, 8, 9 }, memory.ToArray());
        }
    }
}
