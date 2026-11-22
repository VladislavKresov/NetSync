using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using NetSync.Serialization;
using Xunit;

namespace NetSync.Tests {
    public class SerializationTests {
        [Fact]
        public void A_String_That_Makes_The_Buffer_Grow_Is_Written_Whole() {
            // Regression (2.0.0-rc.6): the encoder wrote into the buffer as it was before growing.
            var writer = new NetWriter(16);
            string text = new string('ж', 100); // 200 bytes of UTF-8
            writer.WriteString(text);
            Assert.Equal(text, new NetReader(writer.ToArray()).ReadString());
        }

        [Fact]
        public void Strings_Of_Every_Length_Around_The_Growth_Points_Roundtrip() {
            for (int length = 0; length <= 300; length++) {
                foreach (string unit in new[] { "a", "ж", "€", "😀" }) { // 1, 2, 3 and 4 bytes per character
                    var writer = new NetWriter(16);
                    writer.WriteByte(7);
                    string text = string.Concat(Enumerable.Repeat(unit, length));
                    writer.WriteString(text).WriteInt32(42);
                    var reader = new NetReader(writer.ToArray());
                    Assert.Equal(7, reader.ReadByte());
                    Assert.Equal(text, reader.ReadString());
                    Assert.Equal(42, reader.ReadInt32());
                }
            }
        }

        [Fact]
        public void Mixed_Writes_Roundtrip_Whatever_The_Starting_Capacity() {
            var random = new Random(3);
            for (int round = 0; round < 2000; round++) {
                var writer = new NetWriter(random.Next(1, 64));
                var expected = new List<object>();
                for (int i = random.Next(1, 30); i > 0; i--) {
                    switch (random.Next(4)) {
                        case 0:
                            var text = new string((char)random.Next(0x20, 0x500), random.Next(0, 200));
                            writer.WriteString(text);
                            expected.Add(text);
                            break;
                        case 1:
                            var bytes = new byte[random.Next(0, 300)];
                            random.NextBytes(bytes);
                            writer.WriteBytes(bytes);
                            expected.Add(bytes);
                            break;
                        case 2:
                            int number = random.Next();
                            writer.WriteInt32(number);
                            expected.Add(number);
                            break;
                        default:
                            uint varint = (uint)random.Next();
                            writer.WriteVarUInt(varint);
                            expected.Add(varint);
                            break;
                    }
                }
                var reader = new NetReader(writer.ToArray());
                foreach (var item in expected) {
                    switch (item) {
                        case string text: Assert.Equal(text, reader.ReadString()); break;
                        case byte[] bytes: Assert.Equal(bytes, reader.ReadBytes()); break;
                        case int number: Assert.Equal(number, reader.ReadInt32()); break;
                        case uint varint: Assert.Equal(varint, reader.ReadVarUInt()); break;
                    }
                }
            }
        }

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

        [Theory]
        [InlineData(uint.MaxValue)]       // doesn't even fit an int
        [InlineData(int.MaxValue - 2u)]   // position + count would overflow
        [InlineData(100u)]                // simply longer than the payload
        public void Malicious_Length_Prefix_Is_Malformed_Input(uint claimed) {
            // Callers catch EndOfStreamException as "malformed": nothing else may escape.
            var writer = new NetWriter();
            writer.WriteVarUInt(claimed);
            writer.WriteInt32(7);
            Assert.Throws<EndOfStreamException>(() => new NetReader(writer.WrittenMemory).ReadString());
            Assert.Throws<EndOfStreamException>(() => new NetReader(writer.WrittenMemory).ReadBytes());
            Assert.Throws<EndOfStreamException>(() => new NetReader(writer.WrittenMemory).ReadBytesMemory());
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
