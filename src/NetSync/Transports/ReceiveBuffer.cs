using System;
using NetSync.Internal;

namespace NetSync.Transports {
    /// <summary>
    /// A connection's receive buffer that is small while traffic is small. It starts at
    /// <see cref="SmallSize"/> — enough for many small frames per socket read — grows only
    /// when a frame bigger than that actually arrives (up to the frame limit), and drops back
    /// once it is empty again. An idle connection holds kilobytes, not the worst case: a
    /// server with ten thousand clients doesn't pay for ten thousand 64 KB frames it never sees.
    /// </summary>
    internal sealed class ReceiveBuffer : IDisposable {
        public const int SmallSize = 8 * 1024;
        private const int MinFree = 1024;

        private byte[] _buffer = Pooled.Rent(SmallSize);
        private int _start;
        private int _end;

        public byte[] Array => _buffer;
        public int Start => _start;
        public int Available => _end - _start;

        /// <summary>Marks bytes at <see cref="Start"/> as handled.</summary>
        public void Consume(int count) {
            _start += count;
        }

        /// <summary>
        /// Space for the next read. <paramref name="pendingFrameSize"/>: total size of the
        /// incomplete frame at <see cref="Start"/>, when its header is known (0 otherwise) — the
        /// buffer makes sure it will fit.
        /// </summary>
        public Memory<byte> GetReadMemory(int pendingFrameSize) {
            if (_start == _end) {
                _start = _end = 0;
                if (_buffer.Length > SmallSize && pendingFrameSize <= SmallSize) {
                    Replace(SmallSize); // the big frame is done: give the memory back
                }
            }
            int needed = Math.Max(pendingFrameSize, MinFree);
            if (_buffer.Length - _start < needed) {
                if (_buffer.Length < needed) {
                    Replace(Math.Max(needed, _buffer.Length * 2));
                }
                else {
                    Compact();
                }
            }
            else if (_buffer.Length - _end < MinFree) {
                Compact();
            }
            return _buffer.AsMemory(_end);
        }

        public void Advance(int bytesRead) => _end += bytesRead;

        private void Compact() {
            Buffer.BlockCopy(_buffer, _start, _buffer, 0, _end - _start);
            _end -= _start;
            _start = 0;
        }

        private void Replace(int size) {
            var replacement = Pooled.Rent(size);
            int length = _end - _start;
            Buffer.BlockCopy(_buffer, _start, replacement, 0, length);
            Pooled.Return(_buffer);
            _buffer = replacement;
            _start = 0;
            _end = length;
        }

        public void Dispose() {
            var buffer = _buffer;
            if (buffer.Length > 0) {
                _buffer = System.Array.Empty<byte>();
                Pooled.Return(buffer);
            }
        }
    }
}
