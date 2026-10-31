using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using NetSync.Peers;
using NetSync.Serialization;

namespace NetSync.Streams {
    /// <summary>Metadata of a stream being received.</summary>
    public sealed class NetStreamInfo {
        public uint StreamId { get; }
        public string Name { get; }
        /// <summary>Total size announced by the sender, -1 when the source was not seekable.</summary>
        public long Length { get; }
        public long BytesReceived { get; internal set; }

        internal NetStreamInfo(uint streamId, string name, long length) {
            StreamId = streamId;
            Name = name;
            Length = length;
        }

        public override string ToString() => $"Stream#{StreamId} '{Name}' {BytesReceived}/{(Length < 0 ? "?" : Length.ToString())} B";
    }

    /// <summary>
    /// Sends arbitrarily large data (files, recordings) as a sequence of chunk messages,
    /// with memory bounded by the transport's backpressure — never the whole payload.
    ///
    /// Use a dedicated channel that is ordered and reliable (TCP, or UDP with
    /// <see cref="ReliabilityMode.ReliableOrdered"/>); consider <see cref="ChannelPriority.Low"/>
    /// so transfers yield to real-time traffic. On the receiving side feed every message
    /// of that channel to a <see cref="NetStreamReceiver"/> (one per sender).
    /// </summary>
    public static class NetStreams {
        public const int DefaultChunkSize = 256 * 1024;

        internal const byte KindBegin = 1;
        internal const byte KindChunk = 2;
        internal const byte KindEnd = 3;
        internal const byte KindAbort = 4;
        internal const int ChunkHeaderSize = 5; // [kind][streamId:4 LE]

        private static int _nextStreamId;

        public static Task SendStreamAsync(this NetClient client, byte channel, Stream source, string name = "",
                                           int chunkSize = DefaultChunkSize, CancellationToken ct = default) {
            return SendAsync((data, token) => client.SendAsync(channel, data, token), source, name, chunkSize, ct);
        }

        public static Task SendStreamAsync(this NetConnection connection, byte channel, Stream source, string name = "",
                                           int chunkSize = DefaultChunkSize, CancellationToken ct = default) {
            return SendAsync((data, token) => connection.SendAsync(channel, data, token), source, name, chunkSize, ct);
        }

        /// <summary>
        /// Transport-agnostic core. <paramref name="send"/> must have copied or queued the
        /// payload by the time its task completes (all NetSync send paths do), because
        /// the chunk buffer is reused for the next read.
        /// </summary>
        public static async Task SendAsync(Func<ReadOnlyMemory<byte>, CancellationToken, ValueTask> send, Stream source,
                                           string name, int chunkSize, CancellationToken ct) {
            if (source == null) {
                throw new ArgumentNullException(nameof(source));
            }
            if (chunkSize < 1024) {
                throw new ArgumentOutOfRangeException(nameof(chunkSize), "Chunk size must be at least 1 KB");
            }

            uint id = unchecked((uint)Interlocked.Increment(ref _nextStreamId));
            long length = source.CanSeek ? source.Length - source.Position : -1;

            var header = new NetWriter(64);
            header.WriteByte(KindBegin).WriteUInt32(id).WriteInt64(length).WriteString(name ?? string.Empty);
            await send(header.WrittenMemory, ct).ConfigureAwait(false);

            var buffer = ArrayPool<byte>.Shared.Rent(ChunkHeaderSize + chunkSize);
            bool completed = false;
            try {
                buffer[0] = KindChunk;
                BinaryPrimitives.WriteUInt32LittleEndian(buffer.AsSpan(1, 4), id);
                while (true) {
                    int read = await source.ReadAsync(buffer, ChunkHeaderSize, chunkSize, ct).ConfigureAwait(false);
                    if (read == 0) {
                        break;
                    }
                    await send(buffer.AsMemory(0, ChunkHeaderSize + read), ct).ConfigureAwait(false);
                }

                var end = new byte[ChunkHeaderSize];
                end[0] = KindEnd;
                BinaryPrimitives.WriteUInt32LittleEndian(end.AsSpan(1, 4), id);
                await send(end, ct).ConfigureAwait(false);
                completed = true;
            }
            finally {
                ArrayPool<byte>.Shared.Return(buffer);
                if (!completed) {
                    await TrySendAbortAsync(send, id).ConfigureAwait(false);
                }
            }
        }

        private static async Task TrySendAbortAsync(Func<ReadOnlyMemory<byte>, CancellationToken, ValueTask> send, uint id) {
            try {
                var abort = new NetWriter(32);
                abort.WriteByte(KindAbort).WriteUInt32(id).WriteString("sender aborted");
                await send(abort.WrittenMemory, CancellationToken.None).ConfigureAwait(false);
            }
            catch {
                // the connection is probably gone; the receiver's Dispose cleans up
            }
        }
    }

    /// <summary>
    /// Reassembles streams sent with <see cref="NetStreams"/>: writes each chunk to a
    /// destination stream as it arrives. Create one per sender (per connection on a
    /// server) and call <see cref="HandleMessage(ReadOnlyMemory{byte})"/> for every message of the stream channel.
    ///
    /// HandleMessage writes synchronously on the calling thread. Under Polled delivery
    /// (Unity main thread) prefer fast destinations, or feed it from a worker.
    /// </summary>
    public sealed class NetStreamReceiver : IDisposable {
        private readonly Func<NetStreamInfo, Stream> _openDestination;
        private readonly Dictionary<uint, (NetStreamInfo Info, Stream Destination)> _active = new Dictionary<uint, (NetStreamInfo, Stream)>();

        /// <summary>Dispose destination streams when a transfer ends (default true).</summary>
        public bool DisposeDestinations { get; set; } = true;

        /// <summary>
        /// Streams that may be open at once (default 16). Further announcements are ignored and
        /// reported through <see cref="StreamRefused"/> — a sender can't make you open files without end.
        /// </summary>
        public int MaxActiveStreams { get; set; } = 16;

        public event Action<NetStreamInfo>? StreamStarted;
        public event Action<NetStreamInfo>? StreamCompleted;
        public event Action<NetStreamInfo, string>? StreamFailed;

        /// <summary>A stream was announced but not opened (<see cref="MaxActiveStreams"/> reached).</summary>
        public event Action<NetStreamInfo>? StreamRefused;

        /// <param name="openDestination">Called on stream start: return where the bytes go (file, MemoryStream, …).</param>
        public NetStreamReceiver(Func<NetStreamInfo, Stream> openDestination) {
            _openDestination = openDestination ?? throw new ArgumentNullException(nameof(openDestination));
        }

        public int ActiveStreams => _active.Count;

        public void HandleMessage(byte[] message) => HandleMessage(new ReadOnlyMemory<byte>(message));

        /// <summary>Zero-copy form for <see cref="NetClient.MessageReceived"/> handlers: chunks are written out during the call.</summary>
        public void HandleMessage(ReadOnlyMemory<byte> message) {
            if (message.Length < NetStreams.ChunkHeaderSize) {
                return;
            }
            var span = message.Span;
            byte kind = span[0];
            uint id = BinaryPrimitives.ReadUInt32LittleEndian(span.Slice(1, 4));

            switch (kind) {
                case NetStreams.KindBegin: {
                    long length;
                    string name;
                    try {
                        var reader = new NetReader(message);
                        reader.ReadByte();
                        reader.ReadUInt32();
                        length = reader.ReadInt64();
                        name = reader.ReadString();
                    }
                    catch (EndOfStreamException) {
                        break; // malformed announcement: its chunks will be ignored
                    }
                    var info = new NetStreamInfo(id, name, length);
                    if (_active.ContainsKey(id)) {
                        Fail(id, "duplicate stream id");
                    }
                    if (_active.Count >= MaxActiveStreams) {
                        StreamRefused?.Invoke(info);
                        break;
                    }
                    _active[id] = (info, _openDestination(info));
                    StreamStarted?.Invoke(info);
                    break;
                }

                case NetStreams.KindChunk:
                    if (_active.TryGetValue(id, out var entry)) {
                        int count = message.Length - NetStreams.ChunkHeaderSize;
                        entry.Info.BytesReceived += count;
                        if (entry.Info.Length >= 0 && entry.Info.BytesReceived > entry.Info.Length) {
                            Fail(id, "received more bytes than announced");
                            break;
                        }
                        try {
                            entry.Destination.Write(span.Slice(NetStreams.ChunkHeaderSize, count));
                        }
                        catch (Exception ex) when (ex is IOException || ex is ObjectDisposedException || ex is NotSupportedException) {
                            Fail(id, $"writing the destination failed: {ex.Message}"); // e.g. disk full
                        }
                    }
                    break;

                case NetStreams.KindEnd:
                    if (_active.TryGetValue(id, out var finished)) {
                        if (finished.Info.Length >= 0 && finished.Info.BytesReceived != finished.Info.Length) {
                            Fail(id, $"length mismatch: announced {finished.Info.Length}, received {finished.Info.BytesReceived}");
                            break;
                        }
                        try {
                            finished.Destination.Flush();
                        }
                        catch (Exception ex) when (ex is IOException || ex is ObjectDisposedException) {
                            Fail(id, $"writing the destination failed: {ex.Message}");
                            break;
                        }
                        _active.Remove(id);
                        if (DisposeDestinations) {
                            finished.Destination.Dispose();
                        }
                        StreamCompleted?.Invoke(finished.Info);
                    }
                    break;

                case NetStreams.KindAbort: {
                    string reason = "aborted";
                    try {
                        var reader = new NetReader(message);
                        reader.ReadByte();
                        reader.ReadUInt32();
                        reason = reader.ReadString();
                    }
                    catch (EndOfStreamException) { }
                    Fail(id, reason);
                    break;
                }
            }
        }

        private void Fail(uint id, string reason) {
            if (!_active.TryGetValue(id, out var entry)) {
                return;
            }
            _active.Remove(id);
            if (DisposeDestinations) {
                entry.Destination.Dispose();
            }
            StreamFailed?.Invoke(entry.Info, reason);
        }

        /// <summary>Closes destinations of unfinished streams (e.g. when the connection drops).</summary>
        public void Dispose() {
            foreach (var id in new List<uint>(_active.Keys)) {
                Fail(id, "receiver disposed");
            }
        }
    }
}
