using System;
using System.Collections.Generic;
using NetSync.Internal;

namespace NetSync.Pipeline.Fragmentation {
    /// <summary>
    /// Reassembles fragmented packets with hard memory bounds — the remote decides
    /// fragment counts and sizes, so every number it sends is treated as hostile:
    /// per-packet byte cap, per-packet fragment cap, total buffered-bytes cap,
    /// concurrent-assembly cap, throttled stale cleanup.
    ///
    /// Fragments are held in pooled buffers and the reassembled message is handed out
    /// in a pooled buffer too (<see cref="TryAddFragmentPooled"/>): large transfers don't
    /// churn the large-object heap.
    /// </summary>
    public sealed class FragmentBuffer {
        private sealed class Assembly {
            public readonly byte[]?[] Fragments;
            public readonly int[] Lengths;
            public int ReceivedCount;
            public long TotalBytes;
            public long LastFragmentAtMs;

            public Assembly(uint total) {
                Fragments = new byte[total][];
                Lengths = new int[total];
            }

            public bool IsComplete => ReceivedCount == Fragments.Length;
        }

        private readonly Dictionary<uint, Assembly> _assemblies = new Dictionary<uint, Assembly>();
        private readonly object _lock = new object();
        private long _lastCleanupMs;
        private long _bufferedBytes;

        /// <summary>Incomplete packets are dropped after this long without new fragments.</summary>
        public int FragmentTimeoutMs { get; set; } = 5000;

        /// <summary>Upper bound on simultaneously reassembling packets; oldest is evicted.</summary>
        public int MaxConcurrentPackets { get; set; } = 16;

        /// <summary>Largest fragment count a single packet may claim.</summary>
        public int MaxFragmentsPerPacket { get; set; } = 1 << 16;

        /// <summary>Largest reassembled packet.</summary>
        public long MaxPacketBytes { get; set; } = NetConfig.DefaultMaxMessageSize;

        /// <summary>Total bytes held across all incomplete packets.</summary>
        public long MaxBufferedBytes { get; set; } = 4L * NetConfig.DefaultMaxMessageSize;

        /// <summary>
        /// Convenience wrapper: returns the reassembled payload or null; limit
        /// violations are indistinguishable from "incomplete".
        /// </summary>
        public byte[]? AddFragment(uint sequenceId, uint index, uint total, byte[] data) {
            TryAddFragment(sequenceId, index, total, data, out var complete);
            return complete;
        }

        /// <summary>
        /// Adds a fragment. <paramref name="complete"/> receives the payload when this
        /// fragment completes its packet. Returns false when the fragment violates a
        /// limit or is malformed — the sender is misbehaving and the caller should
        /// drop the connection. Duplicates are not violations (datagrams duplicate).
        /// </summary>
        public bool TryAddFragment(uint sequenceId, uint index, uint total, byte[] data, out byte[]? complete) {
            complete = null;
            if (!TryAddFragmentPooled(sequenceId, index, total, data, out var rented, out int length)) {
                return false;
            }
            if (rented != null) {
                complete = Pooled.ToArray(rented, 0, length);
                Pooled.Return(rented);
            }
            return true;
        }

        /// <summary>
        /// Allocation-free variant: the fragment is copied into a pooled buffer, and a
        /// completed packet comes back as a pooled buffer (<paramref name="complete"/>,
        /// <paramref name="completeLength"/> valid bytes) that the caller must return
        /// via <see cref="System.Buffers.ArrayPool{T}.Shared"/>.
        /// </summary>
        internal bool TryAddFragmentPooled(uint sequenceId, uint index, uint total, ReadOnlySpan<byte> data,
                                           out byte[]? complete, out int completeLength) {
            complete = null;
            completeLength = 0;
            if (total == 0 || index >= total || total > MaxFragmentsPerPacket) {
                return false;
            }
            if (total == 1) {
                // A single-fragment packet is already complete: nothing to buffer.
                if (data.Length > MaxPacketBytes) {
                    return false;
                }
                complete = Pooled.Copy(data);
                completeLength = data.Length;
                return true;
            }

            long now = NetTime.NowMs;
            lock (_lock) {
                CleanupIfDue(now);

                if (!_assemblies.TryGetValue(sequenceId, out var assembly)) {
                    if (_assemblies.Count >= MaxConcurrentPackets) {
                        EvictOldest(except: null);
                    }
                    assembly = new Assembly(total);
                    _assemblies[sequenceId] = assembly;
                }

                if (assembly.Fragments.Length != total || assembly.Fragments[index] != null) {
                    // Duplicate, or the same id reused with another total: ignore.
                    assembly.LastFragmentAtMs = now;
                    return true;
                }

                if (assembly.TotalBytes + data.Length > MaxPacketBytes) {
                    Remove(sequenceId, assembly);
                    return false;
                }
                while (_bufferedBytes + data.Length > MaxBufferedBytes) {
                    if (!EvictOldest(except: assembly)) {
                        Remove(sequenceId, assembly);
                        return false;
                    }
                }

                assembly.Fragments[index] = Pooled.Copy(data);
                assembly.Lengths[index] = data.Length;
                assembly.ReceivedCount++;
                assembly.TotalBytes += data.Length;
                assembly.LastFragmentAtMs = now;
                _bufferedBytes += data.Length;

                if (!assembly.IsComplete) {
                    return true;
                }

                _assemblies.Remove(sequenceId);
                _bufferedBytes -= assembly.TotalBytes;
                var result = Pooled.Rent((int)assembly.TotalBytes);
                int offset = 0;
                for (int i = 0; i < assembly.Fragments.Length; i++) {
                    var fragment = assembly.Fragments[i]!;
                    int length = assembly.Lengths[i];
                    Buffer.BlockCopy(fragment, 0, result, offset, length);
                    offset += length;
                    Pooled.Return(fragment);
                    assembly.Fragments[i] = null;
                }
                complete = result;
                completeLength = offset;
                return true;
            }
        }

        public int ActiveCount {
            get {
                lock (_lock) {
                    return _assemblies.Count;
                }
            }
        }

        public long BufferedBytes {
            get {
                lock (_lock) {
                    return _bufferedBytes;
                }
            }
        }

        public void Clear() {
            lock (_lock) {
                foreach (var assembly in _assemblies.Values) {
                    ReturnFragments(assembly);
                }
                _assemblies.Clear();
                _bufferedBytes = 0;
            }
        }

        private void Remove(uint sequenceId, Assembly assembly) {
            if (_assemblies.Remove(sequenceId)) {
                _bufferedBytes -= assembly.TotalBytes;
                ReturnFragments(assembly);
            }
        }

        private static void ReturnFragments(Assembly assembly) {
            for (int i = 0; i < assembly.Fragments.Length; i++) {
                var fragment = assembly.Fragments[i];
                if (fragment != null) {
                    Pooled.Return(fragment);
                    assembly.Fragments[i] = null;
                }
            }
        }

        private void CleanupIfDue(long nowMs) {
            // Throttle: a full scan at most once per second, not on every fragment.
            if (nowMs - _lastCleanupMs < 1000) {
                return;
            }
            _lastCleanupMs = nowMs;

            List<uint>? stale = null;
            foreach (var kvp in _assemblies) {
                if (nowMs - kvp.Value.LastFragmentAtMs > FragmentTimeoutMs) {
                    (stale ??= new List<uint>()).Add(kvp.Key);
                }
            }
            if (stale != null) {
                foreach (var id in stale) {
                    Remove(id, _assemblies[id]);
                }
            }
        }

        private bool EvictOldest(Assembly? except) {
            uint oldestId = 0;
            Assembly? oldest = null;
            foreach (var kvp in _assemblies) {
                if (!ReferenceEquals(kvp.Value, except) &&
                    (oldest == null || kvp.Value.LastFragmentAtMs < oldest.LastFragmentAtMs)) {
                    oldest = kvp.Value;
                    oldestId = kvp.Key;
                }
            }
            if (oldest == null) {
                return false;
            }
            Remove(oldestId, oldest);
            return true;
        }
    }
}
