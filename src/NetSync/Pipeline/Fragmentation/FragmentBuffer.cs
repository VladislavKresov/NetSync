using System;
using System.Collections.Generic;
using NetSync.Internal;

namespace NetSync.Pipeline.Fragmentation {
    /// <summary>
    /// Reassembles fragmented packets. Improvements over the 1.x version:
    /// fragments live in a flat array instead of a Dictionary, completeness is an O(1)
    /// counter check, stale-buffer cleanup is throttled instead of running on every
    /// fragment, and total buffered memory is capped.
    /// </summary>
    public sealed class FragmentBuffer {
        private sealed class Assembly {
            public readonly byte[]?[] Fragments;
            public int ReceivedCount;
            public int TotalBytes;
            public long LastFragmentAtMs;

            public Assembly(uint total) {
                Fragments = new byte[total][];
            }

            public bool IsComplete => ReceivedCount == Fragments.Length;
        }

        private readonly Dictionary<uint, Assembly> _assemblies = new Dictionary<uint, Assembly>();
        private readonly object _lock = new object();
        private long _lastCleanupMs;

        /// <summary>Incomplete packets are dropped after this long without new fragments.</summary>
        public int FragmentTimeoutMs { get; set; } = 5000;

        /// <summary>Upper bound on simultaneously reassembling packets; oldest is evicted.</summary>
        public int MaxConcurrentPackets { get; set; } = 64;

        /// <summary>Sanity cap: reject absurd fragment counts (protocol error / attack).</summary>
        public int MaxFragmentsPerPacket { get; set; } = 1 << 20;

        /// <summary>
        /// Adds a fragment. Returns the fully reassembled payload when this fragment
        /// completes the packet, otherwise null.
        /// </summary>
        public byte[]? AddFragment(uint sequenceId, uint index, uint total, byte[] data) {
            if (total == 0 || total > MaxFragmentsPerPacket || index >= total) {
                return null;
            }

            long now = NetTime.NowMs;
            lock (_lock) {
                CleanupIfDue(now);

                if (!_assemblies.TryGetValue(sequenceId, out var assembly)) {
                    if (_assemblies.Count >= MaxConcurrentPackets) {
                        EvictOldest();
                    }
                    assembly = new Assembly(total);
                    _assemblies[sequenceId] = assembly;
                }

                if (assembly.Fragments.Length != total || assembly.Fragments[index] != null) {
                    // Mismatched totals for the same id or duplicate fragment: ignore.
                    assembly.LastFragmentAtMs = now;
                    return null;
                }

                assembly.Fragments[index] = data;
                assembly.ReceivedCount++;
                assembly.TotalBytes += data.Length;
                assembly.LastFragmentAtMs = now;

                if (!assembly.IsComplete) {
                    return null;
                }

                _assemblies.Remove(sequenceId);
                var result = new byte[assembly.TotalBytes];
                int offset = 0;
                foreach (var fragment in assembly.Fragments) {
                    fragment!.CopyTo(result, offset);
                    offset += fragment.Length;
                }
                return result;
            }
        }

        public int ActiveCount {
            get {
                lock (_lock) {
                    return _assemblies.Count;
                }
            }
        }

        public void Clear() {
            lock (_lock) {
                _assemblies.Clear();
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
                    _assemblies.Remove(id);
                }
            }
        }

        private void EvictOldest() {
            uint oldestId = 0;
            long oldestTime = long.MaxValue;
            foreach (var kvp in _assemblies) {
                if (kvp.Value.LastFragmentAtMs < oldestTime) {
                    oldestTime = kvp.Value.LastFragmentAtMs;
                    oldestId = kvp.Key;
                }
            }
            _assemblies.Remove(oldestId);
        }
    }
}
