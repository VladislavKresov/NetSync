#if NET8_0_OR_GREATER
using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;

namespace NetSync.Transports.Udp {
    /// <summary>
    /// Allocation-free source addresses for unconnected UDP receives (net8.0): receive into
    /// <see cref="Address"/>, then <see cref="Resolve"/> returns the same <see cref="IPEndPoint"/>
    /// instance for a source seen before. Bounded — a flood of forged sources only costs the
    /// allocations it would have cost anyway. One receive thread per instance.
    /// </summary>
    internal sealed class EndPointCache {
        private readonly Dictionary<SocketAddressKey, IPEndPoint> _known = new Dictionary<SocketAddressKey, IPEndPoint>();
        private readonly IPEndPoint _template;
        private readonly int _capacity;

        public EndPointCache(AddressFamily family, int capacity) {
            Address = new SocketAddress(family);
            _template = new IPEndPoint(family == AddressFamily.InterNetworkV6 ? IPAddress.IPv6Any : IPAddress.Any, 0);
            _capacity = capacity;
        }

        /// <summary>Pass to Socket.ReceiveFrom(Span, SocketFlags, SocketAddress).</summary>
        public SocketAddress Address { get; }

        public IPEndPoint Resolve() {
            if (_known.TryGetValue(new SocketAddressKey(Address), out var known)) {
                return known;
            }
            if (_known.Count >= _capacity) {
                _known.Clear();
            }
            var endPoint = (IPEndPoint)_template.Create(Address);
            _known[SocketAddressKey.Copy(Address)] = endPoint;
            return endPoint;
        }
    }

    /// <summary>
    /// The send-side counterpart (net8.0): Socket.SendTo(…, EndPoint) serializes the endpoint
    /// into a new SocketAddress on every call; this keeps one per destination. Thread-safe,
    /// bounded like <see cref="EndPointCache"/>.
    /// </summary>
    internal sealed class SocketAddressCache {
        private readonly System.Collections.Concurrent.ConcurrentDictionary<IPEndPoint, SocketAddress> _addresses =
            new System.Collections.Concurrent.ConcurrentDictionary<IPEndPoint, SocketAddress>();
        private readonly AddressFamily _socketFamily;
        private readonly int _capacity;

        public SocketAddressCache(AddressFamily socketFamily, int capacity) {
            _socketFamily = socketFamily;
            _capacity = capacity;
        }

        public SocketAddress For(IPEndPoint target) {
            if (_addresses.TryGetValue(target, out var address)) {
                return address;
            }
            if (_addresses.Count >= _capacity) {
                _addresses.Clear();
            }
            // A dual-stack socket reaches IPv4 peers as ::ffff:a.b.c.d.
            var serialized = _socketFamily == AddressFamily.InterNetworkV6 && target.AddressFamily == AddressFamily.InterNetwork
                ? new IPEndPoint(target.Address.MapToIPv6(), target.Port).Serialize()
                : target.Serialize();
            _addresses[target] = serialized;
            return serialized;
        }
    }

    /// <summary>
    /// A socket address compared by content. The hash is computed from the bytes on every call:
    /// receive loops rewrite one SocketAddress in place, so nothing about it may be cached.
    /// </summary>
    internal readonly struct SocketAddressKey : IEquatable<SocketAddressKey> {
        public readonly SocketAddress Address;

        public SocketAddressKey(SocketAddress address) => Address = address;

        /// <summary>A key that owns its bytes (for storing — the receive loop reuses its address).</summary>
        public static SocketAddressKey Copy(SocketAddress source) {
            var copy = new SocketAddress(source.Family, source.Size);
            source.Buffer.Span.Slice(0, source.Size).CopyTo(copy.Buffer.Span);
            return new SocketAddressKey(copy);
        }

        public bool Equals(SocketAddressKey other) {
            return Address.Size == other.Address.Size &&
                   Address.Buffer.Span.Slice(0, Address.Size).SequenceEqual(other.Address.Buffer.Span.Slice(0, other.Address.Size));
        }

        public override bool Equals(object? obj) => obj is SocketAddressKey other && Equals(other);

        public override int GetHashCode() {
            var hash = new HashCode();
            hash.AddBytes(Address.Buffer.Span.Slice(0, Address.Size));
            return hash.ToHashCode();
        }
    }
}
#endif
