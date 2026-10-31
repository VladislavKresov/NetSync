using System;
using System.Collections.Generic;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using NetSync.Internal;

namespace NetSync.Transports.Simulation {
    /// <summary>
    /// Wraps any transport factory with a <see cref="NetworkSimulation"/>. NetSync applies it
    /// automatically when <see cref="NetConfig.Simulation"/> is set; use it directly to
    /// simulate only one slot or a custom transport.
    /// </summary>
    public sealed class SimulatedTransportFactory : ITransportFactory {
        private readonly ITransportFactory _inner;
        private readonly NetworkSimulation _simulation;
        private readonly bool _datagram;

        /// <param name="inner">The real transport.</param>
        /// <param name="simulation">What to simulate.</param>
        /// <param name="datagram">True for a datagram link (loss, duplication and reordering apply).</param>
        public SimulatedTransportFactory(ITransportFactory inner, NetworkSimulation simulation, bool datagram) {
            _inner = inner ?? throw new ArgumentNullException(nameof(inner));
            _simulation = simulation ?? throw new ArgumentNullException(nameof(simulation));
            _datagram = datagram;
        }

        public ITransport CreateClient(TransportSettings settings) {
            return new SimulatedClientTransport(_inner.CreateClient(settings), new Impairment(_simulation, _datagram));
        }

        public IServerTransport CreateServer(TransportSettings settings) {
            return new SimulatedServerTransport(_inner.CreateServer(settings), new Impairment(_simulation, _datagram));
        }
    }

    /// <summary>Lets the peer layer reach the real transport behind a wrapper (trust mode, hooks).</summary>
    internal interface IWrappedServerTransport {
        IServerTransport Inner { get; }
    }

    /// <summary>Decides each packet's fate: dropped, or delivered (maybe twice) at some time.</summary>
    internal sealed class Impairment {
        private readonly NetworkSimulation _simulation;
        private readonly bool _datagram;
        private readonly Random _random;
        private double _lastDueMs; // streams: never deliver before an earlier packet (FIFO)

        public Impairment(NetworkSimulation simulation, bool datagram) {
            _simulation = simulation;
            _datagram = datagram;
            _random = simulation.Seed.HasValue ? new Random(simulation.Seed.Value) : new Random();
        }

        public int ExtraRoundTripMs => 2 * _simulation.LatencyMs;

        /// <summary>The same impairment for the opposite direction: own random stream, own FIFO clock.</summary>
        public Impairment ForOtherDirection() => new Impairment(_simulation, _datagram);

        /// <summary>Schedules <paramref name="deliver"/> per the simulation (0, 1 or 2 times).</summary>
        public void Apply(Action deliver) {
            int copies = 1;
            double now = NetTime.PreciseMs;
            double dueFirst, dueSecond = 0;
            lock (_random) {
                if (_datagram) {
                    if (_random.NextDouble() < _simulation.LossRate) {
                        return;
                    }
                    if (_random.NextDouble() < _simulation.DuplicateRate) {
                        copies = 2;
                    }
                }
                dueFirst = now + _simulation.LatencyMs + _random.NextDouble() * _simulation.JitterMs;
                if (copies == 2) {
                    dueSecond = now + _simulation.LatencyMs + _random.NextDouble() * _simulation.JitterMs;
                }
                if (!_datagram) {
                    dueFirst = Math.Max(dueFirst, _lastDueMs);
                    _lastDueMs = dueFirst;
                }
            }
            SimulationClock.Schedule(dueFirst, deliver);
            if (copies == 2) {
                SimulationClock.Schedule(dueSecond, deliver);
            }
        }
    }

    /// <summary>
    /// One background thread delivering delayed packets on time. Waits coarsely and spins
    /// the last stretch: OS timers (15.6 ms on Windows) would distort small latencies.
    /// </summary>
    internal static class SimulationClock {
        private readonly struct Item {
            public readonly double DueMs;
            public readonly long Sequence; // FIFO among equal due times
            public readonly Action Action;

            public Item(double dueMs, long sequence, Action action) {
                DueMs = dueMs;
                Sequence = sequence;
                Action = action;
            }

            public bool Before(in Item other) => DueMs < other.DueMs || (DueMs == other.DueMs && Sequence < other.Sequence);
        }

        private static readonly object Lock = new object();
        private static readonly List<Item> Heap = new List<Item>();
        private static Thread? _thread;
        private static long _sequence;

        public static void Schedule(double dueMs, Action action) {
            lock (Lock) {
                Push(new Item(dueMs, _sequence++, action));
                if (_thread == null) {
                    _thread = new Thread(Run) { IsBackground = true, Name = "NetSync network simulation" };
                    _thread.Start();
                }
                Monitor.Pulse(Lock);
            }
        }

        private static void Run() {
            while (true) {
                Action? due = null;
                lock (Lock) {
                    if (Heap.Count == 0) {
                        Monitor.Wait(Lock);
                        continue;
                    }
                    double wait = Heap[0].DueMs - NetTime.PreciseMs;
                    if (wait <= 0) {
                        due = Pop().Action;
                    }
                    else if (wait > 20) {
                        Monitor.Wait(Lock, (int)(wait - 16));
                        continue;
                    }
                }
                if (due == null) {
                    Thread.Yield(); // final approach
                    continue;
                }
                try {
                    due();
                }
                catch {
                    // a delivery into a closed link: nothing to do
                }
            }
        }

        private static void Push(Item item) {
            Heap.Add(item);
            int i = Heap.Count - 1;
            while (i > 0) {
                int parent = (i - 1) / 2;
                if (!Heap[i].Before(Heap[parent])) {
                    break;
                }
                (Heap[i], Heap[parent]) = (Heap[parent], Heap[i]);
                i = parent;
            }
        }

        private static Item Pop() {
            var top = Heap[0];
            int last = Heap.Count - 1;
            Heap[0] = Heap[last];
            Heap.RemoveAt(last);
            int i = 0;
            while (true) {
                int left = 2 * i + 1;
                int right = left + 1;
                int smallest = i;
                if (left < Heap.Count && Heap[left].Before(Heap[smallest])) {
                    smallest = left;
                }
                if (right < Heap.Count && Heap[right].Before(Heap[smallest])) {
                    smallest = right;
                }
                if (smallest == i) {
                    break;
                }
                (Heap[i], Heap[smallest]) = (Heap[smallest], Heap[i]);
                i = smallest;
            }
            return top;
        }
    }

    internal sealed class SimulatedClientTransport : ITransport {
        private readonly ITransport _inner;
        private readonly Impairment _outgoing;
        private readonly Impairment _incoming;

        public SimulatedClientTransport(ITransport inner, Impairment impairment) {
            _inner = inner;
            _outgoing = impairment;
            _incoming = impairment.ForOtherDirection();
            _inner.Connected += () => Connected?.Invoke();
            _inner.Disconnected += reason => Disconnected?.Invoke(reason);
            _inner.DataReceived += data => _incoming.Apply(() => DataReceived?.Invoke(data));
        }

        public bool IsConnected => _inner.IsConnected;
        public int PingMs => _inner.PingMs < 0 ? -1 : _inner.PingMs + _outgoing.ExtraRoundTripMs;

        public event Action? Connected;
        public event Action<DisconnectReason>? Disconnected;
        public event Action<byte[]>? DataReceived;

        public Task ConnectAsync(IPEndPoint endpoint, CancellationToken ct = default) => _inner.ConnectAsync(endpoint, ct);

        public ValueTask SendAsync(ReadOnlyMemory<byte> data, CancellationToken ct = default) => SendAsync(ReadOnlyMemory<byte>.Empty, data, ct);

        public ValueTask SendAsync(ReadOnlyMemory<byte> prefix, ReadOnlyMemory<byte> payload, CancellationToken ct = default) {
            if (!_inner.IsConnected) {
                throw new InvalidOperationException("Not connected");
            }
            var copy = new byte[prefix.Length + payload.Length]; // the caller's memory is only ours until we return
            prefix.Span.CopyTo(copy);
            payload.Span.CopyTo(copy.AsSpan(prefix.Length));
            _outgoing.Apply(() => _ = _inner.SendAsync(copy));
            return default;
        }

        public void Disconnect() => _inner.Disconnect();
        public void Dispose() => _inner.Dispose();
    }

    internal sealed class SimulatedServerTransport : IServerTransport, IWrappedServerTransport {
        private readonly IServerTransport _inner;
        private readonly Impairment _outgoing;
        private readonly Impairment _incoming;

        public SimulatedServerTransport(IServerTransport inner, Impairment impairment) {
            _inner = inner;
            _outgoing = impairment;
            _incoming = impairment.ForOtherDirection();
            _inner.PeerConnected += peer => PeerConnected?.Invoke(peer);
            _inner.PeerDisconnected += (peer, reason) => PeerDisconnected?.Invoke(peer, reason);
            _inner.DataReceived += (peer, data) => _incoming.Apply(() => DataReceived?.Invoke(peer, data));
        }

        public IServerTransport Inner => _inner;
        public int Port => _inner.Port;
        public bool IsRunning => _inner.IsRunning;

        public event Action<TransportPeer>? PeerConnected;
        public event Action<TransportPeer, DisconnectReason>? PeerDisconnected;
        public event Action<TransportPeer, byte[]>? DataReceived;

        public Task<int> StartAsync(int port, CancellationToken ct = default) => _inner.StartAsync(port, ct);
        public Task StopAsync() => _inner.StopAsync();

        public ValueTask SendAsync(TransportPeer peer, ReadOnlyMemory<byte> data, CancellationToken ct = default) {
            return SendAsync(peer, ReadOnlyMemory<byte>.Empty, data, ct);
        }

        public ValueTask SendAsync(TransportPeer peer, ReadOnlyMemory<byte> prefix, ReadOnlyMemory<byte> payload, CancellationToken ct = default) {
            var copy = new byte[prefix.Length + payload.Length];
            prefix.Span.CopyTo(copy);
            payload.Span.CopyTo(copy.AsSpan(prefix.Length));
            _outgoing.Apply(() => _ = _inner.SendAsync(peer, copy));
            return default;
        }

        public void DisconnectPeer(TransportPeer peer) => _inner.DisconnectPeer(peer);
        public void Dispose() => _inner.Dispose();
    }
}
