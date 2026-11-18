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
        private readonly SimulationLane _lane = new SimulationLane();
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
            _lane.Schedule(dueFirst, deliver);
            if (copies == 2) {
                _lane.Schedule(dueSecond, deliver);
            }
        }
    }

    /// <summary>
    /// Delivers the delayed packets of one direction of one link, in due order, on its own
    /// thread — like the receive loop of a real link. One lane can't hold up another: a slow
    /// handler on one connection delays only that connection, as on a real network.
    ///
    /// The thread sleeps until the next packet is due rather than spinning, so the simulator
    /// costs no CPU of its own. Sleeps last a whole timer tick, 15.6 ms by default on Windows,
    /// so while a lane runs there it asks for 1 ms ticks (affects this process only, since
    /// Windows 10 2004). The thread ends after a few idle seconds and starts again with the
    /// next packet.
    /// </summary>
    internal sealed class SimulationLane {
        private const int IdleExitMs = 5000;

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

        private readonly object _lock = new object();
        private readonly List<Item> _heap = new List<Item>();
        private Thread? _thread;
        private long _sequence;

        public void Schedule(double dueMs, Action action) {
            lock (_lock) {
                Push(new Item(dueMs, _sequence++, action));
                if (_thread == null) {
                    _thread = new Thread(Run) { IsBackground = true, Name = "NetSync network simulation" };
                    _thread.Start();
                }
                Monitor.Pulse(_lock);
            }
        }

        private void Run() {
            using var resolution = TimerResolution.Fine();
            while (true) {
                Action due;
                lock (_lock) {
                    while (true) {
                        if (_heap.Count == 0) {
                            if (!Monitor.Wait(_lock, IdleExitMs) && _heap.Count == 0) {
                                _thread = null; // the next Schedule starts a new one
                                return;
                            }
                            continue;
                        }
                        double wait = _heap[0].DueMs - NetTime.PreciseMs;
                        if (wait <= 0) {
                            due = Pop().Action;
                            break;
                        }
                        Monitor.Wait(_lock, Math.Max(1, (int)Math.Ceiling(wait)));
                    }
                }
                try {
                    due();
                }
                catch {
                    // a delivery into a closed link: nothing to do
                }
            }
        }

        private void Push(Item item) {
            _heap.Add(item);
            int i = _heap.Count - 1;
            while (i > 0) {
                int parent = (i - 1) / 2;
                if (!_heap[i].Before(_heap[parent])) {
                    break;
                }
                (_heap[i], _heap[parent]) = (_heap[parent], _heap[i]);
                i = parent;
            }
        }

        private Item Pop() {
            var top = _heap[0];
            int last = _heap.Count - 1;
            _heap[0] = _heap[last];
            _heap.RemoveAt(last);
            int i = 0;
            while (true) {
                int left = 2 * i + 1;
                int right = left + 1;
                int smallest = i;
                if (left < _heap.Count && _heap[left].Before(_heap[smallest])) {
                    smallest = left;
                }
                if (right < _heap.Count && _heap[right].Before(_heap[smallest])) {
                    smallest = right;
                }
                if (smallest == i) {
                    break;
                }
                (_heap[i], _heap[smallest]) = (_heap[smallest], _heap[i]);
                i = smallest;
            }
            return top;
        }
    }

    /// <summary>1 ms OS timer ticks while held (Windows; elsewhere sleeps are already that precise).</summary>
    internal readonly struct TimerResolution : IDisposable {
        private readonly bool _held;

        private TimerResolution(bool held) => _held = held;

        public static TimerResolution Fine() {
            if (!System.Runtime.InteropServices.RuntimeInformation.IsOSPlatform(System.Runtime.InteropServices.OSPlatform.Windows)) {
                return default;
            }
            try {
                return new TimerResolution(timeBeginPeriod(1) == 0);
            }
            catch (Exception) { // no winmm (Nano Server, some sandboxes): coarse ticks, still correct
                return default;
            }
        }

        public void Dispose() {
            if (_held) {
                timeEndPeriod(1);
            }
        }

        [System.Runtime.InteropServices.DllImport("winmm.dll")]
        private static extern uint timeBeginPeriod(uint milliseconds);

        [System.Runtime.InteropServices.DllImport("winmm.dll")]
        private static extern uint timeEndPeriod(uint milliseconds);
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
