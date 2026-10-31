using System;
using System.Threading;
using NetSync.Internal;

namespace NetSync.Pipeline.Reliability {
    /// <summary>
    /// One thread drives the retransmission timers of every <see cref="ReliableEndpoint"/>
    /// in the process. A loop per endpoint would mean a timer wake-up per connection every
    /// few milliseconds — 100 000+ wake-ups a second on a server with a thousand clients;
    /// one clock scanning them all costs a few microseconds per idle connection per tick.
    /// Sleeps entirely while no endpoint exists.
    /// </summary>
    internal static class ReliabilityClock {
        private const int PeriodMs = 5;

        private static readonly object Lock = new object();
        private static ReliableEndpoint[] _endpoints = Array.Empty<ReliableEndpoint>(); // copy-on-write
        private static Thread? _thread;

        public static void Register(ReliableEndpoint endpoint) {
            lock (Lock) {
                var updated = new ReliableEndpoint[_endpoints.Length + 1];
                _endpoints.CopyTo(updated, 0);
                updated[updated.Length - 1] = endpoint;
                Volatile.Write(ref _endpoints, updated);
                if (_thread == null) {
                    _thread = new Thread(Run) { IsBackground = true, Name = "NetSync reliability clock" };
                    _thread.Start();
                }
                Monitor.Pulse(Lock);
            }
        }

        public static void Unregister(ReliableEndpoint endpoint) {
            lock (Lock) {
                int index = Array.IndexOf(_endpoints, endpoint);
                if (index < 0) {
                    return;
                }
                var updated = new ReliableEndpoint[_endpoints.Length - 1];
                Array.Copy(_endpoints, 0, updated, 0, index);
                Array.Copy(_endpoints, index + 1, updated, index, _endpoints.Length - index - 1);
                Volatile.Write(ref _endpoints, updated);
            }
        }

        /// <summary>Endpoints currently driven (diagnostics/tests).</summary>
        public static int Count => Volatile.Read(ref _endpoints).Length;

        private static void Run() {
            while (true) {
                lock (Lock) {
                    while (_endpoints.Length == 0) {
                        Monitor.Wait(Lock);
                    }
                }
                Thread.Sleep(PeriodMs);
                long now = NetTime.NowMs;
                foreach (var endpoint in Volatile.Read(ref _endpoints)) {
                    endpoint.Tick(now);
                }
            }
        }
    }
}
