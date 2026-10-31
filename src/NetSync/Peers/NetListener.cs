using NetSync.Transports;

namespace NetSync.Peers {
    /// <summary>An extra transport a <see cref="NetServer"/> accepts clients on (see <see cref="NetServer.AddListener"/>).</summary>
    public sealed class NetListener {
        /// <summary>The slot it serves.</summary>
        public TransportType Slot { get; }

        /// <summary>The bound port while the server runs; 0 otherwise.</summary>
        public int Port { get; internal set; }

        internal ITransportFactory Factory { get; }
        internal int RequestedPort { get; }

        internal NetListener(TransportType slot, ITransportFactory factory, int requestedPort) {
            Slot = slot;
            Factory = factory;
            RequestedPort = requestedPort;
        }

        public override string ToString() => $"{Factory.GetType().Name} ({Slot}) on port {(Port != 0 ? Port : RequestedPort)}";
    }
}
