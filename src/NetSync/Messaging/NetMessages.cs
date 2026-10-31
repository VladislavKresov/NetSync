using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using NetSync.Peers;
using NetSync.Serialization;

namespace NetSync.Messaging {
    /// <summary>
    /// A message type: knows how to write itself to and read itself from the binary
    /// stream. Classes or structs; no reflection involved, IL2CPP/AOT safe.
    /// Struct messages are received without any allocation; a class message costs one
    /// object per message. Read() must copy what it keeps — the reader's bytes are only
    /// valid during the handler.
    /// </summary>
    public interface INetMessage {
        void Write(NetWriter writer);
        void Read(NetReader reader);
    }

    /// <summary>
    /// Maps message types to wire ids. Build one registry and share it between client
    /// and server (same ids on both sides). Wire format: [id: var-uint][body].
    /// </summary>
    public sealed class NetMessageRegistry {
        private readonly Dictionary<Type, uint> _ids = new Dictionary<Type, uint>();
        private readonly Dictionary<uint, Type> _types = new Dictionary<uint, Type>();

        public NetMessageRegistry Register<T>(uint id) where T : INetMessage, new() {
            if (_types.TryGetValue(id, out var existing)) {
                throw new ArgumentException($"Message id {id} is already registered for {existing.Name}", nameof(id));
            }
            if (_ids.ContainsKey(typeof(T))) {
                throw new ArgumentException($"{typeof(T).Name} is already registered", nameof(T));
            }
            _ids[typeof(T)] = id;
            _types[id] = typeof(T);
            return this;
        }

        public bool IsRegistered<T>() => _ids.ContainsKey(typeof(T));

        internal uint GetId<T>() {
            return _ids.TryGetValue(typeof(T), out var id)
                ? id
                : throw new InvalidOperationException($"{typeof(T).Name} is not registered in the NetMessageRegistry");
        }

        internal bool IsKnown(uint id) => _types.ContainsKey(id);

        /// <summary>The message on the wire, in a new buffer the caller owns.</summary>
        public ReadOnlyMemory<byte> Serialize<T>(T message) where T : INetMessage {
            var writer = new NetWriter(64);
            WriteTo(writer, message);
            return writer.WrittenMemory;
        }

        internal void WriteTo<T>(NetWriter writer, T message) where T : INetMessage {
            writer.WriteVarUInt(GetId<T>());
            message.Write(writer);
        }

        [ThreadStatic] private static NetWriter? _sendWriter;

        /// <summary>
        /// Serializes into a per-thread writer and hands the bytes to <paramref name="send"/>, which
        /// must consume them before it returns — every NetSync SendAsync does. No allocation per
        /// message once the writer has grown to the largest message.
        /// </summary>
        internal ValueTask SendWith<T, TTarget>(T message, TTarget target, Func<TTarget, ReadOnlyMemory<byte>, ValueTask> send)
            where T : INetMessage {
            var writer = _sendWriter ?? new NetWriter(256);
            _sendWriter = null; // a nested send on this thread (from Write) gets its own
            try {
                writer.Reset();
                WriteTo(writer, message);
                return send(target, writer.WrittenMemory);
            }
            finally {
                _sendWriter = writer;
            }
        }
    }

    /// <summary>
    /// Shared dispatch logic for the client- and server-side messengers. Allocation-free
    /// per message: handler lists are copy-on-write arrays and readers are reused per thread.
    /// </summary>
    internal sealed class MessageDispatcher<TSender> {
        private readonly NetMessageRegistry _registry;
        // Copy-on-write snapshot: subscribing replaces it, dispatch reads it without a lock.
        private Dictionary<uint, Action<TSender, NetReader>[]> _handlers = new Dictionary<uint, Action<TSender, NetReader>[]>();
        private readonly object _lock = new object();

        [ThreadStatic] private static NetReader? _cachedReader;

        public event Action<TSender, uint>? UnknownMessage;
        public event Action<TSender, Exception>? MalformedMessage;

        public MessageDispatcher(NetMessageRegistry registry) => _registry = registry;

        public void On<T>(Action<TSender, T> handler) where T : INetMessage, new() {
            uint id = _registry.GetId<T>();
            Action<TSender, NetReader> invoke = (sender, reader) => {
                var message = new T();
                message.Read(reader);
                handler(sender, message);
            };
            lock (_lock) {
                var handlers = new Dictionary<uint, Action<TSender, NetReader>[]>(_handlers);
                handlers.TryGetValue(id, out var existing);
                var updated = new Action<TSender, NetReader>[(existing?.Length ?? 0) + 1];
                existing?.CopyTo(updated, 0);
                updated[updated.Length - 1] = invoke;
                handlers[id] = updated;
                Volatile.Write(ref _handlers, handlers);
            }
        }

        /// <summary><paramref name="data"/> is only read during this call.</summary>
        public void Dispatch(TSender sender, ReadOnlyMemory<byte> data) {
            // Reuse one reader per thread; a nested dispatch on the same thread gets its own.
            var reader = _cachedReader ?? new NetReader(ReadOnlyMemory<byte>.Empty);
            _cachedReader = null;
            try {
                reader.Reset(data);
                uint id;
                try {
                    id = reader.ReadVarUInt();
                }
                catch (EndOfStreamException ex) {
                    MalformedMessage?.Invoke(sender, ex);
                    return;
                }
                Volatile.Read(ref _handlers).TryGetValue(id, out var handlers);
                if (handlers == null) {
                    if (!_registry.IsKnown(id)) {
                        UnknownMessage?.Invoke(sender, id);
                    }
                    return; // known type nobody listens to: fine
                }
                int bodyStart = reader.Position;
                try {
                    // Every subscriber reads the full body from the start.
                    foreach (var handler in handlers) {
                        reader.Reset(data.Slice(bodyStart));
                        handler(sender, reader);
                    }
                }
                catch (EndOfStreamException ex) {
                    MalformedMessage?.Invoke(sender, ex);
                }
            }
            finally {
                reader.Reset(ReadOnlyMemory<byte>.Empty); // don't pin the pooled buffer
                _cachedReader = reader;
            }
        }
    }

    /// <summary>
    /// Typed messages on top of a <see cref="NetClient"/>:
    /// <code>
    /// var messenger = new NetClientMessenger(client, registry);
    /// messenger.On&lt;Chat&gt;(chat => Console.WriteLine(chat.Text));
    /// await messenger.SendAsync(0, new Chat { Text = "hi" });
    /// </code>
    /// Handlers run where the client raises DataReceived (PollEvents caller in Polled mode).
    /// </summary>
    public sealed class NetClientMessenger : IDisposable {
        private readonly NetClient _client;
        private readonly NetMessageRegistry _registry;
        private readonly MessageDispatcher<NetClient> _dispatcher;
        private readonly HashSet<byte>? _channels;

        /// <summary>A message id that isn't registered arrived (version skew?).</summary>
        public event Action<uint>? UnknownMessage;
        /// <summary>A message failed to parse.</summary>
        public event Action<Exception>? MalformedMessage;

        /// <param name="client">The client to send through and listen on.</param>
        /// <param name="registry">Message types and ids; must match the server's.</param>
        /// <param name="channels">Channels carrying messages; empty = every channel.</param>
        public NetClientMessenger(NetClient client, NetMessageRegistry registry, params byte[] channels) {
            _client = client ?? throw new ArgumentNullException(nameof(client));
            _registry = registry ?? throw new ArgumentNullException(nameof(registry));
            _channels = channels.Length > 0 ? new HashSet<byte>(channels) : null;
            _dispatcher = new MessageDispatcher<NetClient>(registry);
            _dispatcher.UnknownMessage += (_, id) => UnknownMessage?.Invoke(id);
            _dispatcher.MalformedMessage += (_, ex) => MalformedMessage?.Invoke(ex);
            _client.MessageReceived += OnData;
        }

        public void On<T>(Action<T> handler) where T : INetMessage, new() {
            _dispatcher.On<T>((_, message) => handler(message));
        }

        /// <summary>Serializes into a reused per-thread buffer: no allocation per message.</summary>
        public ValueTask SendAsync<T>(byte channel, T message, CancellationToken ct = default) where T : INetMessage {
            return _registry.SendWith(message, (_client, channel, ct), static (target, bytes) => target._client.SendAsync(target.channel, bytes, target.ct));
        }

        private void OnData(byte channel, ReadOnlyMemory<byte> data) {
            if (_channels == null || _channels.Contains(channel)) {
                _dispatcher.Dispatch(_client, data);
            }
        }

        public void Dispose() => _client.MessageReceived -= OnData;
    }

    /// <summary>Typed messages on top of a <see cref="NetServer"/>; handlers get the sending connection.</summary>
    public sealed class NetServerMessenger : IDisposable {
        private readonly NetServer _server;
        private readonly NetMessageRegistry _registry;
        private readonly MessageDispatcher<NetConnection> _dispatcher;
        private readonly HashSet<byte>? _channels;

        public event Action<NetConnection, uint>? UnknownMessage;
        public event Action<NetConnection, Exception>? MalformedMessage;

        /// <param name="server">The server to send through and listen on.</param>
        /// <param name="registry">Message types and ids; must match the clients'.</param>
        /// <param name="channels">Channels carrying messages; empty = every channel.</param>
        public NetServerMessenger(NetServer server, NetMessageRegistry registry, params byte[] channels) {
            _server = server ?? throw new ArgumentNullException(nameof(server));
            _registry = registry ?? throw new ArgumentNullException(nameof(registry));
            _channels = channels.Length > 0 ? new HashSet<byte>(channels) : null;
            _dispatcher = new MessageDispatcher<NetConnection>(registry);
            _dispatcher.UnknownMessage += (connection, id) => UnknownMessage?.Invoke(connection, id);
            _dispatcher.MalformedMessage += (connection, ex) => MalformedMessage?.Invoke(connection, ex);
            _server.MessageReceived += OnData;
        }

        public void On<T>(Action<NetConnection, T> handler) where T : INetMessage, new() => _dispatcher.On(handler);

        /// <summary>Serializes into a reused per-thread buffer: no allocation per message.</summary>
        public ValueTask SendAsync<T>(NetConnection connection, byte channel, T message, CancellationToken ct = default) where T : INetMessage {
            return _registry.SendWith(message, (connection, channel, ct), static (target, bytes) => target.connection.SendAsync(target.channel, bytes, target.ct));
        }

        /// <summary>Serializes once, sends to every open connection.</summary>
        public Task BroadcastAsync<T>(byte channel, T message, CancellationToken ct = default) where T : INetMessage {
            return _server.BroadcastAsync(channel, _registry.Serialize(message), ct);
        }

        private void OnData(NetConnection connection, byte channel, ReadOnlyMemory<byte> data) {
            if (_channels == null || _channels.Contains(channel)) {
                _dispatcher.Dispatch(connection, data);
            }
        }

        public void Dispose() => _server.MessageReceived -= OnData;
    }
}
