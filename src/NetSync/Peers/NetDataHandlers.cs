using System;

namespace NetSync.Peers {
    /// <summary>
    /// Zero-copy data handler for <see cref="NetClient.MessageReceived"/>.
    /// <paramref name="data"/> points into a pooled buffer and is valid only until the
    /// handler returns: parse it, pass it to SendAsync (which copies before returning),
    /// or copy it (<c>data.ToArray()</c>) if you need it later.
    /// </summary>
    public delegate void NetDataHandler(byte channel, ReadOnlyMemory<byte> data);

    /// <summary>
    /// Zero-copy data handler for <see cref="NetServer.MessageReceived"/>; same lifetime
    /// rule as <see cref="NetDataHandler"/>.
    /// </summary>
    public delegate void NetConnectionDataHandler(NetConnection connection, byte channel, ReadOnlyMemory<byte> data);
}
