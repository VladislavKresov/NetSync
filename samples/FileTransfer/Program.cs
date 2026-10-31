using System;
using System.Diagnostics;
using System.IO;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using NetSync;
using NetSync.Peers;
using NetSync.Streams;

// A file of any size next to real-time traffic, with progress on both ends.
//   dotnet run                              demo in one process: a 64 MB file while positions keep flowing
//   dotnet run -- server 7777 <folder>      receive files into <folder>
//   dotnet run -- client 7777 <file> [host] send a file
//
// Two channels share one UDP link: small, frequent positions, and the file on a reliable-ordered
// channel with Low priority — the transfer takes the spare bandwidth and backs off as soon as it
// starts delaying the positions. Memory stays at a few MB whatever the file size.

const byte Positions = 0;
const byte Files = 1;

if (args.Length == 0) {
    await DemoAsync();
}
else if (args.Length >= 3 && args[0] == "server") {
    using var stop = new CancellationTokenSource();
    Console.CancelKeyPress += (_, e) => { e.Cancel = true; stop.Cancel(); };
    await using var server = await StartServerAsync(int.Parse(args[1]), args[2]);
    Console.WriteLine($"Receiving files into {Path.GetFullPath(args[2])} on port {args[1]}. Ctrl+C to stop.");
    await Task.Delay(Timeout.Infinite, stop.Token).ContinueWith(_ => { });
}
else if (args.Length >= 3 && args[0] == "client") {
    await SendFileAsync(args.Length > 3 ? args[3] : "127.0.0.1", int.Parse(args[1]), args[2]);
}
else {
    Console.WriteLine("Usage: FileTransfer [server <port> <folder> | client <port> <file> [host]]");
}

static NetConfig MakeConfig() {
    var config = new NetConfig { EventDelivery = EventDelivery.Immediate };
    config.Channels[Positions] = new ChannelConfig(TransportType.Udp, ReliabilityMode.UnreliableSequenced);
    config.Channels[Files] = new ChannelConfig(TransportType.Udp, ReliabilityMode.ReliableOrdered, priority: ChannelPriority.Low);
    return config;
}

static async Task<Server> StartServerAsync(int port, string folder) {
    Directory.CreateDirectory(folder);
    var server = new NetServer(MakeConfig());
    var done = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
    server.ConnectionOpened += connection => {
        // One receiver per sender. The name comes from the remote: never use it as a path as is.
        var receiver = new NetStreamReceiver(info => File.Create(Path.Combine(folder, SafeName(info.Name))));
        receiver.StreamStarted += info => Console.WriteLine($"[server] receiving '{info.Name}' ({Megabytes(info.Length)}) from {connection}");
        receiver.StreamCompleted += info => {
            Console.WriteLine($"[server] saved '{info.Name}'");
            done.TrySetResult(Path.Combine(folder, SafeName(info.Name)));
        };
        receiver.StreamFailed += (info, reason) => Console.WriteLine($"[server] '{info.Name}' failed: {reason}");
        connection.Tag = receiver;
    };
    server.ConnectionClosed += (connection, _) => (connection.Tag as NetStreamReceiver)?.Dispose();
    server.MessageReceived += (connection, channel, data) => {
        if (channel == Files) {
            ((NetStreamReceiver)connection.Tag!).HandleMessage(data); // written straight from the network buffer
        }
        else {
            _ = connection.SendAsync(Positions, data); // echo positions: the client times their round trip
        }
    };
    await server.StartAsync(port);
    return new Server(server, done.Task);
}

static async Task SendFileAsync(string host, int port, string path) {
    using var client = new NetClient(MakeConfig());
    var rtt = new RoundTrips();
    client.MessageReceived += (channel, data) => rtt.Echoed(data.Span);
    await client.ConnectAsync(host, port);

    using var cts = new CancellationTokenSource();
    var positions = Task.Run(async () => {
        while (!cts.IsCancellationRequested) {
            await client.SendAsync(Positions, rtt.Stamp()); // 50 position updates a second
            await Task.Delay(20);
        }
    });

    await using var file = new ProgressStream(File.OpenRead(path));
    var stopwatch = Stopwatch.StartNew();
    var transfer = client.SendStreamAsync(Files, file, Path.GetFileName(path));
    while (await Task.WhenAny(transfer, Task.Delay(500)) != transfer) {
        Console.WriteLine($"[client] sent {Megabytes(file.Position)} of {Megabytes(file.Length)} — position round trip {rtt.Report()}");
    }
    await transfer;
    cts.Cancel();
    await positions;
    Console.WriteLine($"[client] {Megabytes(file.Length)} sent in {stopwatch.Elapsed.TotalSeconds:F1} s " +
                      $"({file.Length / 1048576.0 / stopwatch.Elapsed.TotalSeconds:F0} MB/s); position round trip {rtt.Report()}");
    client.Disconnect();
}

static async Task DemoAsync() {
    string source = Path.Combine(Path.GetTempPath(), "netsync-demo.bin");
    string folder = Path.Combine(Path.GetTempPath(), "netsync-received");
    var random = new byte[64 * 1024 * 1024];
    Random.Shared.NextBytes(random);
    await File.WriteAllBytesAsync(source, random);
    try {
        await using var server = await StartServerAsync(0, folder);
        await SendFileAsync("127.0.0.1", server.Port, source);
        string saved = await server.Saved.WaitAsync(TimeSpan.FromSeconds(30));
        bool intact = Hash(source).AsSpan().SequenceEqual(Hash(saved));
        Console.WriteLine(intact ? "File arrived intact (SHA-256 matches)." : "File differs!");
        File.Delete(saved);
    }
    finally {
        File.Delete(source);
    }
}

static byte[] Hash(string path) {
    using var stream = File.OpenRead(path);
    return SHA256.HashData(stream);
}

static string SafeName(string name) {
    string file = Path.GetFileName(name);
    return string.IsNullOrWhiteSpace(file) ? "received.bin" : file;
}

static string Megabytes(long bytes) => bytes < 0 ? "unknown size" : $"{bytes / 1048576.0:F1} MB";

sealed record Server(NetServer Inner, Task<string> Saved) : IAsyncDisposable {
    public int Port => Inner.Port;

    public async ValueTask DisposeAsync() {
        await Inner.StopAsync();
        Inner.Dispose();
    }
}

/// <summary>Timestamps positions and measures how long their echo takes.</summary>
sealed class RoundTrips {
    private readonly byte[] _stamp = new byte[8];
    private readonly object _lock = new object();
    private double _sumMs, _maxMs;
    private int _count;

    public ReadOnlyMemory<byte> Stamp() {
        BitConverter.TryWriteBytes(_stamp, Stopwatch.GetTimestamp());
        return _stamp; // SendAsync copies it before returning: safe to reuse
    }

    public void Echoed(ReadOnlySpan<byte> data) {
        double ms = (Stopwatch.GetTimestamp() - BitConverter.ToInt64(data)) * 1000.0 / Stopwatch.Frequency;
        lock (_lock) {
            _sumMs += ms;
            _maxMs = Math.Max(_maxMs, ms);
            _count++;
        }
    }

    public string Report() {
        lock (_lock) {
            string report = _count == 0 ? "n/a" : $"avg {_sumMs / _count:F2} ms, max {_maxMs:F2} ms";
            _sumMs = _maxMs = 0;
            _count = 0;
            return report;
        }
    }
}

/// <summary>Counts what the transfer has read so far.</summary>
sealed class ProgressStream : Stream {
    private readonly Stream _inner;
    private long _read;

    public ProgressStream(Stream inner) => _inner = inner;

    public override bool CanRead => true;
    public override bool CanSeek => _inner.CanSeek;
    public override bool CanWrite => false;
    public override long Length => _inner.Length;
    public override long Position { get => Interlocked.Read(ref _read); set => throw new NotSupportedException(); }

    public override int Read(byte[] buffer, int offset, int count) {
        int read = _inner.Read(buffer, offset, count);
        Interlocked.Add(ref _read, read);
        return read;
    }

    public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken ct) {
        return ReadAsync(buffer.AsMemory(offset, count), ct).AsTask();
    }

    public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken ct = default) {
        int read = await _inner.ReadAsync(buffer, ct);
        Interlocked.Add(ref _read, read);
        return read;
    }

    public override void Flush() { }
    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
    public override void SetLength(long value) => throw new NotSupportedException();
    public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

    protected override void Dispose(bool disposing) {
        if (disposing) {
            _inner.Dispose();
        }
        base.Dispose(disposing);
    }
}
