using NetSync.Benchmarks;

// Usage:
//   dotnet run -c Release -- micro        framing/fragmentation micro-benchmarks
//   dotnet run -c Release -- throughput   end-to-end loopback throughput/latency/allocation report
//   dotnet run -c Release -- alloc [relay] which types the hot path allocates (diagnostics)
//   dotnet run -c Release -- load [N]     N clients (default 1000) on one server
//   dotnet run -c Release -- compare      NetSync next to LiteNetLib (same methodology)
//   dotnet run -c Release -- reliability  CPU cost of the reliability engine alone
//   dotnet run -c Release                 micro + throughput

if (args.Length == 0 || args[0] == "micro") {
    FramingBenchmarks.Run();
}
if (args.Length == 0 || args[0] == "throughput") {
    await ThroughputHarness.RunAsync();
}
if (args.Length > 0 && args[0] == "alloc") {
    await AllocationTracer.RunAsync(relayed: args.Length > 1 && args[1] == "relay");
}
if (args.Length > 0 && args[0] == "compare") {
    await ComparisonHarness.RunAsync();
}
if (args.Length > 0 && args[0] == "reliability") {
    ReliabilityProfile.Run();
}
if (args.Length > 0 && args[0] == "load") {
    await LoadHarness.RunAsync(args.Length > 1 ? int.Parse(args[1]) : 1000);
}
