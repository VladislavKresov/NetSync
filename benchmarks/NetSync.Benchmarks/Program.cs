using NetSync.Benchmarks;

// Usage:
//   dotnet run -c Release -- micro        framing/fragmentation micro-benchmarks
//   dotnet run -c Release -- throughput   end-to-end loopback throughput/latency/allocation report
//   dotnet run -c Release                 both

if (args.Length == 0 || args[0] == "micro") {
    FramingBenchmarks.Run();
}
if (args.Length == 0 || args[0] == "throughput") {
    await ThroughputHarness.RunAsync();
}
