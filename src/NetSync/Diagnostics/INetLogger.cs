using System;

namespace NetSync.Diagnostics {
    public enum NetLogLevel {
        Debug = 0,
        Info = 1,
        Warning = 2,
        Error = 3
    }

    /// <summary>
    /// Logging abstraction so the core stays platform-agnostic.
    /// Implementations must be thread-safe: transports log from background threads.
    /// </summary>
    public interface INetLogger {
        void Log(NetLogLevel level, string message);
    }

    /// <summary>Discards everything. Default when no logger is supplied.</summary>
    public sealed class NullNetLogger : INetLogger {
        public static readonly NullNetLogger Instance = new NullNetLogger();
        private NullNetLogger() { }
        public void Log(NetLogLevel level, string message) { }
    }

    /// <summary>Writes to <see cref="Console"/>. Handy for samples and server apps.</summary>
    public sealed class ConsoleNetLogger : INetLogger {
        public static readonly ConsoleNetLogger Instance = new ConsoleNetLogger();
        public NetLogLevel MinLevel { get; set; } = NetLogLevel.Info;

        public void Log(NetLogLevel level, string message) {
            if (level < MinLevel) {
                return;
            }
            Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] [{level}] {message}");
        }
    }

    internal static class NetLoggerExtensions {
        public static void Debug(this INetLogger logger, string message) => logger.Log(NetLogLevel.Debug, message);
        public static void Info(this INetLogger logger, string message) => logger.Log(NetLogLevel.Info, message);
        public static void Warning(this INetLogger logger, string message) => logger.Log(NetLogLevel.Warning, message);
        public static void Error(this INetLogger logger, string message) => logger.Log(NetLogLevel.Error, message);
    }
}
