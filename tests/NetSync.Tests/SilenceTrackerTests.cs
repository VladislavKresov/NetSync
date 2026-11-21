using NetSync.Transports;
using Xunit;

namespace NetSync.Tests {
    /// <summary>Keepalive's verdict, round by round, with the time given explicitly.</summary>
    public class SilenceTrackerTests {
        private static readonly TransportSettings Settings = new TransportSettings { PingIntervalMs = 200, PingTimeoutMs = 1000 };

        [Fact]
        public void A_Silent_Peer_Is_Dead_After_Enough_Rounds_And_The_Timeout() {
            var tracker = new SilenceTracker();
            long lastHeard = 0;
            int deadAtRound = -1;
            for (int round = 1; round <= 20 && deadAtRound < 0; round++) {
                if (tracker.IsDead(lastHeard, round * 200, Settings)) {
                    deadAtRound = round;
                }
            }
            // 1000 / 200 = 5 silent rounds, and more than 1000 ms since the peer was heard.
            Assert.Equal(6, deadAtRound);
        }

        [Fact]
        public void Hearing_From_The_Peer_Starts_The_Count_Again() {
            var tracker = new SilenceTracker();
            for (int round = 1; round <= 4; round++) {
                Assert.False(tracker.IsDead(0, round * 200, Settings));
            }
            Assert.False(tracker.IsDead(950, 1000, Settings)); // a pong arrived during that round
            for (int round = 6; round <= 9; round++) {
                Assert.False(tracker.IsDead(950, round * 200, Settings));
            }
        }

        [Fact]
        public void A_Stall_Of_Our_Own_Process_Is_Not_Blamed_On_The_Peer() {
            var tracker = new SilenceTracker();
            Assert.False(tracker.IsDead(0, 200, Settings));
            // Our process froze for 30 s: one round ran, so the peer had one chance to answer, not 150.
            Assert.False(tracker.IsDead(0, 30_200, Settings));
            Assert.False(tracker.IsDead(30_250, 30_400, Settings)); // and it did answer
        }

        [Fact]
        public void No_Timeout_Means_Never_Dead() {
            var tracker = new SilenceTracker();
            var settings = new TransportSettings { PingIntervalMs = 200, PingTimeoutMs = 0 };
            for (int round = 1; round <= 100; round++) {
                Assert.False(tracker.IsDead(0, round * 200, settings));
            }
        }
    }
}
