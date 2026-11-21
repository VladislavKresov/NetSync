using System;
using System.Threading;
using System.Threading.Tasks;
using NetSync.Peers;
using Xunit;

namespace NetSync.Tests {
    /// <summary>The ordering guarantees of the UDP-to-TCP switch, without sockets or timing.</summary>
    public class UdpHandoverUnitTests {
        [Fact]
        public async Task A_Send_During_The_Switch_Waits_Until_It_Completes() {
            var handover = new UdpHandover();
            Assert.True(handover.TryBegin());
            Assert.False(handover.TryEnter()); // no new message enters the reliability layer now

            int sent = 0;
            var buffer = new byte[] { 7 };
            var pending = handover.SendAfterSwitchAsync(buffer, data => {
                Assert.Equal(7, data.Span[0]); // a copy: the caller's buffer was changed meanwhile
                sent++;
                return default;
            });
            buffer[0] = 99;
            Assert.False(pending.IsCompleted);
            Assert.Equal(0, sent);

            Assert.True(await handover.FinishAsync(reliable: null, timeoutMs: 1000, CancellationToken.None));
            await pending;
            Assert.Equal(1, sent);
        }

        [Fact]
        public async Task The_Switch_Waits_For_Sends_Still_Inside_The_Reliability_Layer() {
            var handover = new UdpHandover();
            Assert.True(handover.TryEnter()); // a send started before the switch
            Assert.True(handover.TryBegin());

            var finish = handover.FinishAsync(reliable: null, timeoutMs: 5000, CancellationToken.None);
            await Task.Delay(50);
            Assert.False(finish.IsCompleted); // that send's packet must be handed over first

            handover.Exit();
            Assert.True(await finish);
        }

        [Fact]
        public async Task Closing_Fails_The_Waiting_Sends() {
            var handover = new UdpHandover();
            Assert.True(handover.TryBegin());
            var pending = handover.SendAfterSwitchAsync(new byte[] { 1 }, _ => default);
            handover.Abort();
            await Assert.ThrowsAsync<InvalidOperationException>(async () => await pending);
        }

        [Fact]
        public void Only_One_Switch_Begins() {
            var handover = new UdpHandover();
            Assert.True(handover.TryBegin());
            Assert.False(handover.TryBegin()); // both sides' Degrade crossed: the second is a no-op
            Assert.True(handover.HasStarted);
        }
    }
}
