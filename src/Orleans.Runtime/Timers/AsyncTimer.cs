using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Orleans.Internal;

namespace Orleans.Runtime
{
    internal class AsyncTimer : IAsyncTimer
    {
        /// <summary>
        /// Timers can fire up to 3 seconds late before a warning is emitted and the instance is deemed unhealthy.
        /// </summary>
        private static readonly TimeSpan TimerDelaySlack = TimeSpan.FromSeconds(3);

        private readonly CancellationTokenSource cancellation = new CancellationTokenSource();
        private readonly TimeSpan period;
        private readonly string name;
        private readonly ILogger log;
        private DateTime lastFired = DateTime.MinValue;
        private DateTime? expected;

        public AsyncTimer(TimeSpan period, string name, ILogger log)
        {
            this.log = log;
            this.period = period;
            this.name = name;
        }

        /// <summary>
        /// Returns a task which completes after the required delay.
        /// </summary>
        /// <param name="overrideDelay">An optional override to this timer's configured period.</param>
        /// <returns><see langword="true"/> if the timer completed or <see langword="false"/> if the timer was cancelled</returns>
        public async Task<bool> NextTick(TimeSpan? overrideDelay = default)
        {
            if (cancellation.IsCancellationRequested) return false;

            var start = DateTime.UtcNow;
            var delay = overrideDelay ?? (lastFired.Ticks == 0 ? period : lastFired + period - start);
            if (delay.Ticks < 0) delay = default;

            var dueTime = start + delay;
            this.expected = dueTime;
            if (delay.Ticks > 0)
            {
                // for backwards compatibility, support timers with periods up to ReminderRegistry.MaxSupportedTimeout
                var maxDelay = TimeSpan.FromTicks(int.MaxValue * TimeSpan.TicksPerMillisecond);
                var maxWait = false;
                if (delay > maxDelay)
                {
                    delay -= maxDelay;
                    maxWait = true;
                }

                var task = await Task.Delay(delay, cancellation.Token).NoThrow();
                if (maxWait && !task.IsCanceled)
                {
                    task = await Task.Delay(int.MaxValue, cancellation.Token).NoThrow();
                }
                if (task.IsCanceled)
                {
                    await Task.Yield();
                    return false;
                }
            }

            var now = this.lastFired = DateTime.UtcNow;
            var overshoot = GetOvershootDelay(now, dueTime);
            if (overshoot > TimeSpan.Zero)
            {
                this.log?.LogWarning(
                    "Timer should have fired at {DueTime} but fired at {CurrentTime}, which is {Overshoot} longer than expected",
                    dueTime,
                    now,
                    overshoot);
            }

            return true;
        }

        private static TimeSpan GetOvershootDelay(DateTime now, DateTime dueTime)
        {
            if (dueTime == DateTime.MinValue) return TimeSpan.Zero;
            if (dueTime > now) return TimeSpan.Zero;

            var overshoot = now.Subtract(dueTime);
            if (overshoot > TimerDelaySlack) return overshoot;

            return TimeSpan.Zero;
        }

        public bool CheckHealth(DateTime lastCheckTime, out string reason)
        {
            var now = DateTime.UtcNow;
            var dueTime = this.expected.GetValueOrDefault();
            var overshoot = GetOvershootDelay(now, dueTime);
            if (overshoot > TimeSpan.Zero)
            {
                reason = $"{this.name} timer should have fired at {dueTime}, which is {overshoot} ago";
                return false;
            }

            reason = default;
            return true;
        }

        public void Dispose()
        {
            this.expected = default;
            this.cancellation.Cancel();
        }
    }
}
