using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Orleans.Internal;
using Xunit;

#pragma warning disable 618

namespace UnitTests
{
    public class Async_TimingTests
    {

        [Fact, TestCategory("Functional"), TestCategory("AsynchronyPrimitives")]
        public async Task Async_Task_WithTimeout_NoThrow()
        {
            TimeSpan timeout = TimeSpan.FromMilliseconds(2000);
            TimeSpan sleepTime = TimeSpan.FromMilliseconds(4000);
            TimeSpan delta = TimeSpan.FromMilliseconds(200);
            Stopwatch watch = new Stopwatch();
            watch.Start();

            Task<int> promise = Task<int>.Factory.StartNew(() =>
                {
                    Thread.Sleep(sleepTime);
                    return 5;
                });

            var result = await promise.WithTimeout(timeout).NoThrow();
            watch.Stop();

            Assert.False(promise.IsCompleted);
            Assert.Same(promise, result);
            Assert.True(watch.Elapsed >= timeout - delta, watch.Elapsed.ToString());
            Assert.True(watch.Elapsed <= timeout + delta, watch.Elapsed.ToString());
            Assert.True(watch.Elapsed < sleepTime, watch.Elapsed.ToString());
        }

        [Fact, TestCategory("Functional"), TestCategory("AsynchronyPrimitives")]
        public async Task Async_Task_WithTimeout_Await()
        {
            TimeSpan timeout = TimeSpan.FromMilliseconds(2000);
            TimeSpan sleepTime = TimeSpan.FromMilliseconds(4000);
            TimeSpan delta = TimeSpan.FromMilliseconds(300);
            Stopwatch watch = Stopwatch.StartNew();

            Task<int> promise = Task<int>.Factory.StartNew(() =>
            {
                Thread.Sleep(sleepTime);
                return 5;
            });

            await Assert.ThrowsAsync<TimeoutException>(async () => await promise.WithTimeout(timeout));
            watch.Stop();

            Assert.True(watch.Elapsed >= timeout - delta, watch.Elapsed.ToString());
            Assert.True(watch.Elapsed <= timeout + delta, watch.Elapsed.ToString());
            Assert.True(watch.Elapsed < sleepTime, watch.Elapsed.ToString());
        }
    }
}

#pragma warning restore 618

