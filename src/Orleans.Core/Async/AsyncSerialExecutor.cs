using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Orleans.Internal;

namespace Orleans
{
    /// <summary>
    /// A utility class that provides serial execution of async functions.
    /// In can be used inside reentrant grain code to execute some methods in a non-reentrant (serial) way.
    /// </summary>
    public class AsyncSerialExecutor
    {
        private readonly ConcurrentQueue<(Task<Task> Task, Task Result)> actions = new();
        private readonly InterlockedExchangeLock locker = new();

        private class InterlockedExchangeLock
        {
            private const int Locked = 1;
            private const int Unlocked = 0;
            private int lockState = Unlocked;

            public bool TryGetLock() => Interlocked.CompareExchange(ref lockState, Locked, Unlocked) == Unlocked;

            public void ReleaseLock() => Volatile.Write(ref lockState, Unlocked);
        }

        /// <summary>
        /// Submit the next function for execution. It will execute after all previously submitted functions have finished, without interleaving their executions.
        /// Returns a promise that represents the execution of this given function. 
        /// The returned promise will be resolved when the given function is done executing.
        /// </summary>
        public Task AddNext(Func<Task> func)
        {
            if (locker.TryGetLock())
            {
                if (actions.IsEmpty)
                {
                    var running = OrleansTaskExtentions.SafeExecute(func);
                    _ = ExecuteNext(running);
                    return running;
                }
                locker.ReleaseLock();
            }

            var task = new Task<Task>(func);
            var unwrap = task.Unwrap();
            actions.Enqueue((task, unwrap));
            _ = ExecuteNext();
            return unwrap;
        }

        private async Task ExecuteNext(Task running = null)
        {
            while (running != null || locker.TryGetLock())
                try
                {
                    if (running != null)
                    {
                        await running.SuppressExceptions();
                        running = null;
                    }

                    while (actions.TryDequeue(out var action))
                    {
                        action.Task.Start();
                        await action.Result.SuppressExceptions();
                    }
                }
                finally
                {
                    locker.ReleaseLock();
                }
        }
    }
}
