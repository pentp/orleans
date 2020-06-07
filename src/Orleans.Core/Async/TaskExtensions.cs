using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Orleans.Internal
{
    public static class OrleansTaskExtentions
    {
        internal static readonly Task<object> CanceledTask = TaskFromCanceled<object>();
        internal static readonly Task<object> CompletedTask = Task.FromResult(default(object));

        /// <summary>
        /// Returns a <see cref="Task{Object}"/> for the provided <see cref="Task"/>.
        /// </summary>
        /// <param name="task">The task.</param>
        public static Task<object> ToUntypedTask(this Task task)
        {
            switch (task.Status)
            {
                case TaskStatus.RanToCompletion:
                    return CompletedTask;

                case TaskStatus.Faulted:
                    return TaskFromFaulted(task);

                case TaskStatus.Canceled:
                    return CanceledTask;

                default:
                    return ConvertAsync(task);
            }

            async Task<object> ConvertAsync(Task asyncTask)
            {
                await asyncTask;
                return null;
            }
        }

        /// <summary>
        /// Returns a <see cref="Task{Object}"/> for the provided <see cref="Task{T}"/>.
        /// </summary>
        /// <typeparam name="T">The underlying type of <paramref name="task"/>.</typeparam>
        /// <param name="task">The task.</param>
        public static Task<object> ToUntypedTask<T>(this Task<T> task)
        {
            if (typeof(T) == typeof(object))
                return task as Task<object>;

            switch (task.Status)
            {
                case TaskStatus.RanToCompletion:
                    return Task.FromResult((object)task.Result);

                case TaskStatus.Faulted:
                    return TaskFromFaulted(task);

                case TaskStatus.Canceled:
                    return CanceledTask;

                default:
                    return ConvertAsync(task);
            }

            async Task<object> ConvertAsync(Task<T> asyncTask)
            {
                return await asyncTask.ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Returns a <see cref="Task{Object}"/> for the provided <see cref="Task{T}"/>.
        /// </summary>
        /// <typeparam name="T">The underlying type of <paramref name="task"/>.</typeparam>
        /// <param name="task">The task.</param>
        internal static Task<T> ToTypedTask<T>(this Task<object> task)
        {
            if (typeof(T) == typeof(object))
                return task as Task<T>;

            switch (task.Status)
            {
                case TaskStatus.RanToCompletion:
                    return Task.FromResult((T)task.Result);

                case TaskStatus.Faulted:
                    return TaskFromFaulted<T>(task);

                case TaskStatus.Canceled:
                    return TaskFromCanceled<T>();

                default:
                    return ConvertAsync(task);
            }

            async Task<T> ConvertAsync(Task<object> asyncTask)
            {
                var result = await asyncTask.ConfigureAwait(false);

                if (result is null)
                {
                    if (!NullabilityHelper<T>.IsNullableType)
                    {
                        ThrowInvalidTaskResultType(typeof(T));
                    }

                    return default;
                }

                return (T)result;
            }
        }

        private static class NullabilityHelper<T>
        {
            /// <summary>
            /// True if <typeparamref name="T" /> is an instance of a nullable type (a reference type or <see cref="Nullable{T}"/>), otherwise false.
            /// </summary>
            public static readonly bool IsNullableType = !typeof(T).IsValueType || typeof(T).IsConstructedGenericType && typeof(T).GetGenericTypeDefinition() == typeof(Nullable<>);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void ThrowInvalidTaskResultType(Type type)
        {
            var message = $"Expected result of type {type} but encountered a null value. This may be caused by a grain call filter swallowing an exception.";
            throw new InvalidOperationException(message);
        }

        /// <summary>
        /// Returns a <see cref="Task{Object}"/> for the provided <see cref="Task{Object}"/>.
        /// </summary>
        /// <param name="task">The task.</param>
        public static Task<object> ToUntypedTask(this Task<object> task)
        {
            return task;
        }

        private static Task<object> TaskFromFaulted(Task task)
        {
            var completion = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
            completion.SetException(task.Exception.InnerExceptions);
            return completion.Task;
        }

        private static Task<T> TaskFromFaulted<T>(Task task)
        {
            var completion = new TaskCompletionSource<T>(TaskCreationOptions.RunContinuationsAsynchronously);
            completion.SetException(task.Exception.InnerExceptions);
            return completion.Task;
        }

        private static Task<T> TaskFromCanceled<T>()
        {
            var completion = new TaskCompletionSource<T>(TaskCreationOptions.RunContinuationsAsynchronously);
            completion.SetCanceled();
            return completion.Task;
        }

        /// <summary>
        /// Unwraps the <see cref="AggregateException"/> if it just wraps an inner exception
        /// </summary>
        public static Exception Unwrap(this AggregateException e)
            => e?.InnerException is null || e.InnerExceptions.Count > 1 ? e : e.InnerException;

        public static Task LogException(this Task task, Action<Exception> logger)
        {
            task.ContinueWith((t, state) => ((Action<Exception>)state)(t.Exception.Unwrap()), logger, TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously);
            return task;
        }

        // Executes an async function such as Exception is never thrown but rather always returned as a broken task.
        public static Task SafeExecute(Func<Task> action)
        {
            try
            {
                return action() ?? Task.CompletedTask;
            }
            catch (Exception ex)
            {
                return Task.FromException(ex);
            }
        }

        public static Task SafeExecute<T>(T state, Func<T, Task> action)
        {
            try
            {
                return action(state) ?? Task.CompletedTask;
            }
            catch (Exception ex)
            {
                return Task.FromException(ex);
            }
        }

        internal static String ToString(this Task t)
        {
            return t == null ? "null" : string.Format("[Id={0}, Status={1}]", t.Id, Enum.GetName(typeof(TaskStatus), t.Status));
        }

        /// <summary>
        /// This will apply a timeout delay to the task, allowing us to exit early
        /// </summary>
        /// <param name="taskToComplete">The task we will timeout after timeSpan</param>
        /// <param name="timeout">Amount of time to wait before timing out</param>
        /// <param name="exceptionMessage">Text to put into the timeout exception message</param>
        /// <exception cref="TimeoutException">If we time out we will get this exception</exception>
        /// <returns>The completed task</returns>
        public static async Task WithTimeout(this Task taskToComplete, TimeSpan timeout, string exceptionMessage = null)
        {
            if (taskToComplete.IsCompleted)
            {
                await taskToComplete;
                return;
            }

            var timeoutCancellationTokenSource = new CancellationTokenSource();
            var completedTask = await Task.WhenAny(taskToComplete, Task.Delay(timeout, timeoutCancellationTokenSource.Token));

            // We got done before the timeout, or were able to complete before this code ran, return the result
            if (taskToComplete == completedTask)
            {
                timeoutCancellationTokenSource.Cancel();
                // Await this so as to propagate the exception correctly
                await taskToComplete;
                return;
            }

            // We did not complete before the timeout, we fire and forget to ensure we observe any exceptions that may occur
            taskToComplete.Ignore();
            var errorMessage = exceptionMessage ?? $"WithTimeout has timed out after {timeout}";
            throw new TimeoutException(errorMessage);
        }

        /// <summary>
        /// This will apply a timeout delay to the task, allowing us to exit early
        /// </summary>
        /// <param name="taskToComplete">The task we will timeout after timeSpan</param>
        /// <param name="timeSpan">Amount of time to wait before timing out</param>
        /// <param name="exceptionMessage">Text to put into the timeout exception message</param>
        /// <exception cref="TimeoutException">If we time out we will get this exception</exception>
        /// <exception cref="TimeoutException">If we time out we will get this exception</exception>
        /// <returns>The value of the completed task</returns>
        public static async Task<T> WithTimeout<T>(this Task<T> taskToComplete, TimeSpan timeSpan, string exceptionMessage = null)
        {
            if (taskToComplete.IsCompleted)
            {
                return await taskToComplete;
            }

            var timeoutCancellationTokenSource = new CancellationTokenSource();
            var completedTask = await Task.WhenAny(taskToComplete, Task.Delay(timeSpan, timeoutCancellationTokenSource.Token));

            // We got done before the timeout, or were able to complete before this code ran, return the result
            if (taskToComplete == completedTask)
            {
                timeoutCancellationTokenSource.Cancel();
                // Await this so as to propagate the exception correctly
                return await taskToComplete;
            }

            // We did not complete before the timeout, we fire and forget to ensure we observe any exceptions that may occur
            taskToComplete.Ignore();
            var errorMessage = exceptionMessage ?? $"WithTimeout has timed out after {timeSpan}";
            throw new TimeoutException(errorMessage);
        }

        /// <summary>
        /// For making an uncancellable task cancellable, by ignoring its result.
        /// </summary>
        /// <param name="taskToComplete">The task to wait for unless cancelled</param>
        /// <param name="cancellationToken">A cancellation token for cancelling the wait</param>
        /// <returns></returns>
        internal static Task WithCancellation(this Task taskToComplete, CancellationToken cancellationToken)
        {
            if (taskToComplete.IsCompleted || !cancellationToken.CanBeCanceled)
            {
                return taskToComplete;
            }
            else if (cancellationToken.IsCancellationRequested)
            {
                taskToComplete.Ignore();
                return Task.FromCanceled<object>(cancellationToken);
            }
            else
            {
                return MakeCancellable(taskToComplete, cancellationToken);
            }
        }

        private static async Task MakeCancellable(Task task, CancellationToken cancellationToken)
        {
            var tcs = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
            using (cancellationToken.Register(() => tcs.TrySetCanceled(cancellationToken)))
            {
                var firstToComplete = await Task.WhenAny(task, tcs.Task).ConfigureAwait(false);

                if (firstToComplete != task)
                {
                    task.Ignore();
                }

                await firstToComplete.ConfigureAwait(false);
            }
        }

        /// <summary>
        /// For making an uncancellable task cancellable, by ignoring its result.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="taskToComplete">The task to wait for unless cancelled</param>
        /// <param name="cancellationToken">A cancellation token for cancelling the wait</param>
        /// <returns></returns>
        internal static Task<T> WithCancellation<T>(this Task<T> taskToComplete, CancellationToken cancellationToken)
        {
            if (taskToComplete.IsCompleted || !cancellationToken.CanBeCanceled)
            {
                return taskToComplete;
            }
            else if (cancellationToken.IsCancellationRequested)
            {
                taskToComplete.Ignore();
                return Task.FromCanceled<T>(cancellationToken);
            }
            else
            {
                return MakeCancellable(taskToComplete, cancellationToken);
            }
        }

        private static async Task<T> MakeCancellable<T>(Task<T> task, CancellationToken cancellationToken)
        {
            var tcs = new TaskCompletionSource<T>(TaskCreationOptions.RunContinuationsAsynchronously);
            using (cancellationToken.Register(() => tcs.TrySetCanceled(cancellationToken)))
            {
                var firstToComplete = await Task.WhenAny(task, tcs.Task).ConfigureAwait(false);

                if (firstToComplete != task)
                {
                    task.Ignore();
                }

                return await firstToComplete.ConfigureAwait(false);
            }
        }

        internal static Task WhenCancelled(this CancellationToken token)
        {
            if (token.IsCancellationRequested)
            {
                return Task.CompletedTask;
            }

            var waitForCancellation = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
            token.Register(obj =>
            {
                var tcs = (TaskCompletionSource<object>)obj;
                tcs.TrySetResult(null);
            }, waitForCancellation);

            return waitForCancellation.Task;
        }

        internal static Task WhenCancelled(this CancellationToken token, TimeSpan delay)
        {
            return token.IsCancellationRequested ? Task.Delay(delay)
                : token.CanBeCanceled ? Task.WhenAll(Task.Delay(delay), token.WhenCancelled())
                : Task.Delay(-1);
        }

        /// <summary>
        /// Returns an awaitable that suppresses exceptions, but still observes them.
        /// </summary>
        public static IgnoreExceptionsAwaiter IgnoreExceptions(this Task task) => new(task);

        /// <summary>
        /// Returns an awaitable that suppresses exceptions.
        /// </summary>
        public static SuppressExceptionsAwaiter SuppressExceptions(this Task task) => new(task);

        /// <summary>
        /// Returns an awaitable that suppresses exceptions and runs continuations on the default scheduler.
        /// </summary>
        public static SuppressExceptionsCfgAwaiter SuppressExceptionsAndContext(this Task task) => new(task);

        public readonly struct IgnoreExceptionsAwaiter : ICriticalNotifyCompletion
        {
            private readonly Task _task;
            public IgnoreExceptionsAwaiter(Task task) => _task = task;
            public IgnoreExceptionsAwaiter GetAwaiter() => this;
            public bool IsCompleted => _task.IsCompleted;
            public void OnCompleted(Action action) => _task.GetAwaiter().OnCompleted(action);
            public void UnsafeOnCompleted(Action action) => _task.GetAwaiter().UnsafeOnCompleted(action);
            public void GetResult() => _ = _task.Exception;
        }

        public readonly struct SuppressExceptionsAwaiter : ICriticalNotifyCompletion
        {
            private readonly Task _task;
            public SuppressExceptionsAwaiter(Task task) => _task = task;
            public SuppressExceptionsAwaiter GetAwaiter() => this;
            public bool IsCompleted => _task.IsCompleted;
            public void OnCompleted(Action action) => _task.GetAwaiter().OnCompleted(action);
            public void UnsafeOnCompleted(Action action) => _task.GetAwaiter().UnsafeOnCompleted(action);
            public Task GetResult() => _task;
        }

        public readonly struct SuppressExceptionsCfgAwaiter : ICriticalNotifyCompletion
        {
            private readonly Task _task;
            public SuppressExceptionsCfgAwaiter(Task task) => _task = task;
            public SuppressExceptionsCfgAwaiter GetAwaiter() => this;
            public bool IsCompleted => _task.IsCompleted;
            public void OnCompleted(Action action) => _task.ConfigureAwait(false).GetAwaiter().OnCompleted(action);
            public void UnsafeOnCompleted(Action action) => _task.ConfigureAwait(false).GetAwaiter().UnsafeOnCompleted(action);
            public Task GetResult() => _task;
        }
    }
}
