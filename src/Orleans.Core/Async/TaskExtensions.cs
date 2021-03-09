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
        /// <returns>An awaitable with the given timeout</returns>
        public static WithTimeoutAwaiter WithTimeout(this Task taskToComplete, TimeSpan timeout, Func<TimeSpan, string> exceptionMessage = null) => new(taskToComplete, timeout, exceptionMessage);
        public static WithTimeoutAwaiter WithTimeout(this Task taskToComplete, TimeSpan timeout, string exceptionMessage) => new(taskToComplete, timeout, exceptionMessage);

        public readonly struct WithTimeoutAwaiter : ICriticalNotifyCompletion
        {
            private readonly Task _task;
            private readonly CancellationTokenSource _cancellation;
            private readonly object _message;
            private readonly TimeSpan _timeout;

            internal WithTimeoutAwaiter(Task task, TimeSpan timeout, object message)
            {
                _task = task;
                _timeout = timeout;
                if (task.IsCompleted || timeout == Timeout.InfiniteTimeSpan)
                {
                    _cancellation = null;
                    _message = null;
                }
                else
                {
                    _cancellation = new(timeout);
                    _message = message;
                }
            }

            public WithTimeoutAwaiter GetAwaiter() => this;
            public bool IsCompleted => _task.IsCompleted;
            void INotifyCompletion.OnCompleted(Action action) => throw new NotSupportedException();
            public void UnsafeOnCompleted(Action action) => Setup(action, _task, _cancellation);

            private static void Setup(Action action, Task task, CancellationTokenSource cancellation)
            {
                if (cancellation != null)
                {
                    var cont = new Continuation { Action = action };
                    cancellation.Token.UnsafeRegister(s => ((Continuation)s).MoveNext(), cont);
                    action = cont.MoveNext;
                }
                task.GetAwaiter().UnsafeOnCompleted(action);
            }

            private sealed class Continuation
            {
                public Action Action;
                public void MoveNext() => Interlocked.Exchange(ref Action, null)?.Invoke();
            }

            public void GetResult()
            {
                _cancellation?.Dispose();
                if (_task.IsCompleted)
                {
                    _task.GetAwaiter().GetResult();
                }
                else
                {
                    _task.Ignore();
                    throw new TimeoutException(_message switch { string s => s, Func<TimeSpan, string> f => f(_timeout), _ => $"WithTimeout has timed out after {_timeout}" });
                }
            }

            /// <summary>
            /// Returns an awaitable with the given timeout that does not throw exceptions.
            /// The caller is responsible for observing any exceptions if necessary.
            /// </summary>
            public NoThrowAwaiter NoThrow() => new(_task, _cancellation);

            public readonly struct NoThrowAwaiter : ICriticalNotifyCompletion
            {
                private readonly Task _task;
                private readonly CancellationTokenSource _cancellation;

                internal NoThrowAwaiter(Task task, CancellationTokenSource cancellation)
                {
                    _task = task;
                    _cancellation = cancellation;
                }

                public NoThrowAwaiter GetAwaiter() => this;
                public bool IsCompleted => _task.IsCompleted;
                void INotifyCompletion.OnCompleted(Action action) => throw new NotSupportedException();
                public void UnsafeOnCompleted(Action action) => Setup(action, _task, _cancellation);

                public Task GetResult()
                {
                    _cancellation?.Dispose();
                    return _task;
                }
            }
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

        /// <summary>
        /// For making an uncancellable task cancellable, by returning an awaitable that is completed when the given task completes or the token is canceled.
        /// Does not throw exceptions. The caller is responsible for observing any exceptions if necessary.
        /// </summary>
        public static WhenCompletedOrCanceledAwaiter WhenCompletedOrCanceled(this Task task, CancellationToken token) => new(task, token);

        public readonly struct WhenCompletedOrCanceledAwaiter : ICriticalNotifyCompletion
        {
            private readonly Task _task;
            private readonly CancellationToken _token;

            internal WhenCompletedOrCanceledAwaiter(Task task, CancellationToken token)
            {
                _task = task;
                _token = token;
            }

            public WhenCompletedOrCanceledAwaiter GetAwaiter() => this;
            public bool IsCompleted => _task.IsCompleted || _token.IsCancellationRequested;
            public Task GetResult() => _task;
            void INotifyCompletion.OnCompleted(Action action) => throw new NotSupportedException();

            public void UnsafeOnCompleted(Action action)
            {
                if (_token.CanBeCanceled)
                {
                    var cont = new Continuation { Action = action };
                    cont.Registration = _token.UnsafeRegister(s => ThreadPool.UnsafeQueueUserWorkItem((Continuation)s, true), cont);
                    action = cont.Completed;
                }
                _task.GetAwaiter().UnsafeOnCompleted(action);
            }

            private sealed class Continuation : IThreadPoolWorkItem
            {
                public Action Action;
                public CancellationTokenRegistration Registration;

                public void Execute() => MoveNext(false);
                public void Completed() => MoveNext(true);

                private void MoveNext(bool unregister)
                {
                    if (Interlocked.Exchange(ref Action, null) is { } action)
                    {
                        // reset resources before executing the callback - in case of cancellation the original task could hold a reference to this continuation for a long time
                        if (unregister) Registration.Unregister();
                        Registration = default;
                        action();
                    }
                }
            }

            public Task AsTask() => _task is { IsCompleted: false } t && !_token.IsCancellationRequested ? Task.WhenAny(t, Task.Delay(-1, _token)) : Task.CompletedTask;
        }

        /// <summary>
        /// For making an uncancellable task cancellable, by returning an awaitable that is completed when the given task completes or the token is canceled (after a grace period).
        /// Does not throw exceptions. The caller is responsible for observing any exceptions if necessary.
        /// </summary>
        public static WhenCompletedOrCanceledDelayAwaiter WhenCompletedOrCanceled(this Task task, CancellationToken token, TimeSpan gracePeriod) => new(task, token, gracePeriod);

        public readonly struct WhenCompletedOrCanceledDelayAwaiter : ICriticalNotifyCompletion
        {
            private readonly Task _task;
            private readonly CancellationToken _token;
            private readonly CancellationTokenSource _delay;

            internal WhenCompletedOrCanceledDelayAwaiter(Task task, CancellationToken token, TimeSpan gracePeriod)
            {
                _task = task;
                _token = token;
                _delay = task.IsCompleted || !token.CanBeCanceled ? null : new(gracePeriod);
            }

            public WhenCompletedOrCanceledDelayAwaiter GetAwaiter() => this;
            public bool IsCompleted => _task.IsCompleted;
            void INotifyCompletion.OnCompleted(Action action) => throw new NotSupportedException();

            public void UnsafeOnCompleted(Action action)
            {
                if (_delay != null)
                {
                    var cont = new Continuation { Action = action, Token = _token };
                    _delay.Token.UnsafeRegister(s => ((Continuation)s).GracePeriodEnded(), cont);
                    action = cont.Completed;
                }
                _task.GetAwaiter().UnsafeOnCompleted(action);
            }

            private sealed class Continuation : IThreadPoolWorkItem
            {
                public Action Action;
                public CancellationToken Token;
                public CancellationTokenRegistration Registration;

                public void GracePeriodEnded() => Registration = Token.UnsafeRegister(s => ThreadPool.UnsafeQueueUserWorkItem((Continuation)s, true), this);

                public void Execute() => MoveNext(false);
                public void Completed() => MoveNext(true);

                private void MoveNext(bool unregister)
                {
                    if (Interlocked.Exchange(ref Action, null) is { } action)
                    {
                        // reset resources before executing the callback - in case of cancellation the original task could hold a reference to this continuation for a long time
                        Token = default;
                        if (unregister) Registration.Unregister();
                        Registration = default;
                        action();
                    }
                }
            }

            public Task GetResult()
            {
                _delay?.Dispose();
                return _task;
            }
        }

        /// <summary>
        /// Returns an awaitable that suppresses exceptions, but still observes them.
        /// </summary>
        public static IgnoreExceptionsAwaiter IgnoreExceptions(this Task task) => new(task);

        public readonly struct IgnoreExceptionsAwaiter : ICriticalNotifyCompletion
        {
            private readonly Task _task;
            internal IgnoreExceptionsAwaiter(Task task) => _task = task;
            public IgnoreExceptionsAwaiter GetAwaiter() => this;
            public bool IsCompleted => _task.IsCompleted;
            public void OnCompleted(Action action) => _task.GetAwaiter().OnCompleted(action);
            public void UnsafeOnCompleted(Action action) => _task.GetAwaiter().UnsafeOnCompleted(action);
            public void GetResult() => _ = _task.Exception; // observe exceptions
        }

        /// <summary>
        /// Returns an awaitable that does not throw exceptions.
        /// The caller is responsible for observing any exceptions if necessary.
        /// </summary>
        public static NoThrowAwaiter<Task> NoThrow(this Task task) => new(task);
        public static NoThrowAwaiter<Task<T>> NoThrow<T>(this Task<T> task) => new(task);

        public readonly struct NoThrowAwaiter<T> : ICriticalNotifyCompletion where T:Task
        {
            private readonly T _task;
            internal NoThrowAwaiter(T task) => _task = task;
            public NoThrowAwaiter<T> GetAwaiter() => this;
            public bool IsCompleted => _task.IsCompleted;
            public void OnCompleted(Action action) => _task.GetAwaiter().OnCompleted(action);
            public void UnsafeOnCompleted(Action action) => _task.GetAwaiter().UnsafeOnCompleted(action);
            public T GetResult() => _task;
        }
    }
}
