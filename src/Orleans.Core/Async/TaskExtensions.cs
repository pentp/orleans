using System.Runtime.CompilerServices;

namespace Orleans.Internal
{
    /// <summary>
    /// Extensions for working with <see cref="Task"/> and <see cref="Task{TResult}"/>.
    /// </summary>
    internal static class OrleansTaskExtensions
    {
        public static ConfiguredTaskAwaitable SuppressThrowing(this Task task) => task.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing | ConfigureAwaitOptions.ContinueOnCapturedContext);

        public static void Ignore(this ValueTask valueTask)
        {
            if (!valueTask.IsCompletedSuccessfully)
            {
                valueTask.AsTask().Ignore();
            }
        }
    }
}
