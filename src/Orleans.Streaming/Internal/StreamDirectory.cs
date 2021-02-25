using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Orleans.Runtime;

namespace Orleans.Streams
{
    /// <summary>
    /// Stores all streams associated with a specific silo
    /// </summary>
    internal class StreamDirectory : IAsyncDisposable
    {
        private readonly ConcurrentDictionary<InternalStreamId, object> allStreams = new();

        internal IAsyncStream<T> GetOrAddStream<T>(InternalStreamId streamId, Func<InternalStreamId, IAsyncStream<T>> streamCreator)
        {
            var stream = allStreams.GetOrAdd(streamId, streamCreator);
            var streamOfT = stream as IAsyncStream<T>;
            if (streamOfT == null)
            {
                throw new Runtime.OrleansException($"Stream type mismatch. A stream can only support a single type of data. The generic type of the stream requested ({typeof(T)}) does not match the previously requested type ({stream.GetType().GetGenericArguments().FirstOrDefault()}).");
            }

            return streamOfT;
        }

        internal Task Cleanup(bool cleanupProducers, bool cleanupConsumers)
        {
            if (StreamResourceTestControl.TestOnlySuppressStreamCleanupOnDeactivate)
            {
                return Task.CompletedTask;
            }

            var promises = new List<Task>();
            foreach (var s in allStreams)
            {
                if (s.Value is IStreamControl streamControl)
                    promises.Add(streamControl.Cleanup(cleanupProducers, cleanupConsumers));
            }

            return Task.WhenAll(promises);
        }

        internal void Clear()
        {
            // This is a quick temporary solution to unblock testing for resource leakages for streams.
            allStreams.Clear();
        }

        public ValueTask DisposeAsync() => new(Cleanup(cleanupProducers: true, cleanupConsumers: false));
    }
}
