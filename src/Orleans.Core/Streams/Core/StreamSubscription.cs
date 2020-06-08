using System;
using Orleans.Concurrency;
using Orleans.Runtime;

namespace Orleans.Streams.Core
{
    [Serializable, Immutable]
    public sealed class StreamSubscription
    {
        public StreamSubscription(Guid subscriptionId, string streamProviderName, IStreamIdentity streamId, GrainId grainId)
        {
            this.SubscriptionId = subscriptionId;
            this.StreamProviderName = streamProviderName;
            this.StreamId = streamId;
            this.GrainId = grainId;
        }

        public Guid SubscriptionId { get; }
        public string StreamProviderName { get; }
        public IStreamIdentity StreamId { get; }
        public GrainId GrainId { get; }
    }
}
