using System;
using Orleans.Concurrency;

namespace Orleans.Versions.Selector
{
    [Serializable, Immutable]
    public sealed class MinimumVersion : VersionSelectorStrategy
    {
        public static MinimumVersion Singleton { get; } = new MinimumVersion();
    }
}