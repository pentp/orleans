using System;
using Orleans.Concurrency;

namespace Orleans.Versions.Selector
{
    [Serializable, Immutable]
    public sealed class AllCompatibleVersions : VersionSelectorStrategy
    {
        public static AllCompatibleVersions Singleton { get; } = new AllCompatibleVersions();
    }
}