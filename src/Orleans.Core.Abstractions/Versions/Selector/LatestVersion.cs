using System;
using Orleans.Concurrency;

namespace Orleans.Versions.Selector
{
    [Serializable, Immutable]
    public sealed class LatestVersion : VersionSelectorStrategy
    {
        public static LatestVersion Singleton { get; } = new LatestVersion();
    }
}