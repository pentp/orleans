using System;
using Orleans.Concurrency;

namespace Orleans.Versions.Compatibility
{
    [Serializable, Immutable]
    public sealed class BackwardCompatible : CompatibilityStrategy
    {
        public static BackwardCompatible Singleton { get; } = new BackwardCompatible();
    }
}