using System;
using Orleans.Concurrency;

namespace Orleans.Versions.Compatibility
{
    [Serializable, Immutable]
    public sealed class AllVersionsCompatible : CompatibilityStrategy
    {
        public static AllVersionsCompatible Singleton { get; } = new AllVersionsCompatible();
    }
}
