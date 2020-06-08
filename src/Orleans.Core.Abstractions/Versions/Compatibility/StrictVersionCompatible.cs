using System;
using Orleans.Concurrency;

namespace Orleans.Versions.Compatibility
{
    [Serializable, Immutable]
    public sealed class StrictVersionCompatible : CompatibilityStrategy
    {
        public static StrictVersionCompatible Singleton { get; } = new StrictVersionCompatible();
    }
}