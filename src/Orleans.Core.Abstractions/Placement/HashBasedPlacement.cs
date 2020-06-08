using System;
using Orleans.Concurrency;

namespace Orleans.Runtime
{
    [Serializable, Immutable]
    public sealed class HashBasedPlacement : PlacementStrategy
    {
        internal static HashBasedPlacement Singleton { get; } = new HashBasedPlacement();
    }
}
