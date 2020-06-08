using System;
using System.Collections.Immutable;
using Orleans.Concurrency;
using Orleans.Runtime;

namespace Orleans.Metadata
{
    /// <summary>
    /// Information about types which are available in the cluster.
    /// </summary>
    [Serializable, Immutable]
    public sealed class ClusterManifest
    {
        /// <summary>
        /// Creates a new <see cref="ClusterManifest"/> instance.
        /// </summary>
        public ClusterManifest(MajorMinorVersion version, ImmutableDictionary<SiloAddress, SiloManifest> silos)
        {
            this.Version = version;
            this.Silos = silos;
        }

        /// <summary>
        /// The version of this instance.
        /// </summary>
        public MajorMinorVersion Version { get; }

        /// <summary>
        /// Manifests for each silo in the cluster.
        /// </summary>
        public ImmutableDictionary<SiloAddress, SiloManifest> Silos { get; }
    }
}
