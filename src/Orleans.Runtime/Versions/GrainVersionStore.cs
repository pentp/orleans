using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans;
using Orleans.Storage;
using Orleans.Versions;
using Orleans.Versions.Compatibility;
using Orleans.Versions.Selector;
using Microsoft.Extensions.DependencyInjection;
using System.Threading;

namespace Orleans.Runtime.Versions
{
    internal sealed class GrainVersionStore : IVersionStore, ILifecycleParticipant<ISiloLifecycle>, ILifecycleObserver
    {
        private readonly IInternalGrainFactory grainFactory;
        private readonly IServiceProvider services;
        private readonly string clusterId;
        private IVersionStoreGrain StoreGrain => this.grainFactory.GetGrain<IVersionStoreGrain>(this.clusterId);

        public bool IsEnabled { get; private set; }

        public GrainVersionStore(IInternalGrainFactory grainFactory, ILocalSiloDetails siloDetails, IServiceProvider services)
        {
            this.grainFactory = grainFactory;
            this.services = services;
            this.clusterId = siloDetails.ClusterId;
            this.IsEnabled = false;
        }

        public Task SetCompatibilityStrategy(CompatibilityStrategy strategy)
        {
            ThrowIfNotEnabled();
            return StoreGrain.SetCompatibilityStrategy(strategy);
        }

        public Task SetSelectorStrategy(VersionSelectorStrategy strategy)
        {
            ThrowIfNotEnabled();
            return StoreGrain.SetSelectorStrategy(strategy);
        }

        public Task SetCompatibilityStrategy(GrainInterfaceType interfaceType, CompatibilityStrategy strategy)
        {
            ThrowIfNotEnabled();
            return StoreGrain.SetCompatibilityStrategy(interfaceType, strategy);
        }

        public Task SetSelectorStrategy(GrainInterfaceType interfaceType, VersionSelectorStrategy strategy)
        {
            ThrowIfNotEnabled();
            return StoreGrain.SetSelectorStrategy(interfaceType, strategy);
        }

        public Task<Dictionary<GrainInterfaceType, CompatibilityStrategy>> GetCompatibilityStrategies()
        {
            ThrowIfNotEnabled();
            return StoreGrain.GetCompatibilityStrategies();
        }

        public Task<Dictionary<GrainInterfaceType, VersionSelectorStrategy>> GetSelectorStrategies()
        {
            ThrowIfNotEnabled();
            return StoreGrain.GetSelectorStrategies();
        }

        public Task<CompatibilityStrategy> GetCompatibilityStrategy()
        {
            ThrowIfNotEnabled();
            return StoreGrain.GetCompatibilityStrategy();
        }

        public Task<VersionSelectorStrategy> GetSelectorStrategy()
        {
            ThrowIfNotEnabled();
            return StoreGrain.GetSelectorStrategy();
        }

        private void ThrowIfNotEnabled()
        {
            if (!IsEnabled)
                throw new OrleansException("Version store not enabled, make sure the store is configured");
        }

        public void Participate(ISiloLifecycle lifecycle)
        {
            lifecycle.Subscribe(nameof(GrainVersionStore), ServiceLifecycleStage.ApplicationServices, this);
        }

        Task ILifecycleObserver.OnStart(CancellationToken token)
        {
            this.IsEnabled = this.services.GetService<IGrainStorage>() != null;
            return Task.CompletedTask;
        }

        Task ILifecycleObserver.OnStop(CancellationToken token) => Task.CompletedTask;
    }
}
