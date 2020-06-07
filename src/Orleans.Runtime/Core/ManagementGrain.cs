using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Orleans.Internal;
using Orleans.Metadata;
using Orleans.Runtime.MembershipService;
using Orleans.Versions;
using Orleans.Versions.Compatibility;
using Orleans.Versions.Selector;

namespace Orleans.Runtime.Management
{
    /// <summary>
    /// Implementation class for the Orleans management grain.
    /// </summary>
    internal class ManagementGrain : Grain, IManagementGrain
    {
        private readonly IInternalGrainFactory internalGrainFactory;
        private readonly ISiloStatusOracle siloStatusOracle;
        private readonly IVersionStore versionStore;
        private readonly MembershipTableManager membershipTableManager;
        private readonly GrainManifest siloManifest;
        private readonly ClusterManifest clusterManifest;
        private readonly ILogger logger;
        private readonly Catalog catalog;

        public ManagementGrain(
            IInternalGrainFactory internalGrainFactory,
            ISiloStatusOracle siloStatusOracle,
            IVersionStore versionStore,
            ILogger<ManagementGrain> logger,
            MembershipTableManager membershipTableManager,
            IClusterManifestProvider clusterManifestProvider,
            Catalog catalog)
        {
            this.membershipTableManager = membershipTableManager;
            this.siloManifest = clusterManifestProvider.LocalGrainManifest;
            this.clusterManifest = clusterManifestProvider.Current;
            this.internalGrainFactory = internalGrainFactory;
            this.siloStatusOracle = siloStatusOracle;
            this.versionStore = versionStore;
            this.logger = logger;
            this.catalog = catalog;
        }

        public async Task<Dictionary<SiloAddress, SiloStatus>> GetHosts(bool onlyActive = false)
        {
            await this.membershipTableManager.Refresh();
            return this.siloStatusOracle.GetApproximateSiloStatuses(onlyActive);
        }

        public async Task<MembershipEntry[]> GetDetailedHosts(bool onlyActive = false)
        {
            logger.Info("GetDetailedHosts onlyActive={0}", onlyActive);

            await this.membershipTableManager.Refresh();

            var table = this.membershipTableManager.MembershipTableSnapshot;

            MembershipEntry[] result;
            if (onlyActive)
            {
                result = table.Entries
                    .Where(item => item.Value.Status == SiloStatus.Active)
                    .Select(x => x.Value)
                    .ToArray();
            }
            else
            {
                result = table.Entries
                    .Select(x => x.Value)
                    .ToArray();
            }

            return result;
        }

        public Task ForceGarbageCollection(SiloAddress[] siloAddresses)
        {
            var silos = GetSiloAddresses(siloAddresses);
            logger.Info("Forcing garbage collection on {0}", Utils.EnumerableToString(silos));
            return Task.WhenAll(silos.Select(s =>
                GetSiloControlReference(s).ForceGarbageCollection()));
        }

        public Task ForceActivationCollection(SiloAddress[] siloAddresses, TimeSpan ageLimit)
        {
            var silos = GetSiloAddresses(siloAddresses);
            return Task.WhenAll(silos.Select(s =>
                GetSiloControlReference(s).ForceActivationCollection(ageLimit)));
        }

        public async Task ForceActivationCollection(TimeSpan ageLimit)
        {
            Dictionary<SiloAddress, SiloStatus> hosts = await GetHosts(true);
            SiloAddress[] silos = hosts.Keys.ToArray();
            await ForceActivationCollection(silos, ageLimit);
        }

        public Task ForceRuntimeStatisticsCollection(SiloAddress[] siloAddresses)
        {
            var silos = GetSiloAddresses(siloAddresses);
            logger.Info("Forcing runtime statistics collection on {0}", Utils.EnumerableToString(silos));
            return Task.WhenAll(silos.Select(s =>
                GetSiloControlReference(s).ForceRuntimeStatisticsCollection()));
        }

        public Task<SiloRuntimeStatistics[]> GetRuntimeStatistics(SiloAddress[] siloAddresses)
        {
            var silos = GetSiloAddresses(siloAddresses);
            if (logger.IsEnabled(LogLevel.Debug)) logger.Debug("GetRuntimeStatistics on {0}", Utils.EnumerableToString(silos));
            return Task.WhenAll(silos.Select(s =>
                GetSiloControlReference(s).GetRuntimeStatistics()));
        }

        public async Task<SimpleGrainStatistic[]> GetSimpleGrainStatistics(SiloAddress[] hostsIds)
        {
            var res = await Task.WhenAll(GetSiloAddresses(hostsIds).Select(s =>
                GetSiloControlReference(s).GetSimpleGrainStatistics()));
            return res.SelectMany(s => s).ToArray();
        }

        public async Task<SimpleGrainStatistic[]> GetSimpleGrainStatistics()
        {
            Dictionary<SiloAddress, SiloStatus> hosts = await GetHosts(true);
            SiloAddress[] silos = hosts.Keys.ToArray();
            return await GetSimpleGrainStatistics(silos);
        }

        public async Task<DetailedGrainStatistic[]> GetDetailedGrainStatistics(string[] types = null, SiloAddress[] hostsIds = null)
        {
            if (hostsIds == null)
            {
                Dictionary<SiloAddress, SiloStatus> hosts = await GetHosts(true);
                hostsIds = hosts.Keys.ToArray();
            }

            var res = await Task.WhenAll(GetSiloAddresses(hostsIds).Select(s =>
                GetSiloControlReference(s).GetDetailedGrainStatistics(types)));
            return res.SelectMany(s => s).ToArray();
        }

        public async Task<int> GetGrainActivationCount(GrainReference grainReference)
        {
            var hosts = await GetHosts(true);
            var res = await Task.WhenAll(hosts.Select(s =>
                GetSiloControlReference(s.Key).GetDetailedGrainReport(grainReference.GrainId)));

            return res.Sum(r => r.LocalActivations.Count);
        }

        public Task SetCompatibilityStrategy(CompatibilityStrategy strategy)
        {
            return SetStrategy(strategy, (t, s) => t.SetCompatibilityStrategy(s));
        }

        public Task SetSelectorStrategy(VersionSelectorStrategy strategy)
        {
            return SetStrategy(strategy, (t, s) => t.SetSelectorStrategy(s));
        }

        public Task SetCompatibilityStrategy(GrainInterfaceType interfaceType, CompatibilityStrategy strategy)
        {
            CheckIfIsExistingInterface(interfaceType);
            return SetStrategy(strategy, (t, s) => t.SetCompatibilityStrategy(interfaceType, s));
        }

        public Task SetSelectorStrategy(GrainInterfaceType interfaceType, VersionSelectorStrategy strategy)
        {
            CheckIfIsExistingInterface(interfaceType);
            return SetStrategy(strategy, (t, s) => t.SetSelectorStrategy(interfaceType, s));
        }

        public async Task<int> GetTotalActivationCount()
        {
            var hosts = await GetHosts(true);
            var res = await Task.WhenAll(hosts.Select(s => GetSiloControlReference(s.Key).GetActivationCount()));
            return res.Sum();
        }

        public Task<object[]> SendControlCommandToProvider(string providerTypeFullName, string providerName, int command, object arg)
        {
            return ExecutePerSiloCall(isc => isc.SendControlCommandToProvider(providerTypeFullName, providerName, command, arg),
                String.Format("SendControlCommandToProvider of type {0} and name {1} command {2}.", providerTypeFullName, providerName, command));
        }

        public ValueTask<SiloAddress> GetActivationAddress(IAddressable reference)
        {
            var grainReference = reference as GrainReference;
            var grainId = grainReference.GrainId;

            GrainProperties grainProperties = default;
            if (!siloManifest.Grains.TryGetValue(grainId.Type, out grainProperties))
            {
                var grainManifest = clusterManifest.AllGrainManifests
                    .SelectMany(m => m.Grains.Where(g => g.Key == grainId.Type))
                    .FirstOrDefault();
                if (grainManifest.Value != null)
                {
                    grainProperties = grainManifest.Value;
                }
                else
                {
                    throw new ArgumentException($"Unable to find Grain type '{grainId.Type}'. Make sure it is added to the Application Parts Manager at the Silo configuration.");
                }
            }

            if (grainProperties != default &&
                grainProperties.Properties.TryGetValue(WellKnownGrainTypeProperties.PlacementStrategy, out string placementStrategy))
            {
                if (placementStrategy == nameof(StatelessWorkerPlacement))
                {
                    throw new InvalidOperationException(
                        $"Grain '{grainReference.ToString()}' is a Stateless Worker. This type of grain can't be looked up by this method"
                    );
                }
            }

            if (this.catalog.FastLookup(grainId, out var addresses))
            {
                var placementResult = addresses.FirstOrDefault();
                return new ValueTask<SiloAddress>(placementResult?.Silo);
            }

            return LookupAsync(grainId, catalog);

            async ValueTask<SiloAddress> LookupAsync(GrainId grainId, Catalog catalog)
            {
                var places = await catalog.FullLookup(grainId);
                return places.FirstOrDefault()?.Silo;
            }
        }

        private void CheckIfIsExistingInterface(GrainInterfaceType interfaceType)
        {
            GrainInterfaceType lookupId;
            if (GenericGrainInterfaceType.TryParse(interfaceType, out var generic))
            {
                lookupId = generic.Value;
            }
            else
            {
                lookupId = interfaceType;
            }

            if (!this.siloManifest.Interfaces.TryGetValue(lookupId, out _))
            {
                throw new ArgumentException($"Interface '{interfaceType} not found", nameof(interfaceType));
            }
        }

        private async Task SetStrategy<T>(T strategy, Func<IVersionManager, T, Task> func)
        {
            await func(versionStore, strategy);
            var silos = GetSiloAddresses(null);
            var actionPromises = new List<Task>();
            foreach (var s in silos)
            {
                actionPromises.Add(func(GetSiloControlReference(s), strategy));
            }
            var task = await Task.WhenAll(actionPromises).SuppressExceptions();
            // exceptions ignored: silos that failed to set the new strategy will reload it from the storage in the future.
            _ = task.Exception;
        }

        private async Task<object[]> ExecutePerSiloCall(Func<ISiloControl, Task<object>> action, string actionToLog)
        {
            var silos = await GetHosts(true);

            if (logger.IsEnabled(LogLevel.Debug))
            {
                logger.Debug("Executing {0} against {1}", actionToLog, Utils.EnumerableToString(silos.Keys));
            }

            var actionPromises = new List<Task<object>>();
            foreach (SiloAddress siloAddress in silos.Keys.ToArray())
                actionPromises.Add(action(GetSiloControlReference(siloAddress)));

            return await Task.WhenAll(actionPromises);
        }

        private SiloAddress[] GetSiloAddresses(SiloAddress[] silos)
        {
            if (silos != null && silos.Length > 0)
                return silos;

            return this.siloStatusOracle
                       .GetApproximateSiloStatuses(true).Keys.ToArray();
        }

        private ISiloControl GetSiloControlReference(SiloAddress silo)
        {
            return this.internalGrainFactory.GetSystemTarget<ISiloControl>(Constants.SiloControlType, silo);
        }
    }
}
