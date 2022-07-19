using System;
using System.Collections.Generic;
using System.Net;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Configuration.Overrides;
using Orleans.GrainDirectory;
using Orleans.Internal;
using Orleans.Runtime;
using LogLevel = Microsoft.Extensions.Logging.LogLevel;

namespace Orleans.AzureCosmos
{
    internal sealed class AzureCosmosGrainDirectory : AzureCosmosStorage, IGrainDirectory, ILifecycleParticipant<ISiloLifecycle>, ILifecycleObserver
    {
        private readonly AzureCosmosGrainDirectoryOptions options;
        private readonly string name;
        private readonly string clusterId;
        private readonly PartitionKey partitionKey;

        public static IGrainDirectory Create(IServiceProvider sp, string name)
            => ActivatorUtilities.CreateInstance<AzureCosmosGrainDirectory>(sp, name, sp.GetProviderClusterOptions(name));

        public AzureCosmosGrainDirectory(
            string name,
            IOptionsMonitor<AzureCosmosGrainDirectoryOptions> optionsSnapshot,
            IOptions<ClusterOptions> clusterOptions,
            ILoggerFactory loggerFactory)
            : base(loggerFactory)
        {
            this.name = name;
            this.options = optionsSnapshot.Get(name);
            this.clusterId = clusterOptions.Value.ClusterId;
            this.partitionKey = new(clusterId);
        }

        public void Participate(ISiloLifecycle lifecycle) => lifecycle.Subscribe(OptionFormattingUtilities.Name<AzureCosmosGrainDirectory>(name), ServiceLifecycleStage.RuntimeInitialize, this);

        public Task OnStart(CancellationToken ct) => Task.Run(() =>
        {
            logger.LogInformation("Initializing {Name} grain directory container for cluster {ClusterId}", name, clusterId);
            return Init(options, new()
            {
                PartitionKeyPath = "/" + nameof(GrainRecord.Cluster),
                IndexingPolicy = new() { ExcludedPaths = { new() { Path = "/*" } } }
            });
        });

        public Task OnStop(CancellationToken ct) => Task.CompletedTask;

        public async Task<GrainAddress> Lookup(GrainId grainId)
        {
            try
            {
                if (logger.IsEnabled(LogLevel.Trace)) logger.LogTrace("Reading: GrainId={GrainId} PK={ClusterId} from Container={ContainerName}", grainId, clusterId, options.ContainerName);

                await OrleansTaskExtensions.SwitchToThreadPool(); // workaround for https://github.com/Azure/azure-cosmos-dotnet-v2/issues/687
                var startTime = DateTime.UtcNow;
                using var res = await container.ReadItemStreamAsync(grainId.ToString(), partitionKey);
                CheckAlertSlowAccess(startTime, "ReadItem");

                if (res.StatusCode == HttpStatusCode.NotFound)
                {
                    if (logger.IsEnabled(LogLevel.Trace)) logger.LogTrace("NotFound reading: GrainId={GrainId} PK={ClusterId} from Container={ContainerName}", grainId, clusterId, options.ContainerName);
                    return null;
                }

                res.EnsureSuccessStatusCode();
                if (logger.IsEnabled(LogLevel.Trace)) logger.LogTrace("Read: GrainId={GrainId} PK={ClusterId} from Container={ContainerName}", grainId, clusterId, options.ContainerName);
                return Deserialize<GrainRecord>(res).ToGrainAddress();
            }
            catch (Exception ex) when (Log(ex)) { throw; }
        }

        public async Task<GrainAddress> Register(GrainAddress address)
        {
            try
            {
                var record = AsGrainRecord(address);
                if (logger.IsEnabled(LogLevel.Trace)) logger.LogTrace("Writing: GrainId={GrainId} PK={ClusterId} to Container={ContainerName}", record.Id, record.Cluster, options.ContainerName);

                var payload = record.Serialize();

                await OrleansTaskExtensions.SwitchToThreadPool(); // workaround for https://github.com/Azure/azure-cosmos-dotnet-v2/issues/687
                var startTime = DateTime.UtcNow;
                using (var res = await container.CreateItemStreamAsync(payload, partitionKey, noContentResponse))
                {
                    CheckAlertSlowAccess(startTime, "CreateItem");
                    if (res.StatusCode != HttpStatusCode.Conflict)
                    {
                        res.EnsureSuccessStatusCode();
                        if (logger.IsEnabled(LogLevel.Trace)) logger.LogTrace("Wrote: GrainId={GrainId} PK={ClusterId} to Container={ContainerName}", record.Id, record.Cluster, options.ContainerName);
                        return address;
                    }
                }
                logger.LogInformation("Conflict writing: GrainId={GrainId} PK={ClusterId} to Container={ContainerName}", record.Id, record.Cluster, options.ContainerName);
                return await Lookup(record.Id);
            }
            catch (Exception ex) when (Log(ex)) { throw; }
        }

        public async Task Unregister(GrainAddress address)
        {
            try
            {
                var id = address.GrainId.ToString();
                if (logger.IsEnabled(LogLevel.Trace)) logger.LogTrace("Reading: GrainId={GrainId} PK={ClusterId} from Container={ContainerName}", id, clusterId, options.ContainerName);
                GrainRecord record;

                await OrleansTaskExtensions.SwitchToThreadPool(); // workaround for https://github.com/Azure/azure-cosmos-dotnet-v2/issues/687
                var startTime = DateTime.UtcNow;
                using (var res = await container.ReadItemStreamAsync(id, partitionKey))
                {
                    CheckAlertSlowAccess(startTime, "ReadItem");

                    if (res.StatusCode == HttpStatusCode.NotFound)
                    {
                        if (logger.IsEnabled(LogLevel.Trace)) logger.LogTrace("NotFound reading: GrainId={GrainId} PK={ClusterId} from Container={ContainerName}", id, clusterId, options.ContainerName);
                        return;
                    }

                    res.EnsureSuccessStatusCode();
                    if (logger.IsEnabled(LogLevel.Trace)) logger.LogTrace("Read: GrainId={GrainId} PK={ClusterId} from Container={ContainerName} with ETag={ETag}", id, clusterId, options.ContainerName, res.Headers.ETag);
                    record = Deserialize<GrainRecord>(res);
                }

                if (record.ActivationId != address.ActivationId)
                {
                    logger.LogInformation("Will not delete: GrainId={GrainId} PK={ClusterId} from Container={ContainerName} with ActivationId={ActivationId} Expected={ExpectedActivationId}", id, clusterId, options.ContainerName, record.ActivationId, address.ActivationId);
                    return;
                }

                if (logger.IsEnabled(LogLevel.Trace)) logger.LogTrace("Deleting: GrainId={GrainId} PK={ClusterId} from Container={ContainerName}", id, clusterId, options.ContainerName);

                startTime = DateTime.UtcNow;
                using (var res = await container.DeleteItemStreamAsync(id, partitionKey, new() { IfMatchEtag = record.ETag }))
                {
                    CheckAlertSlowAccess(startTime, "DeleteItem");

                    if (res.StatusCode is HttpStatusCode.NotFound or HttpStatusCode.PreconditionFailed)
                    {
                        logger.LogInformation("{StatusCode} deleting: GrainId={GrainId} PK={ClusterId} from Container={ContainerName} with ETag={ETag}", res.StatusCode, id, clusterId, options.ContainerName, record.ETag);
                        return;
                    }
                    res.EnsureSuccessStatusCode();
                    if (logger.IsEnabled(LogLevel.Trace)) logger.LogTrace("Deleted: GrainId={GrainId} PK={ClusterId} from Container={ContainerName}", id, clusterId, options.ContainerName);
                }
            }
            catch (Exception ex) when (Log(ex)) { throw; }
        }

        public Task UnregisterSilos(List<SiloAddress> siloAddresses) => Task.CompletedTask;

        private sealed class GrainRecord : RecordBase
        {
            [JsonPropertyName("id")]
            public new GrainId Id { get; set; }

            public string Cluster { get; set; }

            public SiloAddress SiloAddress { get; set; }
            public ActivationId ActivationId { get; set; }
            public MembershipVersion MembershipVersion { get; set; }

            public GrainAddress ToGrainAddress() => new() { GrainId = Id, SiloAddress = SiloAddress, ActivationId = ActivationId, MembershipVersion = MembershipVersion };
        }

        private GrainRecord AsGrainRecord(GrainAddress r) => new()
        {
            Id = r.GrainId,
            Cluster = clusterId,
            SiloAddress = r.SiloAddress,
            ActivationId = r.ActivationId,
            MembershipVersion = r.MembershipVersion
        };
    }
}
