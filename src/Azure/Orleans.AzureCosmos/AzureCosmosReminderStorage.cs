using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Internal;
using Orleans.Runtime;
using LogLevel = Microsoft.Extensions.Logging.LogLevel;

namespace Orleans.AzureCosmos
{
    internal sealed class AzureCosmosReminderStorage : AzureCosmosStorage, IReminderTable
    {
        private readonly AzureCosmosReminderOptions options;
        private readonly string partitionPrefix;

        public AzureCosmosReminderStorage(
            IOptions<AzureCosmosReminderOptions> options,
            IOptions<ClusterOptions> clusterOptions,
            ILoggerFactory loggerFactory)
            : base(loggerFactory)
        {
            this.options = options.Value;
            this.partitionPrefix = clusterOptions.Value.ServiceId + "/";
        }

        public Task Init()
        {
            logger.LogInformation("Initializing reminders container for service id {ServiceId}", partitionPrefix);
            return Init(options, new()
            {
                PartitionKeyPath = "/" + nameof(ReminderRecord.HashRange),
                IndexingPolicy = new()
                {
                    ExcludedPaths = { new() { Path = "/*" } },
                    IncludedPaths = { new() { Path = "/" + nameof(ReminderRecord.HashRange) + "/?" } }
                }
            });
        }

        public async Task<ReminderEntry> ReadRow(GrainId grainId, string reminderName)
        {
            try
            {
                var pk = GetKeyString(grainId);
                var rowKey = GetRowKey(grainId, reminderName);
                if (logger.IsEnabled(LogLevel.Trace)) logger.LogTrace("Reading: RowKey={RowKey} PK={PK} GrainId={GrainId} from Container={ContainerName}", rowKey, pk, grainId, options.ContainerName);

                await OrleansTaskExtensions.SwitchToThreadPool(); // workaround for https://github.com/Azure/azure-cosmos-dotnet-v2/issues/687
                var startTime = DateTime.UtcNow;
                using var res = await container.ReadItemStreamAsync(rowKey, new PartitionKey(pk));
                CheckAlertSlowAccess(startTime, "ReadItem");

                if (res.StatusCode == HttpStatusCode.NotFound)
                {
                    if (logger.IsEnabled(LogLevel.Trace)) logger.LogTrace("NotFound reading: RowKey={RowKey} PK={PK} from Container={ContainerName}", rowKey, pk, options.ContainerName);
                    return null;
                }

                res.EnsureSuccessStatusCode();
                if (logger.IsEnabled(LogLevel.Trace)) logger.LogTrace("Read: RowKey={RowKey} PK={PK} from Container={ContainerName} with ETag={ETag}", rowKey, pk, options.ContainerName, res.Headers.ETag);
                return AsReminderEntry(Deserialize<ReminderRecord>(res));
            }
            catch (Exception ex) when (Log(ex)) { throw; }
        }

        public Task<ReminderTableData> ReadRows(GrainId grainId)
        {
            try
            {
                var pk = GetKeyString(grainId);
                var rowKey = GetRowKey(grainId, null);
                if (logger.IsEnabled(LogLevel.Trace)) logger.LogTrace("Reading: RowKey={RowKey} PK={PK} GrainId={GrainId} from Container={ContainerName}", rowKey, pk, grainId, options.ContainerName);

                return ReadRows(new QueryDefinition("SELECT * FROM c WHERE STARTSWITH(c.id, @id)")
                    .WithParameter("@id", rowKey),
                    requestOptions: new() { PartitionKey = new(pk) });
            }
            catch (Exception ex) when (Log(ex)) { throw; }
        }

        public Task<ReminderTableData> ReadRows(uint begin, uint end)
        {
            try
            {
                string pkBegin = GetKeyString(begin), pkEnd = GetKeyString(end);
                if (logger.IsEnabled(LogLevel.Trace)) logger.LogTrace("Reading: begin={Begin} end={End} from Container={ContainerName}", pkBegin, pkEnd, options.ContainerName);
                var query = "SELECT * FROM c WHERE c.HashRange>@b AND c.HashRange<=@e";
                var sql = begin < end
                    ? new QueryDefinition(query)
                        .WithParameter("@b", pkBegin).WithParameter("@e", pkEnd)
                    : new QueryDefinition(query + " OR c.HashRange>@bx AND c.HashRange<=@ex")
                        .WithParameter("@b", partitionPrefix).WithParameter("@e", pkEnd)
                        .WithParameter("@bx", pkBegin).WithParameter("@ex", GetKeyString(uint.MaxValue));
                return ReadRows(sql);
            }
            catch (Exception ex) when (Log(ex)) { throw; }
        }

        private async Task<ReminderTableData> ReadRows(QueryDefinition sql, QueryRequestOptions requestOptions = null)
        {
            await OrleansTaskExtensions.SwitchToThreadPool(); // workaround for https://github.com/Azure/azure-cosmos-dotnet-v2/issues/687
            try
            {
                using var query = container.GetItemQueryStreamIterator(sql, null, requestOptions);

                var startTime = DateTime.UtcNow;
                var ls = new List<ReminderRecord>();
                do
                {
                    using var res = await query.ReadNextAsync();
                    res.EnsureSuccessStatusCode();
                    ls.AddRange(Deserialize<QueryResponse>(res).Documents);
                } while (query.HasMoreResults);
                CheckAlertSlowAccess(startTime, "ReadItems");

                var data = new ReminderTableData(ls.Select(AsReminderEntry));
                if (logger.IsEnabled(LogLevel.Trace)) logger.LogTrace("Read reminders table:\n{Data}", data);
                return data;
            }
            catch (Exception ex) when (Log(ex)) { throw; }
        }

        public async Task<string> UpsertRow(ReminderEntry entry)
        {
            try
            {
                var record = AsReminderRecord(entry);
                if (logger.IsEnabled(LogLevel.Trace)) logger.LogTrace("Writing: RowKey={RowKey} PK={PK} GrainId={GrainId} to Container={ContainerName}", record.Id, record.HashRange, entry.GrainId, options.ContainerName);

                var payload = record.Serialize();

                await OrleansTaskExtensions.SwitchToThreadPool(); // workaround for https://github.com/Azure/azure-cosmos-dotnet-v2/issues/687
                var startTime = DateTime.UtcNow;
                using var res = await container.UpsertItemStreamAsync(payload, new PartitionKey(record.HashRange), noContentResponse);
                CheckAlertSlowAccess(startTime, "UpsertItem");

                var eTag = res.EnsureSuccessStatusCode().Headers.ETag;
                if (logger.IsEnabled(LogLevel.Trace)) logger.LogTrace("Wrote: RowKey={RowKey} PK={PK} to Container={ContainerName} with ETag={ETag}", record.Id, record.HashRange, options.ContainerName, eTag);
                return eTag;
            }
            catch (Exception ex) when (Log(ex)) { throw; }
        }

        public async Task<bool> RemoveRow(GrainId grainId, string reminderName, string eTag)
        {
            try
            {
                var pk = GetKeyString(grainId);
                var rowKey = GetRowKey(grainId, reminderName);
                if (logger.IsEnabled(LogLevel.Trace)) logger.LogTrace("Deleting: RowKey={RowKey} PK={PK} GrainId={GrainId} from Container={ContainerName}", rowKey, pk, grainId, options.ContainerName);

                await OrleansTaskExtensions.SwitchToThreadPool(); // workaround for https://github.com/Azure/azure-cosmos-dotnet-v2/issues/687
                var startTime = DateTime.UtcNow;
                using var res = await container.DeleteItemStreamAsync(rowKey, new PartitionKey(pk));
                CheckAlertSlowAccess(startTime, "DeleteItem");

                if (res.StatusCode == HttpStatusCode.NotFound)
                {
                    if (logger.IsEnabled(LogLevel.Trace)) logger.LogTrace("Reminder was not found when deleting: RowKey={RowKey} PK={PK} from Container={ContainerName}", rowKey, pk, options.ContainerName);
                    return true;
                }

                res.EnsureSuccessStatusCode();
                if (logger.IsEnabled(LogLevel.Trace)) logger.LogTrace("Deleted: RowKey={RowKey} PK={PK} from Container={ContainerName}", rowKey, pk, options.ContainerName);
                return true;
            }
            catch (Exception ex) when (Log(ex)) { throw; }
        }

        public Task TestOnlyClearTable() => throw new NotSupportedException();

        private string GetKeyString(GrainId grainId) => GetKeyString(grainId.GetUniformHashCode());
        private string GetKeyString(uint hash) => $"{partitionPrefix}{hash:X8}";

        private static string GetRowKey(GrainId grainId, string reminderName) => $"{grainId}${reminderName}";

        private sealed class ReminderRecord : RecordBase
        {
            public string HashRange { get; set; }

            public DateTime StartAt { get; set; }
            public long Period { get; set; }
        }

        private sealed class QueryResponse
        {
            public ReminderRecord[] Documents { get; set; }
        }

        private ReminderEntry AsReminderEntry(ReminderRecord r)
        {
            var i = r.Id.IndexOf('$');
            var key = r.Id.Substring(0, i);
            var reminderName = r.Id.Substring(i + 1);
            return new()
            {
                GrainId = GrainId.Parse(key),
                ReminderName = reminderName,
                StartAt = r.StartAt,
                Period = new TimeSpan(r.Period),
                ETag = r.ETag,
            };
        }

        private ReminderRecord AsReminderRecord(ReminderEntry r) => new()
        {
            Id = GetRowKey(r.GrainId, r.ReminderName),
            HashRange = GetKeyString(r.GrainId),
            StartAt = r.StartAt,
            Period = r.Period.Ticks,
        };
    }
}
