using System;
using System.Collections.Generic;
using System.Globalization;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.AzureCosmos;
using Orleans.AzureUtils;
using Orleans.Configuration;
using LogLevel = Microsoft.Extensions.Logging.LogLevel;

namespace Orleans.Runtime.MembershipService
{
    internal class AzureCosmosMembershipTable : IMembershipTable
    {
        private readonly ILogger logger;
        private readonly AzureCosmosClusteringOptions options;
        private readonly string deploymentId;
        private readonly PartitionKey partitionKey;
        private readonly string databaseId;
        private readonly string collectionId;
        private readonly DocumentClient client;

        public AzureCosmosMembershipTable(ILoggerFactory loggerFactory, IOptions<AzureCosmosClusteringOptions> clusteringOptions, IOptions<ClusterOptions> clusterOptions)
        {
            this.logger = loggerFactory.CreateLogger<AzureCosmosMembershipTable>();
            this.options = clusteringOptions.Value;
            this.deploymentId = clusterOptions.Value.ClusterId;
            this.partitionKey = new PartitionKey(deploymentId);

            //TableName = options.TableName;
            //storage = new AzureTableDataManager<SiloInstanceTableEntry>(
            //    options.TableName,
            //    options.ConnectionString,
            //    loggerFactory.CreateLogger<AzureTableDataManager<SiloInstanceTableEntry>>(),
            //    options.StoragePolicyOptions);
            //this.storagePolicyOptions = options.StoragePolicyOptions;
        }

        public async Task InitializeMembershipTable(bool tryInitTableVersion)
        {
            try
            {
                var startTime = DateTime.UtcNow;
                try
                {
                    var didCreate = false;
                    try
                    {
                        await client.CreateDocumentCollectionAsync(UriFactory.CreateDatabaseUri(databaseId), new DocumentCollection
                        {
                            Id = collectionId,
                            PartitionKey = new PartitionKeyDefinition
                            {
                                Paths = new System.Collections.ObjectModel.Collection<string> { "/DeploymentId" },
                            }
                        });
                        didCreate = true;
                    }
                    catch (DocumentClientException ex) when (ex.StatusCode == HttpStatusCode.Conflict) { }

                    logger.Info((int)Utilities.ErrorCode.AzureTable_01, "{0} Azure Cosmos collection {1}", didCreate ? "Created" : "Attached to", collectionId);
                }
                finally
                {
                    CheckAlertSlowAccess(startTime);
                }
            }
            catch (TimeoutException te)
            {
                var errorMsg = $"Unable to create or connect to the Azure table in {this.StoragePolicyOptions.CreationTimeout}";
                logger.Error((int)Utilities.ErrorCode.AzureTable_TableNotCreated, errorMsg, te);
                throw new OrleansException(errorMsg, te);
            }
            catch (Exception ex)
            {
                string errorMsg = string.Format("Exception trying to create or connect to the Azure table: {0}", ex.Message);
                logger.Error((int)TableStorageErrorCode.AzureTable_33, errorMsg, ex);
                throw new OrleansException(errorMsg, ex);
            }

            // even if I am not the one who created the table,
            // try to insert an initial table version if it is not already there,
            // so we always have a first table version row, before this silo starts working.
            if (tryInitTableVersion)
                await TryCreateTableVersionEntryAsync();
        }

        private async Task TryCreateTableVersionEntryAsync()
        {
            var entry = CreateTableVersionEntry(0);
            var startTime = DateTime.UtcNow;
            if (logger.IsEnabled(LogLevel.Debug)) logger.Trace("{0} table {1} partitionKey {2} rowKey = {3}", operation, collectionId, deploymentId, entry.Id);
            try
            {
                try
                {
                    var url = UriFactory.CreateDocumentCollectionUri(databaseId, collectionId);
                    await client.CreateDocumentAsync(url, entry, new RequestOptions { PartitionKey = partitionKey }, disableAutomaticIdGeneration: true);
                    logger.Info("Created new table version row.");
                }
                catch (DocumentClientException ex) when (ex.StatusCode == HttpStatusCode.Conflict) { }
            }
            finally
            {
                CheckAlertSlowAccess(startTime);
            }
        }

        public Task DeleteMembershipTableEntries(string clusterId)
        {
            if (clusterId == null) throw new ArgumentNullException(nameof(clusterId));

            var entries = await storage.ReadAllTableEntriesForPartitionAsync(clusterId);
            var entriesList = new List<Tuple<SiloInstanceTableEntry, string>>(entries);

            await DeleteEntriesBatch(entriesList);
        }

        public async Task CleanupDefunctSiloEntries(DateTimeOffset beforeDate)
        {
            var entriesList = (await FindAllSiloEntries())
                .Where(entry => entry.Item1.Status == INSTANCE_STATUS_DEAD && entry.Item1.Timestamp < beforeDate)
                .ToList();

            await DeleteEntriesBatch(entriesList);
        }

        private async Task DeleteEntriesBatch(List<Tuple<SiloInstanceTableEntry, string>> entriesList)
        {
            if (entriesList.Count <= this.storagePolicyOptions.MaxBulkUpdateRows)
            {
                await storage.DeleteTableEntriesAsync(entriesList);
            }
            else
            {
                var tasks = new List<Task>();
                foreach (var batch in entriesList.BatchIEnumerable(this.storagePolicyOptions.MaxBulkUpdateRows))
                {
                    tasks.Add(storage.DeleteTableEntriesAsync(batch));
                }
                await Task.WhenAll(tasks);
            }
        }

        public async Task<MembershipTableData> ReadRow(SiloAddress key)
        {
            try
            {
                string rowKey = SiloInstanceTableEntry.ConstructRowKey(key);

                string filterOnPartitionKey = TableQuery.GenerateFilterCondition(nameof(SiloInstanceTableEntry.PartitionKey), QueryComparisons.Equal,
                        this.deploymentId);
                string filterOnRowKey1 = TableQuery.GenerateFilterCondition(nameof(SiloInstanceTableEntry.RowKey), QueryComparisons.Equal,
                    rowKey);
                string filterOnRowKey2 = TableQuery.GenerateFilterCondition(nameof(SiloInstanceTableEntry.RowKey), QueryComparisons.Equal, SiloInstanceTableEntry.TABLE_VERSION_ROW);
                string query = TableQuery.CombineFilters(filterOnPartitionKey, TableOperators.And, TableQuery.CombineFilters(filterOnRowKey1, TableOperators.Or, filterOnRowKey2));

                var queryResults = await storage.ReadTableEntriesAndEtagsAsync(query);

                var asList = queryResults.ToList();
                if (asList.Count < 1 || asList.Count > 2)
                    throw new KeyNotFoundException(string.Format("Could not find table version row or found too many entries. Was looking for key {0}, found = {1}", key.ToLongString(), Utils.EnumerableToString(asList)));

                int numTableVersionRows = asList.Count(tuple => tuple.Item1.RowKey == SiloInstanceTableEntry.TABLE_VERSION_ROW);
                if (numTableVersionRows < 1)
                    throw new KeyNotFoundException(string.Format("Did not read table version row. Read = {0}", Utils.EnumerableToString(asList)));

                if (numTableVersionRows > 1)
                    throw new KeyNotFoundException(string.Format("Read {0} table version rows, while was expecting only 1. Read = {1}", numTableVersionRows, Utils.EnumerableToString(asList)));

                var entries = asList;

                MembershipTableData data = Convert(entries);
                if (logger.IsEnabled(LogLevel.Debug)) logger.Debug("Read my entry {0} Table=" + Environment.NewLine + "{1}", key.ToLongString(), data.ToString());
                return data;
            }
            catch (Exception exc)
            {
                logger.Warn((int)TableStorageErrorCode.AzureTable_20,
                    $"Intermediate error reading silo entry for key {key.ToLongString()} from the table {collectionId}.", exc);
                throw;
            }
        }

        public async Task<MembershipTableData> ReadAll()
        {
            try
            {
                var queryResults = await storage.ReadAllTableEntriesForPartitionAsync(this.deploymentId);

                var asList = queryResults.ToList();
                if (asList.Count < 1)
                    throw new KeyNotFoundException(string.Format("Could not find enough rows in the FindAllSiloEntries call. Found = {0}", Utils.EnumerableToString(asList)));

                int numTableVersionRows = asList.Count(tuple => tuple.Item1.RowKey == SiloInstanceTableEntry.TABLE_VERSION_ROW);
                if (numTableVersionRows < 1)
                    throw new KeyNotFoundException(string.Format("Did not find table version row. Read = {0}", Utils.EnumerableToString(asList)));
                if (numTableVersionRows > 1)
                    throw new KeyNotFoundException(string.Format("Read {0} table version rows, while was expecting only 1. Read = {1}", numTableVersionRows, Utils.EnumerableToString(asList)));

                var entries = asList;
                MembershipTableData data = Convert(entries);
                if (logger.IsEnabled(LogLevel.Trace)) logger.Trace("ReadAll Table=" + Environment.NewLine + "{0}", data.ToString());

                return data;
            }
            catch (Exception exc)
            {
                logger.Warn((int)TableStorageErrorCode.AzureTable_21,
                    $"Intermediate error reading all silo entries {collectionId}.", exc);
                throw;
            }
        }

        private SiloInstanceTableEntry CreateTableVersionEntry(int tableVersion)
        {
            return new SiloInstanceTableEntry
            {
                DeploymentId = deploymentId,
                PartitionKey = deploymentId,
                RowKey = SiloInstanceTableEntry.TABLE_VERSION_ROW,
                MembershipVersion = tableVersion.ToString(CultureInfo.InvariantCulture)
            };
        }

        public async Task<bool> InsertRow(MembershipEntry entry, TableVersion tableVersion)
        {
            try
            {
                if (logger.IsEnabled(LogLevel.Debug)) logger.Debug("InsertRow entry = {0}, table version = {1}", entry.ToFullString(), tableVersion);
                var tableEntry = Convert(entry, deploymentId);
                var versionEntry = CreateTableVersionEntry(tableVersion.Version);

                try
                {
                    await storage.InsertTwoTableEntriesConditionallyAsync(tableEntry, versionEntry, tableVersion.VersionEtag);
                    return true;
                }
                catch (Exception exc)
                {
                    if (!AzureTableUtils.EvaluateException(exc, out var httpStatusCode, out var restStatus)) throw;

                    if (logger.IsEnabled(LogLevel.Trace)) logger.Trace("InsertSiloEntryConditionally failed with httpStatusCode={0}, restStatus={1}", httpStatusCode, restStatus);
                    if (!(AzureTableUtils.IsContentionError(httpStatusCode))) throw;
                }

                logger.Warn((int)TableStorageErrorCode.AzureTable_22,
                    $"Insert failed due to contention on the table. Will retry. Entry {entry.ToFullString()}, table version = {tableVersion}");
                return false;
            }
            catch (Exception exc)
            {
                logger.Warn((int)TableStorageErrorCode.AzureTable_23,
                    $"Intermediate error inserting entry {entry.ToFullString()} tableVersion {(tableVersion == null ? "null" : tableVersion.ToString())} to the table {collectionId}.", exc);
                throw;
            }
        }

        public async Task<bool> UpdateRow(MembershipEntry entry, string etag, TableVersion tableVersion)
        {
            try
            {
                if (logger.IsEnabled(LogLevel.Debug)) logger.Debug("UpdateRow entry = {0}, etag = {1}, table version = {2}", entry.ToFullString(), etag, tableVersion);
                var siloEntry = Convert(entry, deploymentId);
                var versionEntry = CreateTableVersionEntry(tableVersion.Version);

                try
                {
                    await storage.UpdateTwoTableEntriesConditionallyAsync(siloEntry, etag, versionEntry, tableVersion.VersionEtag);
                    return true;
                }
                catch (Exception exc)
                {
                    if (!AzureTableUtils.EvaluateException(exc, out var httpStatusCode, out var restStatus)) throw;

                    if (logger.IsEnabled(LogLevel.Trace)) logger.Trace("UpdateSiloEntryConditionally failed with httpStatusCode={0}, restStatus={1}", httpStatusCode, restStatus);
                    if (!AzureTableUtils.IsContentionError(httpStatusCode)) throw;
                }
                logger.Warn((int)TableStorageErrorCode.AzureTable_24,
                    $"Update failed due to contention on the table. Will retry. Entry {entry.ToFullString()}, eTag {etag}, table version = {tableVersion} ");
                return false;
            }
            catch (Exception exc)
            {
                logger.Warn((int)TableStorageErrorCode.AzureTable_25,
                    $"Intermediate error updating entry {entry.ToFullString()} tableVersion {(tableVersion == null ? "null" : tableVersion.ToString())} to the table {collectionId}.", exc);
                throw;
            }
        }

        public async Task UpdateIAmAlive(MembershipEntry entry)
        {
            try
            {
                if (logger.IsEnabled(LogLevel.Debug)) logger.Debug("Merge entry = {0}", entry.ToFullString());
                var siloEntry = ConvertPartial(entry, deploymentId);
                await storage.MergeTableEntryAsync(siloEntry, AzureTableUtils.ANY_ETAG); // we merge this without checking eTags.
            }
            catch (Exception exc)
            {
                logger.Warn((int)TableStorageErrorCode.AzureTable_26,
                    $"Intermediate error updating IAmAlive field for entry {entry.ToFullString()} to the table {collectionId}.", exc);
                throw;
            }
        }

        private MembershipTableData Convert(List<Tuple<SiloInstanceTableEntry, string>> entries)
        {
            try
            {
                var memEntries = new List<Tuple<MembershipEntry, string>>();
                TableVersion tableVersion = null;
                foreach (var tuple in entries)
                {
                    var tableEntry = tuple.Item1;
                    if (tableEntry.RowKey.Equals(SiloInstanceTableEntry.TABLE_VERSION_ROW))
                    {
                        tableVersion = new TableVersion(Int32.Parse(tableEntry.MembershipVersion), tuple.Item2);
                    }
                    else
                    {
                        try
                        {

                            MembershipEntry membershipEntry = Parse(tableEntry);
                            memEntries.Add(new Tuple<MembershipEntry, string>(membershipEntry, tuple.Item2));
                        }
                        catch (Exception exc)
                        {
                            logger.Error((int)TableStorageErrorCode.AzureTable_61,
                                $"Intermediate error parsing SiloInstanceTableEntry to MembershipTableData: {tableEntry}. Ignoring this entry.", exc);
                        }
                    }
                }
                var data = new MembershipTableData(memEntries, tableVersion);
                return data;
            }
            catch (Exception exc)
            {
                logger.Error((int)TableStorageErrorCode.AzureTable_60,
                    $"Intermediate error parsing SiloInstanceTableEntry to MembershipTableData: {Utils.EnumerableToString(entries, tuple => tuple.Item1.ToString())}.", exc);
                throw;
            }
        }

        private static MembershipEntry Parse(SiloInstanceTableEntry tableEntry)
        {
            var parse = new MembershipEntry
            {
                HostName = tableEntry.HostName,
                Status = (SiloStatus)Enum.Parse(typeof(SiloStatus), tableEntry.Status)
            };

            if (!string.IsNullOrEmpty(tableEntry.ProxyPort))
                parse.ProxyPort = int.Parse(tableEntry.ProxyPort);

            int port = 0;
            if (!string.IsNullOrEmpty(tableEntry.Port))
                int.TryParse(tableEntry.Port, out port);

            int gen = 0;
            if (!string.IsNullOrEmpty(tableEntry.Generation))
                int.TryParse(tableEntry.Generation, out gen);

            parse.SiloAddress = SiloAddress.New(new IPEndPoint(IPAddress.Parse(tableEntry.Address), port), gen);

            parse.RoleName = tableEntry.RoleName;
            if (!string.IsNullOrEmpty(tableEntry.SiloName))
            {
                parse.SiloName = tableEntry.SiloName;
            }
            else if (!string.IsNullOrEmpty(tableEntry.InstanceName))
            {
                // this is for backward compatability: in a mixed cluster of old and new version,
                // some entries will have the old InstanceName column.
                parse.SiloName = tableEntry.InstanceName;
            }
            if (!string.IsNullOrEmpty(tableEntry.UpdateZone))
                parse.UpdateZone = int.Parse(tableEntry.UpdateZone);

            if (!string.IsNullOrEmpty(tableEntry.FaultZone))
                parse.FaultZone = int.Parse(tableEntry.FaultZone);

            parse.StartTime = !string.IsNullOrEmpty(tableEntry.StartTime) ?
                LogFormatter.ParseDate(tableEntry.StartTime) : default(DateTime);

            parse.IAmAliveTime = !string.IsNullOrEmpty(tableEntry.IAmAliveTime) ?
                LogFormatter.ParseDate(tableEntry.IAmAliveTime) : default(DateTime);

            var suspectingSilos = new List<SiloAddress>();
            var suspectingTimes = new List<DateTime>();

            if (!string.IsNullOrEmpty(tableEntry.SuspectingSilos))
            {
                string[] silos = tableEntry.SuspectingSilos.Split('|');
                foreach (string silo in silos)
                {
                    suspectingSilos.Add(SiloAddress.FromParsableString(silo));
                }
            }

            if (!string.IsNullOrEmpty(tableEntry.SuspectingTimes))
            {
                string[] times = tableEntry.SuspectingTimes.Split('|');
                foreach (string time in times)
                    suspectingTimes.Add(LogFormatter.ParseDate(time));
            }

            if (suspectingSilos.Count != suspectingTimes.Count)
                throw new OrleansException(String.Format("SuspectingSilos.Length of {0} as read from Azure table is not equal to SuspectingTimes.Length of {1}", suspectingSilos.Count, suspectingTimes.Count));

            for (int i = 0; i < suspectingSilos.Count; i++)
                parse.AddSuspector(suspectingSilos[i], suspectingTimes[i]);

            return parse;
        }

        private static SiloInstanceTableEntry Convert(MembershipEntry memEntry, string deploymentId)
        {
            var tableEntry = new SiloInstanceTableEntry
            {
                DeploymentId = deploymentId,
                Address = memEntry.SiloAddress.Endpoint.Address.ToString(),
                Port = memEntry.SiloAddress.Endpoint.Port.ToString(CultureInfo.InvariantCulture),
                Generation = memEntry.SiloAddress.Generation.ToString(CultureInfo.InvariantCulture),
                HostName = memEntry.HostName,
                Status = memEntry.Status.ToString(),
                ProxyPort = memEntry.ProxyPort.ToString(CultureInfo.InvariantCulture),
                RoleName = memEntry.RoleName,
                SiloName = memEntry.SiloName,
                // this is for backward compatability: in a mixed cluster of old and new version,
                // we need to populate both columns.
                InstanceName = memEntry.SiloName,
                UpdateZone = memEntry.UpdateZone.ToString(CultureInfo.InvariantCulture),
                FaultZone = memEntry.FaultZone.ToString(CultureInfo.InvariantCulture),
                StartTime = LogFormatter.PrintDate(memEntry.StartTime),
                IAmAliveTime = LogFormatter.PrintDate(memEntry.IAmAliveTime)
            };

            if (memEntry.SuspectTimes != null)
            {
                var siloList = new StringBuilder();
                var timeList = new StringBuilder();
                bool first = true;
                foreach (var tuple in memEntry.SuspectTimes)
                {
                    if (!first)
                    {
                        siloList.Append('|');
                        timeList.Append('|');
                    }
                    siloList.Append(tuple.Item1.ToParsableString());
                    timeList.Append(LogFormatter.PrintDate(tuple.Item2));
                    first = false;
                }

                tableEntry.SuspectingSilos = siloList.ToString();
                tableEntry.SuspectingTimes = timeList.ToString();
            }
            else
            {
                tableEntry.SuspectingSilos = String.Empty;
                tableEntry.SuspectingTimes = String.Empty;
            }
            tableEntry.PartitionKey = deploymentId;
            tableEntry.RowKey = SiloInstanceTableEntry.ConstructRowKey(memEntry.SiloAddress);

            return tableEntry;
        }

        private static SiloInstanceTableEntry ConvertPartial(MembershipEntry memEntry, string deploymentId)
        {
            return new SiloInstanceTableEntry
            {
                DeploymentId = deploymentId,
                IAmAliveTime = LogFormatter.PrintDate(memEntry.IAmAliveTime),
                PartitionKey = deploymentId,
                RowKey = SiloInstanceTableEntry.ConstructRowKey(memEntry.SiloAddress)
            };
        }

        private void CheckAlertSlowAccess(DateTime startOperation, [System.Runtime.CompilerServices.CallerMemberName]string operation = "")
        {
            var timeSpan = DateTime.UtcNow - startOperation;
            if (timeSpan > this.StoragePolicyOptions.OperationTimeout)
            {
                logger.Warn((int)Utilities.ErrorCode.AzureTable_15, "Slow access to Azure Table {0} for {1}, which took {2}.", collectionId, operation, timeSpan);
            }
        }
    }
}
