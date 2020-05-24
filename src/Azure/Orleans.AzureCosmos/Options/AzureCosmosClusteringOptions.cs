namespace Orleans.AzureCosmos
{
    /// <summary>
    /// Specify options used for AzureTableBasedMembership
    /// </summary>
    public class AzureCosmosClusteringOptions : AzureStorageOperationOptions
    {
        public override string TableName { get; set; } = DEFAULT_TABLE_NAME;
        public const string DEFAULT_TABLE_NAME = "OrleansSiloInstances";
    }
}
