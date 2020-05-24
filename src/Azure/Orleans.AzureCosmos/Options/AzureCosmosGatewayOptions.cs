namespace Orleans.AzureCosmos
{
    public class AzureCosmosGatewayOptions : AzureStorageOperationOptions
    {
        public override string TableName { get; set; } = AzureStorageClusteringOptions.DEFAULT_TABLE_NAME;
    }
}