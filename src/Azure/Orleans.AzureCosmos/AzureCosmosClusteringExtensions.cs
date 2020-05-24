using System;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.AzureCosmos;
using Orleans.AzureUtils;
using Orleans.Configuration;
using Orleans.Messaging;
using Orleans.Runtime.MembershipService;

namespace Orleans.Hosting
{
    public static class AzureCosmosClusteringExtensions
    {
        /// <summary>
        /// Configures the silo to use Azure Cosmos DB for clustering.
        /// </summary>
        /// <param name="builder">
        /// The silo builder.
        /// </param>
        /// <param name="configureOptions">
        /// The configuration delegate.
        /// </param>
        /// <returns>
        /// The provided <see cref="ISiloHostBuilder"/>.
        /// </returns>
        public static ISiloHostBuilder UseAzureCosmosClustering(this ISiloHostBuilder builder, Action<AzureCosmosClusteringOptions> configureOptions)
        {
            return builder.ConfigureServices(services =>
            {
                if (configureOptions != null)
                    services.Configure(configureOptions);

                services.AddSingleton<IMembershipTable, AzureCosmosMembershipTable>().ConfigureFormatter<AzureCosmosClusteringOptions>();
            });
        }

        /// <summary>
        /// Configures the silo to use Azure Cosmos DB for clustering.
        /// </summary>
        /// <param name="builder">
        /// The silo builder.
        /// </param>
        /// <param name="configureOptions">
        /// The configuration delegate.
        /// </param>
        /// <returns>
        /// The provided <see cref="ISiloHostBuilder"/>.
        /// </returns>
        public static ISiloHostBuilder UseAzureCosmosClustering(this ISiloHostBuilder builder, Action<OptionsBuilder<AzureCosmosClusteringOptions>> configureOptions)
        {
            return builder.ConfigureServices(services =>
            {
                configureOptions?.Invoke(services.AddOptions<AzureCosmosClusteringOptions>());
                services.AddSingleton<IMembershipTable, AzureCosmosMembershipTable>().ConfigureFormatter<AzureCosmosClusteringOptions>();
            });
        }

        /// <summary>
        /// Configures the silo to use Azure Cosmos DB for clustering.
        /// </summary>
        /// <param name="builder">
        /// The silo builder.
        /// </param>
        /// <param name="configureOptions">
        /// The configuration delegate.
        /// </param>
        /// <returns>
        /// The provided <see cref="ISiloBuilder"/>.
        /// </returns>
        public static ISiloBuilder UseAzureCosmosClustering(this ISiloBuilder builder, Action<AzureCosmosClusteringOptions> configureOptions)
        {
            return builder.ConfigureServices(services =>
            {
                if (configureOptions != null)
                    services.Configure(configureOptions);

                services.AddSingleton<IMembershipTable, AzureCosmosMembershipTable>().ConfigureFormatter<AzureCosmosClusteringOptions>();
            });
        }

        /// <summary>
        /// Configures the silo to use Azure Cosmos DB for clustering.
        /// </summary>
        /// <param name="builder">
        /// The silo builder.
        /// </param>
        /// <param name="configureOptions">
        /// The configuration delegate.
        /// </param>
        /// <returns>
        /// The provided <see cref="ISiloBuilder"/>.
        /// </returns>
        public static ISiloBuilder UseAzureCosmosClustering(this ISiloBuilder builder, Action<OptionsBuilder<AzureCosmosClusteringOptions>> configureOptions)
        {
            return builder.ConfigureServices(services =>
            {
                configureOptions?.Invoke(services.AddOptions<AzureCosmosClusteringOptions>());
                services.AddSingleton<IMembershipTable, AzureCosmosMembershipTable>().ConfigureFormatter<AzureCosmosClusteringOptions>();
            });
        }

        /// <summary>
        /// Configures the client to use Azure Cosmos DB for clustering.
        /// </summary>
        /// <param name="builder">
        /// The client builder.
        /// </param>
        /// <param name="configureOptions">
        /// The configuration delegate.
        /// </param>
        /// <returns>
        /// The provided <see cref="IClientBuilder"/>.
        /// </returns>
        public static IClientBuilder UseAzureCosmosClustering(this IClientBuilder builder, Action<AzureCosmosGatewayOptions> configureOptions)
        {
            return builder.ConfigureServices(services =>
            {
                if (configureOptions != null)
                    services.Configure(configureOptions);

                services.AddSingleton<IGatewayListProvider, AzureCosmosGatewayListProvider>().ConfigureFormatter<AzureCosmosGatewayOptions>();
            });
        }

        /// <summary>
        /// Configures the client to use Azure Cosmos DB for clustering.
        /// </summary>
        /// <param name="builder">
        /// The client builder.
        /// </param>
        /// <param name="configureOptions">
        /// The configuration delegate.
        /// </param>
        /// <returns>
        /// The provided <see cref="IClientBuilder"/>.
        /// </returns>
        public static IClientBuilder UseAzureCosmosClustering(this IClientBuilder builder, Action<OptionsBuilder<AzureCosmosGatewayOptions>> configureOptions)
        {
            return builder.ConfigureServices(services =>
            {
                configureOptions?.Invoke(services.AddOptions<AzureCosmosGatewayOptions>());
                services.AddSingleton<IGatewayListProvider, AzureCosmosGatewayListProvider>().ConfigureFormatter<AzureCosmosGatewayOptions>();
            });
        }
    }
}
