using System;
using System.IO;
using System.Net;
using System.Runtime.CompilerServices;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Logging;

namespace Orleans.AzureCosmos
{
    internal abstract class AzureCosmosStorage
    {
        protected readonly ILogger logger;
        protected Container container;

        protected AzureCosmosStorage(ILoggerFactory loggerFactory) => logger = loggerFactory.CreateLogger(GetType());

        protected async Task Init(StorageOptionsBase options, ContainerProperties properties)
        {
            try
            {
                var db = options.Connection().GetDatabase(options.DatabaseName);
                var container = db.GetContainer(options.ContainerName);

                bool create;
                var startTime = DateTime.UtcNow;
                using (var res = await container.ReadContainerStreamAsync())
                {
                    CheckAlertSlowAccess(startTime, "ReadContainer");
                    if (!(create = res.StatusCode == HttpStatusCode.NotFound))
                        res.EnsureSuccessStatusCode();
                }

                if (create)
                {
                    properties.Id = options.ContainerName;
                    startTime = DateTime.UtcNow;
                    using var res = await db.CreateContainerStreamAsync(properties);
                    CheckAlertSlowAccess(startTime, "CreateContainer", 5);

                    create = res.StatusCode != HttpStatusCode.Conflict;
                    if (create) res.EnsureSuccessStatusCode();
                }

                logger.LogInformation("{Action} Azure Cosmos container {ContainerName}", create ? "Created" : "Attached to", options.ContainerName);
                this.container = container;
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "Error connecting to Azure Cosmos container {ContainerName}", options.ContainerName);
                throw;
            }
        }

        protected void CheckAlertSlowAccess(DateTime startOperation, string operation, int multiplier = 1)
        {
            var duration = DateTime.UtcNow - startOperation;
            if (duration.Ticks > 3 * TimeSpan.TicksPerSecond * multiplier)
                logger.LogWarning("Slow access to Azure Cosmos container {ContainerName} for {Operation}, which took {Duration}.", container.Id, operation, duration);
        }

        protected bool Log(Exception ex, [CallerMemberName] string memberName = "")
        {
            logger.LogWarning(ex, "{Operation} failed", memberName);
            return false;
        }

        protected static readonly ItemRequestOptions noContentResponse = new() { EnableContentResponseOnWrite = false };

        private static readonly JsonSerializerOptions SerializerOptions = new()
        {
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingDefault,
            Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping
        };

        protected static T Deserialize<T>(ResponseMessage res) => res.Content switch
        {
            MemoryStream ms when ms.TryGetBuffer(out var buf) => JsonSerializer.Deserialize<T>(buf),
            var stream => JsonSerializer.Deserialize<T>(stream),
        };

        protected static MemoryStream Serialize<T>(T value)
        {
            var buf = JsonSerializer.SerializeToUtf8Bytes(value, SerializerOptions);
            return new MemoryStream(buf, 0, buf.Length, false, true);
        }

        protected abstract class RecordBase
        {
            [JsonPropertyName("id")]
            public string Id { get; set; }

            [JsonPropertyName("_etag")]
            public string ETag { get; set; }

            public MemoryStream Serialize() => Serialize<object>(this);
        }
    }
}
