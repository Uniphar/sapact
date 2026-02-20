namespace SapAct.Workers;

public class SQLWorker(
    string workerName,
    ServiceBusTopicConfiguration serviceBusTopicConfiguration,
    IAzureClientFactory<ServiceBusClient> sbClientFactory,
    IAzureClientFactory<ServiceBusAdministrationClient> sbAdminClientFactory,
    SQLService sqlService,
    ILogger<SQLWorker> logger,
    SapActMetrics metrics,
    IConfiguration configuration
)
    : SapActBaseWorker<SQLWorker>(workerName, serviceBusTopicConfiguration, sbClientFactory, sbAdminClientFactory, metrics, configuration, logger)
{
    public override async Task IngestMessageAsync(string topic, JsonElement item, CancellationToken cancellationToken)
    {
        await sqlService.IngestMessageAsync(item, cancellationToken);
    }
}