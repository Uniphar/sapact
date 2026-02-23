namespace SapAct.Workers;

public class ADXWorker(
    string workerName,
    ServiceBusTopicConfiguration serviceBusTopicConfiguration,
    IAzureClientFactory<ServiceBusClient> sbClientFactory,
    IAzureClientFactory<ServiceBusAdministrationClient> sbAdminClientFactory,
    ADXService adxService,
    ILogger<ADXWorker> logger,
    SapActMetrics metrics,
    IConfiguration configuration
)
    : SapActBaseWorker<ADXWorker>(workerName, serviceBusTopicConfiguration, sbClientFactory, sbAdminClientFactory, metrics, configuration, logger)
{
    public override async Task IngestMessageAsync(string topic, JsonElement item, CancellationToken cancellationToken)
    {
        await adxService.IngestMessage(topic, item, cancellationToken);
    }
}