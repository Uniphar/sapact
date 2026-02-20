namespace SapAct.Workers;

public class LogAnalyticsWorker(
    string workerName,
    ServiceBusTopicConfiguration serviceBusTopicConfiguration,
    IAzureClientFactory<ServiceBusClient> sbClientFactory,
    IAzureClientFactory<ServiceBusAdministrationClient> sbAdminClientFactory,
    LogAnalyticsService logAnalyticsService,
    ILogger<LogAnalyticsWorker> logger,
    SapActMetrics metrics,
    IConfiguration configuration
)
    : SapActBaseWorker<LogAnalyticsWorker>(workerName, serviceBusTopicConfiguration, sbClientFactory, sbAdminClientFactory, metrics, configuration, logger)
{
    public override async Task IngestMessageAsync(string topic, JsonElement item, CancellationToken cancellationToken)
    {
        await logAnalyticsService.IngestMessage(topic, item, cancellationToken);
    }
}