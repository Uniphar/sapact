namespace SapAct.Workers;

public class LogAnalyticsWorker(
    string workerName, 
    ServiceBusTopicConfiguration serviceBusTopicConfiguration, 
    IAzureClientFactory<ServiceBusClient> sbClientFactory, 
    IAzureClientFactory<ServiceBusAdministrationClient> sbAdminClientFactory, 
    LogAnalyticsService logAnalyticsService, 
    ILogger<LogAnalyticsWorker> logger, 
    IConfiguration configuration) 
        : SapActBaseWorker<LogAnalyticsWorker>(workerName, serviceBusTopicConfiguration, sbClientFactory, sbAdminClientFactory, configuration, logger)
{
    public override async Task IngestMessageAsync(JsonElement item, CancellationToken cancellationToken)
    {
        await logAnalyticsService.IngestMessage(item, cancellationToken);
    }
}
