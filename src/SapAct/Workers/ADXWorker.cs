using SapAct.Metrics;

namespace SapAct.Workers;

public class ADXWorker(
    string workerName, 
    ServiceBusTopicConfiguration serviceBusTopicConfiguration, 
    IAzureClientFactory<ServiceBusClient> sbClientFactory, 
    IAzureClientFactory<ServiceBusAdministrationClient> sbAdminClientFactory, 
    ADXService adxService, 
    ILogger<ADXWorker> logger, 
    ICustomEventTelemetryClient telemetryClient,
    SapActMetrics metrics,
    IConfiguration configuration) 
        : SapActBaseWorker<ADXWorker>(workerName, serviceBusTopicConfiguration, sbClientFactory, sbAdminClientFactory, telemetryClient, metrics, configuration, logger)
{
    public override async Task IngestMessageAsync(JsonElement item, CancellationToken cancellationToken)
    {
        await adxService.IngestMessage(item, cancellationToken);
	}
}
