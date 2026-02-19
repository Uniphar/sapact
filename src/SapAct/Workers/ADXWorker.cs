using Uniphar.Platform.Telemetry;

namespace SapAct.Workers;

public class ADXWorker(
    string workerName, 
    ServiceBusTopicConfiguration serviceBusTopicConfiguration, 
    IAzureClientFactory<ServiceBusClient> sbClientFactory, 
    IAzureClientFactory<ServiceBusAdministrationClient> sbAdminClientFactory, 
    ADXService adxService, 
    ILogger<ADXWorker> logger, 
    ICustomEventTelemetryClient telemetryClient,
    IConfiguration configuration) 
        : SapActBaseWorker<ADXWorker>(workerName, serviceBusTopicConfiguration, sbClientFactory, sbAdminClientFactory, telemetryClient, configuration, logger)
{
    public override async Task IngestMessageAsync(JsonElement item, CancellationToken cancellationToken)
    {
        await adxService.IngestMessage(item, cancellationToken);
	}
}
