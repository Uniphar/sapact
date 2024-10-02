namespace SapAct.Workers;

public class ADXWorker(ServiceBusClient sbClient, ServiceBusAdministrationClient managementClient, ADXService adxService, ILogger<ADXWorker> logger, IConfiguration configuration) : SapActBaseWorker<ADXWorker>(sbClient, managementClient, configuration, logger)
{
    public override async Task IngestMessageAsync(JsonElement item, CancellationToken cancellationToken)
    {
        await adxService.IngestMessage(item, cancellationToken);
    }
}
