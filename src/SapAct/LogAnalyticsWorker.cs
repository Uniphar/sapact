namespace SapAct;

public class LogAnalyticsWorker(ServiceBusClient sbClient, ServiceBusAdministrationClient managementClient, LogAnalyticsService logAnalyticsService, ILogger<LogAnalyticsWorker> logger, IConfiguration configuration) : SapActBaseWorker<LogAnalyticsWorker>(sbClient, managementClient, configuration, logger)
{
	public override async Task IngestMessageAsync(JsonElement item, CancellationToken cancellationToken)
	{
		await logAnalyticsService.IngestMessage(item, cancellationToken);
	}
}
