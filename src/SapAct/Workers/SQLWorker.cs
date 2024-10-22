namespace SapAct.Workers;

public class SQLWorker(
	 string workerName,
	ServiceBusTopicConfiguration serviceBusTopicConfiguration,
	IAzureClientFactory<ServiceBusClient> sbClientFactory,
	IAzureClientFactory<ServiceBusAdministrationClient> sbAdminClientFactory,
	SQLService sqlService,
	ILogger<SQLWorker> logger,
	TelemetryClient telemetryClient,
	IConfiguration configuration)
		: SapActBaseWorker<SQLWorker>(workerName, serviceBusTopicConfiguration, sbClientFactory, sbAdminClientFactory, telemetryClient, configuration, logger)
{
	public override async Task IngestMessageAsync(JsonElement item, CancellationToken cancellationToken)
	{
		await sqlService.IngestMessageAsync(item, cancellationToken);
	}
}
