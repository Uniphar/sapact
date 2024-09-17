namespace SapAct;

public class ADXWorker(ServiceBusClient sbClient, ADXService adxService, ILogger<ADXWorker> logger) : BackgroundService
{
	private ServiceBusReceiver? serviceBusReceiver;

	protected override async Task ExecuteAsync(CancellationToken stoppingToken)
	{
		serviceBusReceiver = sbClient.CreateReceiver("sap-events", "SDV", new ServiceBusReceiverOptions() //TODO: make topic subscription name configurable
		{
			ReceiveMode = ServiceBusReceiveMode.PeekLock
#if (DEBUG)			
			,PrefetchCount = 1
#endif
		});
		
		do
		{
			ServiceBusReceivedMessage? message=null;
			try
			{
				message = await serviceBusReceiver.ReceiveMessageAsync(cancellationToken: stoppingToken);				
				await ProcessMessageAsync(message, stoppingToken);				
			}
			catch (Exception ex)
			{
				if (message!=null)
				{
					await serviceBusReceiver.AbandonMessageAsync(message, cancellationToken: stoppingToken);
				}

				logger.LogError(ex, "Error processing message");
			}

		}
		while (stoppingToken.IsCancellationRequested == false);		
	}

	private async Task ProcessMessageAsync(ServiceBusReceivedMessage message, CancellationToken cancellationToken)
	{
		try
		{
			JsonDocument jsonDocument = JsonDocument.Parse(Encoding.UTF8.GetString(message.Body));

			for (int x = 0; x < jsonDocument.RootElement.GetArrayLength(); x++) //TODO: this is temporary, array not expected
			{
				var item = jsonDocument.RootElement[x];

				await adxService.IngestMessage(item, cancellationToken);
				await serviceBusReceiver!.CompleteMessageAsync(message, cancellationToken);
			}
		}
		catch (Exception ex)
		{
			if (message != null)
			{
				await serviceBusReceiver!.AbandonMessageAsync(message, cancellationToken: cancellationToken);
			}

			logger.LogError(ex, "Error processing message");
		}
	}
}
