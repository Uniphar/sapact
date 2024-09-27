namespace SapAct;

public abstract class SapActBaseWorker<T>(ServiceBusClient sbClient, ServiceBusAdministrationClient managementClient, IConfiguration configuration, ILogger<T> logger) :  BackgroundService
{
	private ServiceBusReceiver? serviceBusReceiver;

	internal async Task EnsureTopicSubscriptionAsync(string topicName, string subscriptionName, CancellationToken cancellationToken = default)
	{
		if (!await managementClient.SubscriptionExistsAsync(topicName, subscriptionName, cancellationToken))
		{
			await managementClient.CreateSubscriptionAsync(topicName, subscriptionName, cancellationToken);
		}
	}

	protected override async Task ExecuteAsync(CancellationToken cancellationToken)
	{
		var topicName = configuration.GetServiceBusTopicName()!;
		var subscriptionName = configuration.GetTopicSubscriptionNameOrDefault<T>();
		await EnsureTopicSubscriptionAsync(topicName, subscriptionName, cancellationToken);
		serviceBusReceiver = sbClient.CreateReceiver(configuration[Consts.ServiceBusTopicNameConfigKey], subscriptionName, new ServiceBusReceiverOptions()
		{
			ReceiveMode = ServiceBusReceiveMode.PeekLock
#if (DEBUG)
			,
			PrefetchCount = 1
#endif
		});

		do
		{
			ServiceBusReceivedMessage? message = null;
			try
			{
				message = await serviceBusReceiver.ReceiveMessageAsync(cancellationToken: cancellationToken);
				await ProcessMessageAsync(message, cancellationToken);
			}
			catch (Exception ex)
			{
				if (message != null)
				{
					await serviceBusReceiver.AbandonMessageAsync(message, cancellationToken: cancellationToken);
				}

				logger.LogError(ex, "Error processing message");
			}

		}
		while (cancellationToken.IsCancellationRequested == false);
	}

	private async Task ProcessMessageAsync(ServiceBusReceivedMessage message, CancellationToken cancellationToken)
	{
		if (message == null || message.Body == null)
			return;

		try
		{
			JsonDocument jsonDocument = JsonDocument.Parse(Encoding.UTF8.GetString(message.Body));

			for (int x = 0; x < jsonDocument.RootElement.GetArrayLength(); x++) //TODO: this is temporary, array not expected
			{
				var item = jsonDocument.RootElement[x];

				await IngestMessageAsync(item, cancellationToken);
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

	public abstract Task IngestMessageAsync(JsonElement item, CancellationToken cancellationToken);
}
