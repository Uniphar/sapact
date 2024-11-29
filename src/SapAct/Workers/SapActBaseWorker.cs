namespace SapAct.Workers;

public abstract class SapActBaseWorker<T>(
    string workerName, 
    ServiceBusTopicConfiguration serviceBusTopicConfiguration, 
    IAzureClientFactory<ServiceBusClient> sbClientFactory, 
    IAzureClientFactory<ServiceBusAdministrationClient> sbAdminClientFactory, 
    TelemetryClient telemetryClient,
    IConfiguration configuration, 
    ILogger<T> logger) 
        : BackgroundService
{
    private ServiceBusReceiver? serviceBusReceiver;

    internal async Task EnsureServiceBusResourcesAsync(string topicName, string subscriptionName, CancellationToken cancellationToken = default)
    {
		var managementClient = sbAdminClientFactory.CreateClient(workerName);

        if (!await managementClient.TopicExistsAsync(topicName, cancellationToken))
		{
            try
            {
                await managementClient.CreateTopicAsync(topicName, cancellationToken);
            }
            catch (ServiceBusException ex) when (ex.Reason == ServiceBusFailureReason.MessagingEntityAlreadyExists)
			{
                //this may happen as topic is shared by worker subscriptions so race condition is possible
				//do nothing, topic already exists
			}
			catch (Exception ex)
			{
				logger.LogError(ex, "Error creating topic");
				throw;
			}
		}

		if (!await managementClient.SubscriptionExistsAsync(topicName, subscriptionName, cancellationToken))
        {
			try
			{
				await managementClient.CreateSubscriptionAsync(topicName, subscriptionName, cancellationToken);
			}
			catch (ServiceBusException ex) when (ex.Reason == ServiceBusFailureReason.MessagingEntityAlreadyExists)
			{
				//do nothing, subscription already exists
			}
			catch (Exception ex)
			{
				logger.LogError(ex, "Error creating subscription");
				throw;
			}
		}
    }

    protected override async Task ExecuteAsync(CancellationToken cancellationToken)
    {
        var topicName = serviceBusTopicConfiguration.TopicName;
        var subscriptionName = configuration.GetTopicSubscriptionNameOrDefault<T>();

        await EnsureServiceBusResourcesAsync(topicName, subscriptionName, cancellationToken);

        var sbClient = sbClientFactory.CreateClient(workerName);

		serviceBusReceiver = sbClient.CreateReceiver(topicName, subscriptionName, new ServiceBusReceiverOptions()
        {
            ReceiveMode = ServiceBusReceiveMode.PeekLock
#if (DEBUG)
            ,
            PrefetchCount = 1
#endif
        });

        try
        {
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

                    logger.LogError(ex, $"Error processing message - {message?.MessageId}");
                }

            }
            while (cancellationToken.IsCancellationRequested == false);
        }
        finally
        {
            telemetryClient.Flush();
		}
    }

    private async Task ProcessMessageAsync(ServiceBusReceivedMessage message, CancellationToken cancellationToken)
    {
        if (message == null || message.Body == null)
            return;

		JsonDocument jsonDocument = JsonDocument.Parse(Encoding.UTF8.GetString(message.Body));

		IEnumerable<JsonElement> items = jsonDocument.RootElement.ValueKind switch
		{
			JsonValueKind.Array => jsonDocument.RootElement.EnumerateArray().ToList(),
			JsonValueKind.Object => [jsonDocument.RootElement],	
			_ => throw new ApplicationException("Unexpected message format")
		};

        int x=0;

		foreach (var item in items)
		{
			await IngestMessageAsync(item, cancellationToken);
			telemetryClient.TrackMetric(GetTelemetryMetric(items.Count()==1 ? message.MessageId: $"{message.MessageId}-{x++}"));
		}

		await serviceBusReceiver!.CompleteMessageAsync(message, cancellationToken);
	}

    private MetricTelemetry GetTelemetryMetric(string messageId)
    { 
        var message = new MetricTelemetry() { Name = "SapActMessageIngestion", Sum = 1 };
        
        message.Properties.Add(Consts.TelemetrySinkTypeDimensionName, typeof(T).Name);
		message.Properties.Add(Consts.TelemetryMessageIdDimensionName, messageId);
		message.Properties.Add(Consts.TelemetryWorkerNameDimensionName, workerName);

		return message;
	}

	public abstract Task IngestMessageAsync(JsonElement item, CancellationToken cancellationToken);
}
