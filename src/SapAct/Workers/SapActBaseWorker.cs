namespace SapAct.Workers;

public abstract class SapActBaseWorker<T>(
    string workerName, 
    ServiceBusTopicConfiguration serviceBusTopicConfiguration, 
    IAzureClientFactory<ServiceBusClient> sbClientFactory, 
    IAzureClientFactory<ServiceBusAdministrationClient> sbAdminClientFactory, 
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
            await managementClient.CreateSubscriptionAsync(topicName, subscriptionName, cancellationToken);
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

        JsonDocument jsonDocument = JsonDocument.Parse(Encoding.UTF8.GetString(message.Body));

		for (int x = 0; x < jsonDocument.RootElement.GetArrayLength(); x++) //TODO: this is temporary, array not expected
        {
            var item = jsonDocument.RootElement[x];

            await IngestMessageAsync(item, cancellationToken);
            await serviceBusReceiver!.CompleteMessageAsync(message, cancellationToken);
        }
    }

    public abstract Task IngestMessageAsync(JsonElement item, CancellationToken cancellationToken);
}
