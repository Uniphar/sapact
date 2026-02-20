namespace SapAct.Workers;

public abstract class SapActBaseWorker<T>(
    string workerName,
    ServiceBusTopicConfiguration serviceBusTopicConfiguration,
    IAzureClientFactory<ServiceBusClient> sbClientFactory,
    IAzureClientFactory<ServiceBusAdministrationClient> sbAdminClientFactory,
    SapActMetrics metrics,
    IConfiguration configuration,
    ILogger<T> logger
)
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
                // Subscription already exists - this is expected in concurrent scenarios where multiple workers may attempt creation.
                // It is safe to ignore this exception because the desired resource is present.
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Error creating subscription {SubscriptionName} for topic {TopicName}", subscriptionName, topicName);
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

        serviceBusReceiver = sbClient.CreateReceiver(topicName,
            subscriptionName,
            new()
            {
                ReceiveMode = ServiceBusReceiveMode.PeekLock
#if DEBUG
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

                if (message == null) continue;

                await ProcessMessageAsync(message, cancellationToken);
            }
            catch (Exception ex)
            {
                if (message != null) await serviceBusReceiver.AbandonMessageAsync(message, cancellationToken: cancellationToken);

                logger.LogError(ex, $"Error processing message - {message?.MessageId}");
            }
        } while (!cancellationToken.IsCancellationRequested);
    }

    private static string? GetBodyString(ServiceBusReceivedMessage message)
    {
        try
        {
            var body = message.Body;
            if (body != null) return Encoding.UTF8.GetString(body);
        }
        catch (NotSupportedException)
        {
            // ignore, we need to use the raw version
        }

        var amqpMessage = message.GetRawAmqpMessage();
        if (amqpMessage.Body.TryGetValue(out var value))
        {
            return value switch
            {
                string messageString => messageString,
                byte[] byteArray => Encoding.UTF8.GetString(byteArray),
                _ => null
            };
        }
        return null;
    }

    private async Task ProcessMessageAsync(ServiceBusReceivedMessage message, CancellationToken cancellationToken)
    {
        var bodyString = GetBodyString(message);
        if (bodyString==null) return;

        using var jsonDocument = JsonDocument.Parse(bodyString);

        IEnumerable<JsonElement> items = jsonDocument.RootElement.ValueKind switch
        {
            JsonValueKind.Array => jsonDocument.RootElement.EnumerateArray().ToList(),
            JsonValueKind.Object => [jsonDocument.RootElement],
            _ => throw new ApplicationException("Unexpected message format")
        };

        var x = 0;
        var count = items.Count();
        foreach (var item in items)
        {
            await IngestMessageAsync(item, cancellationToken);

            metrics.TrackMetricIngestion(typeof(T).Name, count == 1 ? message.MessageId : $"{message.MessageId}-{x++}", workerName);
        }

        await serviceBusReceiver!.CompleteMessageAsync(message, cancellationToken);
    }


    public abstract Task IngestMessageAsync(JsonElement item, CancellationToken cancellationToken);
}