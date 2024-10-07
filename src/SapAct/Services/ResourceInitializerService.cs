namespace SapAct.Services;

public class ResourceInitializerService(BlobServiceClient blobServiceClient, ServiceBusAdministrationClient serviceBusAdministrationClient,  IConfiguration configuration)
{
	public async Task InitializeResources()
	{
		if (!await serviceBusAdministrationClient.TopicExistsAsync(configuration.GetServiceBusTopicName()))
		{
			await serviceBusAdministrationClient.CreateTopicAsync(configuration.GetServiceBusTopicName());
		}
		//SB subscriptions created per subscription/worker
		await blobServiceClient.GetBlobContainerClient(configuration.GetLockServiceBlobContainerNameOrDefault()).CreateIfNotExistsAsync();
	}
}
