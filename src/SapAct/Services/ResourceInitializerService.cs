namespace SapAct.Services;

public class ResourceInitializerService(BlobServiceClient blobServiceClient, ServiceBusAdministrationClient serviceBusAdministrationClient,  IConfiguration configuration)
{
	public async Task InitializeResources()
	{
		await serviceBusAdministrationClient.CreateTopicAsync(configuration.GetServiceBusTopicName());
		//SB subscriptions created per subscription/worker
		await blobServiceClient.GetBlobContainerClient(configuration.GetLockServiceBlobContainerNameOrDefault()).CreateIfNotExistsAsync();
	}
}
