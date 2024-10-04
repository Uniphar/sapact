namespace SapAct.Services;

public class ResourceInitializerService(BlobServiceClient blobServiceClient, IConfiguration configuration)
{
	public async Task InitializeResources()
	{
		await blobServiceClient.GetBlobContainerClient(configuration.GetLockServiceBlobContainerNameOrDefault()).CreateIfNotExistsAsync();
	}
}
