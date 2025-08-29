namespace SapAct.Services;

public class ResourceInitializerService(BlobServiceClient blobServiceClient, IConfiguration configuration)
{
    public async Task InitializeResourcesAsync()
    {
        var containerName = configuration.GetLockServiceBlobContainerNameOrDefault();
        var blobContainerClient = blobServiceClient.GetBlobContainerClient(containerName);

        if (!blobContainerClient.ExistsAsync())
        {
            await blobContainerClient.CreateAsync();
        }
    }
}