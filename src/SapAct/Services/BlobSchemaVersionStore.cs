namespace SapAct.Services;

public class BlobSchemaVersionStore(BlobServiceClient blobServiceClient, IConfiguration configuration) : ISchemaVersionStore
{
    private readonly BlobContainerClient _containerClient = blobServiceClient.GetBlobContainerClient(configuration.GetLockServiceBlobContainerNameOrDefault());

    public async Task<string?> GetSchemaVersionAsync(string objectType, TargetStorageEnum targetStorage)
    {
        var blobClient = GetBlobClient(objectType, targetStorage);
        if (!await blobClient.ExistsAsync()) return null;
        var props = await blobClient.GetPropertiesAsync();
        return props.Value.Metadata.TryGetValue(Consts.SyncedSchemaVersionLockBlobMetadataKey, out var version) ? version : null;
    }

    public async Task SetSchemaVersionAsync(string objectType, TargetStorageEnum targetStorage, string version)
    {
        var blobClient = GetBlobClient(objectType, targetStorage);
        if (!await blobClient.ExistsAsync()) await blobClient.UploadAsync(BinaryData.FromString(string.Empty));

        await blobClient.SetMetadataAsync(new Dictionary<string, string>
            { { Consts.SyncedSchemaVersionLockBlobMetadataKey, version } });
    }

    public static string GetBlobName(string objectType, TargetStorageEnum targetStorage) => $"{objectType}-{targetStorage}";

    private BlobClient GetBlobClient(string objectType, TargetStorageEnum targetStorage) => _containerClient.GetBlobClient(GetBlobName(objectType, targetStorage));
}
