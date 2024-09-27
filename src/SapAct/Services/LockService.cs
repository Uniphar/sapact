namespace SapAct.Services;

public class LockService
{
	public BlobServiceClient BlobServiceClient { get; init; }
	private BlobContainerClient _containerClient;
	private readonly IConfiguration configuration;

	public LockService(BlobServiceClient blobServiceClient, IConfiguration configuration)
    {
		BlobServiceClient = blobServiceClient;
		this.configuration = configuration;
		_containerClient = GetContainerClient();
	}

	private BlobContainerClient GetContainerClient()
	{
		_containerClient ??= BlobServiceClient.GetBlobContainerClient(configuration.GetLockServiceBlobContainerName());

		return _containerClient;
	}

	public async Task<(string? leaseId, LockStateEnum lockState)> ObtainLockAsync(string tableName, string version, TargetStorageEnum targetStorageEnum)
	{
		var blobClient = GetBlobClient(tableName, version, targetStorageEnum);

		do
		{
			try
			{
				var lease = await blobClient.GetBlobLeaseClient().AcquireAsync(TimeSpan.FromSeconds(60));

				return (lease.Value.LeaseId, LockStateEnum.LockObtained);
			}
			catch (RequestFailedException ex) when (ex.Status == 409)
			{
				return (null, LockStateEnum.AlreadyLocked);
			}
			catch (RequestFailedException ex) when (ex.Status == 404)
			{
				var path = Path.Combine(Path.GetTempPath(), $"{tableName}-{version}");
				using var stream = File.Create(path);
				stream.Close();
				await blobClient.UploadAsync(path);
			}
		} while (true);
	}

	public async Task<bool> CheckSchemaLockPresence(string tableName, string version, TargetStorageEnum targetStorage)
	{
		var props = await GetBlockLeasePropertiesAsync(tableName, version, targetStorage);
		if (props == null)
			return false;

		return props.LeaseState == LeaseState.Leased || props.LeaseState == LeaseState.Breaking;
	}

	public async Task<BlobProperties?> GetBlockLeasePropertiesAsync(string tableName, string version, TargetStorageEnum targetStorage)
	{
		var blobClient = GetBlobClient(tableName, version, targetStorage);

		if (!await blobClient.ExistsAsync())
			return null;

		return await blobClient.GetPropertiesAsync();
	}

	public async Task ReleaseLockAsync(string tableName, string version, TargetStorageEnum targetStorage, string leaseId)
	{
		var blobClient = GetBlobClient(tableName, version, targetStorage);
		var leaseClient = blobClient.GetBlobLeaseClient(leaseId);

		await leaseClient.ReleaseAsync();

		await blobClient.SetMetadataAsync(new Dictionary<string, string>() {{ Consts.SyncedSchemaLockBlobMetadataKey, true.ToString(CultureInfo.InvariantCulture) }});
	}

	public async Task<LockStateEnum> WaitForLockDissolvedAsync(string tableName, string version, TargetStorageEnum targetStorage)
	{

		BlobProperties? props;
		do
		{
			await Task.Delay(1000);
			props = await GetBlockLeasePropertiesAsync(tableName, version, targetStorage);
		} while (props == null || props.LeaseStatus == LeaseStatus.Locked);

		return props==null ? LockStateEnum.Available : TranslateLockState(props!.LeaseState);
	}

	private static LockStateEnum TranslateLockState(LeaseState leaseState)
	{
		return leaseState switch
		{
			LeaseState.Available => LockStateEnum.Available,
			LeaseState.Leased => LockStateEnum.AlreadyLocked,
			LeaseState.Expired => LockStateEnum.Available,
			LeaseState.Breaking => LockStateEnum.Breaking,
			LeaseState.Broken => LockStateEnum.Broken,
			_ => throw new ArgumentOutOfRangeException(nameof(leaseState), leaseState, null),
		};
	}

	public static string GetBlobName(string tableName, string version, TargetStorageEnum targetStorage) => $"{tableName}-{version}-{targetStorage}";
	private BlobClient GetBlobClient(string tableName, string version, TargetStorageEnum targetStorage) 
		=> GetContainerClient().GetBlobClient(GetBlobName(tableName, version, targetStorage));
}
