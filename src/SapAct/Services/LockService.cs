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
		_containerClient ??= BlobServiceClient.GetBlobContainerClient(configuration.GetLockServiceBlobContainerNameOrDefault());

		return _containerClient;
	}

	public async Task<(string? leaseId, LockState lockState)> ObtainLockAsync(string tableName, string version, TargetStorageEnum targetStorageEnum)
	{
		var blobClient = GetBlobClient(tableName, targetStorageEnum);

		do
		{
			try
			{
				var lease = await blobClient.GetBlobLeaseClient().AcquireAsync(TimeSpan.FromSeconds(60));

				return (lease.Value.LeaseId, LockState.LockObtained);
			}
			catch (RequestFailedException ex) when (ex.Status == 409)
			{
				return (null, LockState.AlreadyLocked);
			}
			catch (RequestFailedException ex) when (ex.Status == 404)
			{
				var path = Path.Combine(Path.GetTempPath(), GetBlobName(tableName, targetStorageEnum));
				using var stream = File.Create(path);
				stream.Close();
				await blobClient.UploadAsync(path);
			}
		} while (true);
	}

	public async Task<bool> CheckSchemaLockPresence(string tableName, TargetStorageEnum targetStorage)
	{
		var props = await GetBlobPropertiesAsync(tableName, targetStorage);

		if (props == null)
			return false;

		return props.LeaseState == LeaseState.Leased || props.LeaseState == LeaseState.Breaking;
	}

	public async Task<BlobProperties?> GetBlobPropertiesAsync(string tableName, TargetStorageEnum targetStorage)
	{
		var blobClient = GetBlobClient(tableName, targetStorage);

		if (!await blobClient.ExistsAsync())
			return null;

		return await blobClient.GetPropertiesAsync();
	}

	public async Task ReleaseLockAsync(string tableName, string version, TargetStorageEnum targetStorage, string leaseId)
	{
		var blobClient = GetBlobClient(tableName, targetStorage);
		var leaseClient = blobClient.GetBlobLeaseClient(leaseId);

		await blobClient.SetMetadataAsync(new Dictionary<string, string>()
		{
			{ Consts.SyncedSchemaVersionLockBlobMetadataKey, version }
		}, new BlobRequestConditions { LeaseId = leaseId});

		await leaseClient.ReleaseAsync();	
	}

	public async Task<LockState> WaitForLockDissolvedAsync(string tableName, string version, TargetStorageEnum targetStorage)
	{

		BlobProperties? props;
		do
		{
			await Task.Delay(1000);
			props = await GetBlobPropertiesAsync(tableName, targetStorage);
		} while (props == null || props.LeaseStatus == LeaseStatus.Locked);

		return props==null ? LockState.Available : TranslateLockState(props!.LeaseState);
	}

	private static LockState TranslateLockState(LeaseState leaseState)
	{
		return leaseState switch
		{
			LeaseState.Available => LockState.Available,
			LeaseState.Leased => LockState.AlreadyLocked,
			LeaseState.Expired => LockState.Available,
			LeaseState.Breaking => LockState.Breaking,
			LeaseState.Broken => LockState.Broken,
			_ => throw new ArgumentOutOfRangeException(nameof(leaseState), leaseState, null),
		};
	}

	public static string GetBlobName(string tableName, TargetStorageEnum targetStorage) => $"{tableName}-{targetStorage}";
	
	private BlobClient GetBlobClient(string tableName, TargetStorageEnum targetStorage) 
		=> GetContainerClient().GetBlobClient(GetBlobName(tableName, targetStorage));
}
