namespace SapAct.Services;

public abstract class VersionedSchemaBaseService(ILockService lockService)
{
	private readonly ConcurrentDictionary<string, string> _tableVersionMapping = new();

	protected async Task<SchemaCheckResultState> CheckObjectTypeSchemaAsync(string objectType, string version, TargetStorageEnum targetStorage)
	{
		bool found = _tableVersionMapping.TryGetValue(objectType, out string? schemaVersion);


		SchemaCheckResultState schemaCompareResult;

		if (found)
		{
			schemaCompareResult = CompareSchemaVersion(version, schemaVersion);

			if (schemaCompareResult != SchemaCheckResultState.Older)
				return schemaCompareResult;
		}

		
		//might have been done by other instance
		var props = await lockService.GetBlobPropertiesAsync(objectType, targetStorage);

		if (props == null || !props.Metadata.TryGetValue(Consts.SyncedSchemaVersionLockBlobMetadataKey, out string? metadataValue))
			return SchemaCheckResultState.Unknown;
		else
		{
			//push whatever is at blob to local cache
			UpdateObjectTypeSchema(objectType, metadataValue);

			return CompareSchemaVersion(version, metadataValue);
		}		
	}

	private static SchemaCheckResultState CompareSchemaVersion(string version, string? schemaVersion)
	{
		SchemaCheckResultState schemaCompareResult;
		schemaCompareResult = string.Compare(version, schemaVersion) switch
		{
			< 0 => SchemaCheckResultState.Newer, //current record version is older than the one seen before
			0 => SchemaCheckResultState.Current, //same
			> 0 => SchemaCheckResultState.Older, //current record version follows the one seen before
		};
		return schemaCompareResult;
	}

	protected static MessageRootProperties ExtractMessageRootProperties(JsonElement payload)
	{
		var objectKey = payload.GetProperty(Consts.MessageObjectKeyPropertyName).GetString();
		var objectType = payload.GetProperty(Consts.MessageObjectTypePropertyName).GetString();
		var dataVersion = payload.GetProperty(Consts.MessageDataVersionPropertyName).GetString();
		
		var eventTypePropertyExists = payload.TryGetProperty(Consts.MessageEventTypePropertyName, out var eventTypeProperty);
		var eventType = eventTypePropertyExists ? eventTypeProperty.GetString() : null;

		ArgumentException.ThrowIfNullOrWhiteSpace(objectKey);
		ArgumentException.ThrowIfNullOrWhiteSpace(objectType);
		ArgumentException.ThrowIfNullOrWhiteSpace(dataVersion);

		return new MessageRootProperties
		{
			objectKey = objectKey,
			objectType = objectType,
			dataVersion = dataVersion,
			eventType = eventType
		};	
	}

	protected void UpdateObjectTypeSchema(string objectType, string version)
	{
		_tableVersionMapping.AddOrUpdate(objectType, version, (key, oldValue) => version);
	}

	protected async Task ReleaseLockAsync(string objectType, string version, TargetStorageEnum targetStorage,  string leaseId)
	{
		await lockService.ReleaseLockAsync(objectType, version, targetStorage, leaseId);
	}

	protected async Task<(LockState lockState, string? leaseId)> ObtainLockAsync(string objectType, string version, TargetStorageEnum targetStorage)
	{
		string? leaseId;
		do
		{
			(leaseId, var lockState) = await lockService.ObtainLockAsync(objectType, version, targetStorage);
			if (!string.IsNullOrWhiteSpace(leaseId))
				return (lockState, leaseId);

			lockState = await lockService.WaitForLockDissolvedAsync(objectType, version, targetStorage);

			if (lockState == LockState.Available)
				return (LockState.Available, null); 	
			
		} while (true);		
	}
}
