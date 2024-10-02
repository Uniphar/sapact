using System.Net.Sockets;
using System.Threading;

namespace SapAct.Services;

public abstract class VersionedSchemaBaseService(LockService lockService)
{
	private readonly ConcurrentDictionary<string, string> _tableVersionMapping = new();

	protected async Task<SchemaCheckResultState> CheckObjectTypeSchemaAsync(string objectType, string version, TargetStorageEnum targetStorage)
	{
		bool found = _tableVersionMapping.TryGetValue(objectType, out string? schemaVersion);

		if (!found)
		{
			//might have been done by other instance
			var props = await lockService.GetBlockLeasePropertiesAsync(objectType, version, targetStorage);

			if (props == null || !props.Metadata.ContainsKey(Consts.SyncedSchemaLockBlobMetadataKey))
				return SchemaCheckResultState.Unknown;
			else
			{
				UpdateObjectTypeSchema(objectType, version);

				return SchemaCheckResultState.Current;
			}
		}
		else
		{
			var schemaCompareResult = string.Compare(version, schemaVersion) switch
			{
				< 0 => SchemaCheckResultState.Current, //current record version is older than the one seen before
				0 => SchemaCheckResultState.Current, //same
				> 0 => SchemaCheckResultState.Older, //current record version follows the one seen before
			};

			return schemaCompareResult;
		}
	}

	protected static void ExtractKeyMessageProperties(JsonElement payload, out string? objectKey, out string? objectType, out string? dataVersion)
	{
		objectKey = payload.GetProperty("objectKey").GetString();
		objectType = payload.GetProperty("objectType").GetString();
		dataVersion = payload.GetProperty("dataVersion").GetString();
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
