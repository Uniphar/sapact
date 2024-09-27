namespace SapAct.Services;

public class ADXService (IAzureDataExplorerClient adxClient, LockService lockService) : VersionedSchemaBaseService(lockService)
{
	public async Task IngestMessage(JsonElement payload, CancellationToken cancellationToken)
	{
		//get key properties
		ExtractKeyMessageProperties(payload, out var objectKey, out var objectType, out var dataVersion);

		if (!string.IsNullOrWhiteSpace(objectType) && !string.IsNullOrWhiteSpace(dataVersion) && !string.IsNullOrWhiteSpace(objectKey))
		{			
			//schema check
			var schemaCheck = await CheckObjectTypeSchemaAsync(objectType!, dataVersion!, TargetStorageEnum.ADX);
			if (schemaCheck == SchemaCheckResultState.Older || schemaCheck == SchemaCheckResultState.Unknown)
			{
				bool updateNeccessary = true;
				do
				{
					(var lockState, string? leaseId) = await ObtainLockAsync(objectType!, dataVersion!, TargetStorageEnum.ADX);
					if (lockState == LockStateEnum.LockObtained)
					{
						List<ColumnDefinition> columnsList = payload.GenerateColumnList(TargetStorageEnum.ADX);

						await adxClient.CreateOrUpdateTableAsync(objectType!, columnsList, cancellationToken);
						UpdateObjectTypeSchema(objectType!, dataVersion!);
						await ReleaseLock(objectType!, dataVersion!, TargetStorageEnum.ADX, leaseId!);

						updateNeccessary = false;
					}
					else if (lockState == LockStateEnum.Available)
					{
						//schema was updated by another instance but let's check against persistent storage
						var status = await CheckObjectTypeSchemaAsync(objectType!, dataVersion!, TargetStorageEnum.ADX);
						updateNeccessary = status != SchemaCheckResultState.Current;
					}
				} while (updateNeccessary);

			}
			
			await adxClient.IngestDataAsync(objectType!, payload, cancellationToken);
		}
	}
}
