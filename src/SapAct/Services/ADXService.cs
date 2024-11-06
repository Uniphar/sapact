namespace SapAct.Services;

public class ADXService (IAzureDataExplorerClient adxClient, ILockService lockService) : VersionedSchemaBaseService(lockService)
{
	public async Task IngestMessage(JsonElement payload, CancellationToken cancellationToken)
	{
		//get key properties
		var messageProperties = ExtractKeyMessageProperties(payload);
		if (Consts.DeltaEventType == messageProperties.eventType)
			return;

		//schema check
		var schemaCheck = await CheckObjectTypeSchemaAsync(messageProperties.objectType, messageProperties.dataVersion, TargetStorageEnum.ADX);
		if (schemaCheck == SchemaCheckResultState.Older || schemaCheck == SchemaCheckResultState.Unknown)
		{
			bool updateNecessary = true;
			do
			{
				(var lockState, string? leaseId) = await ObtainLockAsync(messageProperties.objectType, messageProperties.dataVersion, TargetStorageEnum.ADX);
				if (lockState == LockState.LockObtained)
				{
					List<ColumnDefinition> columnsList = payload.GenerateColumnList(TargetStorageEnum.ADX);

					await adxClient.CreateOrUpdateTableAsync(messageProperties.objectType, columnsList, cancellationToken);
					UpdateObjectTypeSchema(messageProperties.objectType, messageProperties.dataVersion);
					await ReleaseLockAsync(messageProperties.objectType, messageProperties.dataVersion, TargetStorageEnum.ADX, leaseId!);

					updateNecessary = false;
				}
				else if (lockState == LockState.Available)
				{
					//schema was updated by another instance but let's check against persistent storage
					var status = await CheckObjectTypeSchemaAsync(messageProperties.objectType, messageProperties.dataVersion, TargetStorageEnum.ADX);
					updateNecessary = status != SchemaCheckResultState.Current;
				}
			} while (updateNecessary);

		}
						
		await adxClient.IngestDataAsync(messageProperties.objectType, payload, cancellationToken);		
	}
}
