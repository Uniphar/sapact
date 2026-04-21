namespace SapAct.Services;

public class ADXService(IAzureDataExplorerClient adxClient, DistributedLockService distributedLockService, ISchemaVersionStore schemaVersionStore)
    : VersionedSchemaBaseService(distributedLockService, schemaVersionStore)
{
    public async Task IngestMessage(string topic, JsonElement payload, CancellationToken cancellationToken)
    {
        //get key properties
        var messageProperties = ExtractMessageRootProperties(payload);
        var objectType = (messageProperties?.objectType ?? topic).MakeTableFriendly();
        var dataVersion = messageProperties?.dataVersion ?? "1";


        if (Consts.DeltaEventType == messageProperties?.eventType) return;

        //schema check
        var schemaCheck = await CheckObjectTypeSchemaAsync(objectType, dataVersion, TargetStorageEnum.ADX);
        if (schemaCheck is SchemaCheckResultState.Older or SchemaCheckResultState.Unknown)
        {
            bool lockAcquired = await AcquireSchemaLockAsync(objectType, TargetStorageEnum.ADX);
            if (lockAcquired)
            {
                var columnsList = payload.GenerateColumnList(TargetStorageEnum.ADX);
                await adxClient.CreateOrUpdateTableAsync(objectType, columnsList, cancellationToken);
                await CommitSchemaVersionAsync(objectType, dataVersion, TargetStorageEnum.ADX);
            }
        }

        await adxClient.IngestDataAsync(objectType, payload, cancellationToken);
    }
}
