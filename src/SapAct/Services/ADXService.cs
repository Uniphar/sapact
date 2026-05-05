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
        // keep checking, might be the other cluster that resolves it
        while (schemaCheck is SchemaCheckResultState.Older or SchemaCheckResultState.Unknown)
        {
            schemaCheck = await CheckObjectTypeSchemaAsync(objectType, dataVersion, TargetStorageEnum.ADX);
            var lockAcquired = await AcquireSchemaLockAsync(objectType, TargetStorageEnum.ADX, cancellationToken);
            if (lockAcquired)
            {
                var columnsList = payload.GenerateColumnList(TargetStorageEnum.ADX);
                await adxClient.CreateOrUpdateTableAsync(objectType, columnsList, cancellationToken);
                await CommitSchemaVersionAsync(objectType, dataVersion, TargetStorageEnum.ADX);
                // no CancellationToken for release lock, we want to make sure it's released
                await ReleaseSchemaLockAsync(objectType, TargetStorageEnum.ADX, CancellationToken.None);
                break;
            }

            await Task.Delay(1000, cancellationToken);
        }

        await adxClient.IngestDataAsync(objectType, payload, cancellationToken);
    }
}
