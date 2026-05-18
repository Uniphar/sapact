namespace SapAct.Services;

public class ADXService(IAzureDataExplorerClient adxClient, DistributedLockService distributedLockService, ISchemaVersionStore schemaVersionStore, ILogger<ADXService> logger)
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
        logger.LogDebug("ADX schema check for {ObjectType} v{Version}: {Result}", objectType, dataVersion, schemaCheck);

        // keep checking, might be the other cluster that resolves it
        while (schemaCheck is SchemaCheckResultState.Older or SchemaCheckResultState.Unknown)
        {
            schemaCheck = await CheckObjectTypeSchemaAsync(objectType, dataVersion, TargetStorageEnum.ADX);
            if (!schemaCheck.IsUpdateRequired()) break;

            var lockAcquired = await AcquireSchemaLockAsync(objectType, TargetStorageEnum.ADX, cancellationToken);
            if (lockAcquired)
            {
                logger.LogInformation("ADX schema lock acquired for {ObjectType} v{Version}", objectType, dataVersion);
                try
                {
                    var columnsList = payload.GenerateColumnList(TargetStorageEnum.ADX);
                    await adxClient.CreateOrUpdateTableAsync(objectType, columnsList, cancellationToken);
                    await CommitSchemaVersionAsync(objectType, dataVersion, TargetStorageEnum.ADX);
                    logger.LogInformation("ADX schema committed for {ObjectType} v{Version}", objectType, dataVersion);
                }
                finally
                {
                    // no CancellationToken for release lock, we want to make sure it's released
                    await ReleaseSchemaLockAsync(objectType, TargetStorageEnum.ADX);
                    logger.LogDebug("ADX schema lock released for {ObjectType}", objectType);
                }
                break;
            }

            logger.LogDebug("ADX schema lock not acquired for {ObjectType}, waiting for other region", objectType);
            await Task.Delay(WaitBetweenChecks, cancellationToken);
        }

        await adxClient.IngestDataAsync(objectType, payload, cancellationToken);
    }
}
