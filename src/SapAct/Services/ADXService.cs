namespace SapAct.Services;

public class ADXService (IAzureDataExplorerClient adxClient, ILockService lockService) : VersionedSchemaBaseService(lockService)
{
    public async Task IngestMessage(string topic, JsonElement payload, CancellationToken cancellationToken)
    {
        //get key properties
        var messageProperties = ExtractMessageRootProperties(payload);
        var objectType = messageProperties?.objectType ?? topic;
        var dataVersion = messageProperties?.dataVersion ?? "1";


        if (Consts.DeltaEventType == messageProperties?.eventType) return;

        //schema check
        var schemaCheck = await CheckObjectTypeSchemaAsync(objectType, dataVersion, TargetStorageEnum.ADX);
        if (schemaCheck is SchemaCheckResultState.Older or SchemaCheckResultState.Unknown)
        {
            var updateNecessary = true;
            do
            {
                var (lockState, leaseId) = await ObtainLockAsync(objectType, TargetStorageEnum.ADX);
                if (lockState == LockState.LockObtained)
                {
                    var columnsList = payload.GenerateColumnList(TargetStorageEnum.ADX);

                    await adxClient.CreateOrUpdateTableAsync(objectType, columnsList, cancellationToken);
                    UpdateObjectTypeSchema(objectType, dataVersion);
                    await ReleaseLockAsync(objectType, dataVersion, TargetStorageEnum.ADX, leaseId!);

                    updateNecessary = false;
                }
                else if (lockState == LockState.Available)
                {
                    //schema was updated by another instance but let's check against persistent storage
                    var status = await CheckObjectTypeSchemaAsync(objectType, dataVersion, TargetStorageEnum.ADX);
                    updateNecessary = status != SchemaCheckResultState.Current;
                }
            } while (updateNecessary);
        }

        await adxClient.IngestDataAsync(objectType, payload, cancellationToken);
    }
}
