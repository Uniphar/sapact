namespace SapAct.Services;

public abstract class VersionedSchemaBaseService(DistributedLockService distributedLockService, ISchemaVersionStore schemaVersionStore)
{
    internal const int WaitBetweenChecks = 1000;
    private readonly ConcurrentDictionary<string, string> _tableVersionMapping = new();

    protected async Task<SchemaCheckResultState> CheckObjectTypeSchemaAsync(string objectType, string version, TargetStorageEnum targetStorage)
    {
        var found = _tableVersionMapping.TryGetValue(objectType, out var schemaVersion);

        if (found)
        {
            var schemaCompareResult = CompareSchemaVersion(version, schemaVersion);

            if (schemaCompareResult != SchemaCheckResultState.Older) return schemaCompareResult;
        }

        var persistedVersion = await schemaVersionStore.GetSchemaVersionAsync(objectType, targetStorage);
        if (persistedVersion == null) return SchemaCheckResultState.Unknown;

        UpdateObjectTypeSchema(objectType, persistedVersion);
        return CompareSchemaVersion(version, persistedVersion);
    }

    private static SchemaCheckResultState CompareSchemaVersion(string version, string? schemaVersion)
    {
        var schemaCompareResult = string.Compare(version, schemaVersion) switch
        {
            < 0 => SchemaCheckResultState.Newer, //current record version is older than the one seen before
            0 => SchemaCheckResultState.Current, //same
            > 0 => SchemaCheckResultState.Older //current record version follows the one seen before
        };
        return schemaCompareResult;
    }

    protected static MessageRootProperties? ExtractMessageRootProperties(JsonElement payload)
    {
        var objectKey = payload.TryGetProperty(Consts.MessageObjectKeyPropertyName, out var objectKeyProperty) ? objectKeyProperty.GetString() : null;
        var objectType = payload.TryGetProperty(Consts.MessageObjectTypePropertyName, out var objectTypeProperty) ? objectTypeProperty.GetString() : null;
        var dataVersion = payload.TryGetProperty(Consts.MessageDataVersionPropertyName, out var dataVersionProperty) ? dataVersionProperty.GetString() : null;
        if (string.IsNullOrWhiteSpace(objectKey) || string.IsNullOrWhiteSpace(objectType) || string.IsNullOrWhiteSpace(dataVersion)) return null;

        var eventTypePropertyExists = payload.TryGetProperty(Consts.MessageEventTypePropertyName, out var eventTypeProperty);
        var eventType = eventTypePropertyExists ? eventTypeProperty.GetString() : null;

        return new()
        {
            objectKey = objectKey,
            objectType = objectType,
            dataVersion = dataVersion,
            eventType = eventType
        };
    }

    private void UpdateObjectTypeSchema(string objectType, string version)
    {
        _tableVersionMapping.AddOrUpdate(objectType, version, (key, oldValue) => version);
    }

    /// <summary>
    ///     Acquires schema ownership for the given objectType + targetStorage combination.
    ///     Returns true if this instance owns schema updates for this key; false if another instance owns it.
    /// </summary>
    protected Task<bool> AcquireSchemaLockAsync(string objectType, TargetStorageEnum targetStorage, CancellationToken token) => distributedLockService.AcquireJobLockAsync($"{objectType}-{targetStorage}", token);

    /// <summary>
    ///     Acquires schema ownership for the given objectType + targetStorage combination.
    ///     Returns true if this instance owns schema updates for this key; false if another instance owns it.
    /// </summary>
    protected Task<bool> ReleaseSchemaLockAsync(string objectType, TargetStorageEnum targetStorage) => distributedLockService.ReleaseJobLockAsync($"{objectType}-{targetStorage}", token);

    /// <summary>
    ///     Updates the in-memory schema version cache and persists the version to the schema store.
    ///     Call this after a successful schema update.
    /// </summary>
    protected async Task CommitSchemaVersionAsync(string objectType, string version, TargetStorageEnum targetStorage)
    {
        UpdateObjectTypeSchema(objectType, version);
        await schemaVersionStore.SetSchemaVersionAsync(objectType, targetStorage, version);
    }
}
