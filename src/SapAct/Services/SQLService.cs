namespace SapAct.Services;

//TODO: consider splitting schema upsert and data sink into separate services for readability
public class SQLService(
    IServiceProvider serviceProvider,
    ISqlTableService sqlTableService,
    ISqlDatabaseService sqlDatabaseService,
    ILockService lockService,
    ILogger<SQLService> logger) : VersionedSchemaBaseService(lockService)
{
    public async Task IngestMessageAsync(JsonElement payload, CancellationToken cancellationToken = default)
    {
        var messageProperties = ExtractMessageRootProperties(payload);
        if (Consts.DeltaEventType == messageProperties.eventType)
            return;

        var sqlConnection = serviceProvider.GetRequiredService<SqlConnection>();
        await using (sqlConnection)
        {
            await sqlConnection.OpenAsync(cancellationToken);
            var sqlTransaction = sqlConnection.BeginTransaction();
            await using (sqlTransaction)
            {
                try
                {
                    //schema check
                    var schemaCheck = await CheckObjectTypeSchemaAsync(messageProperties.objectType, messageProperties.dataVersion, TargetStorageEnum.SQL);

                    var schemaDescriptor = await GenerateSchemaDescriptorAsync(sqlConnection, sqlTransaction, messageProperties.objectType, payload, cancellationToken);
                    ///dry run to check if schema update is necessary as certain (sub)structures may only be populated for specific payload instances
                    ///so we can only build up a schema when these are set - data version property refers to logical schema but not it used in its entirety
                    var dryRunSchemaCheck = !schemaCheck.IsUpdateRequired() && await UpsertSQLStructuresAsync(schemaDescriptor, dryRun: true, cancellationToken);

                    if (schemaCheck.IsUpdateRequired() || dryRunSchemaCheck)
                    {
                        bool updateNecessary = true;

                        do
                        {
                            (var lockState, string? leaseId) = await ObtainLockAsync(messageProperties.objectType, messageProperties.dataVersion, TargetStorageEnum.SQL);
                            if (lockState == LockState.LockObtained)
                            {
                                await UpsertSQLStructuresAsync(schemaDescriptor, cancellationToken: cancellationToken);

                                UpdateObjectTypeSchema(messageProperties.objectType, messageProperties.dataVersion);

                                await ReleaseLockAsync(messageProperties.objectType, messageProperties.dataVersion, TargetStorageEnum.SQL, leaseId!);

                                updateNecessary = false;
                            }
                            else if (lockState == LockState.Available)
                            {
                                //schema was updated by another instance but let's check against persistent storage
                                var status = await CheckObjectTypeSchemaAsync(messageProperties.objectType, messageProperties.dataVersion, TargetStorageEnum.SQL);
                                updateNecessary = status != SchemaCheckResultState.Current;
                            }
                        } while (updateNecessary);
                    }

                    await SinkDataAsyncInnerAsync(sqlConnection, sqlTransaction, payload, schemaDescriptor,
                        new KeyDescriptor { RootKey = messageProperties.objectKey, ForeignKey = string.Empty }, cancellationToken);
                    await sqlTransaction.CommitAsync(cancellationToken);
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "Error upserting SQL structures");

                    sqlTransaction.Rollback();
                    throw;
                }
            }
        }
    }

    private async Task SinkDataAsyncInnerAsync(
        SqlConnection sqlConnection,
        SqlTransaction sqlTransaction,
        JsonElement element,
        SQLTableDescriptor schemaDescriptor,
        KeyDescriptor keyDescriptor,
        CancellationToken cancellationToken = default)
    {
        if (schemaDescriptor.IsEmpty)
            return;

        var primaryKey = schemaDescriptor.Depth == 0 ? keyDescriptor.RootKey : keyDescriptor.ForeignKey;
        if (keyDescriptor.ArrayIndex.HasValue)
        {
            primaryKey = $"{primaryKey}_{keyDescriptor.ArrayIndex.Value}";
        }

        if (element.ValueKind == JsonValueKind.Array)
        {
            int arrayIndexCurrent = 0;
            foreach (var arrayElement in element.EnumerateArray())
            {
                await SinkDataAsyncInnerAsync(sqlConnection, sqlTransaction, arrayElement, schemaDescriptor, 
                    keyDescriptor with { ArrayIndex = arrayIndexCurrent }, cancellationToken);
                arrayIndexCurrent++;
            }
        }
        else if (element.ValueKind == JsonValueKind.Object)
        {
            await sqlDatabaseService.SinkJsonObjectAsync(sqlConnection, sqlTransaction, schemaDescriptor.SqlTableName, element, schemaDescriptor,
                new KeyDescriptor { RootKey = primaryKey, ForeignKey = keyDescriptor.ForeignKey }, cancellationToken);
            foreach (var nonScalar in element.GetNonScalarProperties())
            {
                var childTable = schemaDescriptor.GetChildTableDescriptor(nonScalar.Name);
                await SinkDataAsyncInnerAsync(sqlConnection, sqlTransaction, nonScalar.Value, childTable!, 
                    keyDescriptor with { ForeignKey = primaryKey }, cancellationToken);
            }
        }
        else
        {
            throw new InvalidOperationException("Unexpected JSON structure - only objects and arrays are expected to be processed");
        }
    }

    private async Task<bool> UpsertSQLStructuresAsync(SQLTableDescriptor schemaDescriptor, bool dryRun = false, CancellationToken cancellationToken = default)
    {
        await using var connection = serviceProvider.GetRequiredService<SqlConnection>();
        await connection.OpenAsync(cancellationToken);
        var transaction = connection.BeginTransaction();

        try
        {
            var schemaChangeDetected = await sqlDatabaseService.UpsertSQLTableAsync(schemaDescriptor, connection, transaction, dryRun: dryRun, cancellationToken: cancellationToken);

            if (!dryRun)
            {
                await transaction.CommitAsync(cancellationToken);
            }

            return schemaChangeDetected;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error upserting SQL structures");

            transaction.Rollback();
            throw;
        }
    }

    private async Task<SQLTableDescriptor> GenerateSchemaDescriptorAsync(
        SqlConnection sqlConnection,
        SqlTransaction sqlTransaction,
        string tableName,
        JsonElement item,
        CancellationToken cancellationToken = default)
    {
        var rootTable = tableName;
        bool tableNamingCtxChanged = false;
        var tableNamingCtx = await sqlDatabaseService.PrefillSchemaTableAsync(rootTable, sqlConnection, sqlTransaction, cancellationToken); //TODO: consider caching based on schema version check

        var schema = sqlTableService.GenerateSchemaDescriptorInner(tableName, item);

        AugmentSchema(schema, "$");

        if (tableNamingCtxChanged)
        {
            await sqlDatabaseService.UpdateSchemaTableAsync(tableName, sqlConnection, sqlTransaction, tableNamingCtx, cancellationToken);
        }

        return schema;

        void AugmentSchema(SQLTableDescriptor schema, string parentPrefix, SQLTableDescriptor? parentSchema = null)
        {
            schema.SqlTableName = GetSQLTableName(parentPrefix);

            if (parentSchema == null) //top level
            {
                //handle designation of primary key for objectKey property of the payload - we have to use key column type rather than generic property type
                var pkColumn = schema.GetIgnoreCaseColumnDescriptor(Consts.MessageObjectKeyPropertyName) ??
                               throw new InvalidOperationException($"Column {Consts.MessageObjectKeyPropertyName} not found in schema descriptor, this is unexpected");
                pkColumn.SQLDataType = Consts.SQLKeyColumnDefaultDataType;
            }

            foreach (var child in schema.ChildTables)
            {
                var childPrefix = $"{parentPrefix}.{child.TableName}";
                AugmentSchema(child, childPrefix, schema);
            }
        }

        string GetSQLTableName(string jsonPath)
        {
            if ("$" == jsonPath)
                return rootTable;

            if (!tableNamingCtx.TryGetValue(jsonPath, out int index))
            {
                index = tableNamingCtx.Count;
                tableNamingCtxChanged = true;
                tableNamingCtx.Add(jsonPath, index);
            }

            string value = $"{rootTable}{index}";

            return value;
        }
    }
}
