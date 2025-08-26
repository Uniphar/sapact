namespace SapAct.Services;

//TODO: consider splitting schema upsert and data sink into separate services for readability
public class SQLService(IServiceProvider serviceProvider, ILockService lockService, ILogger<SQLService> logger) : VersionedSchemaBaseService(lockService)
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

                    var schemaDescriptor = await GenerateSchemaDescriptorAsync(sqlConnection, sqlTransaction, messageProperties.objectType, payload);
                    ///dry run to check if schema update is necessary as certain (sub)structures may only be populated for specific payload instances
                    ///so we can only build up a schema when these are set - data version property refers to logical schema but not it used in its entirety
                    var dryRunSchemaCheck = !schemaCheck.IsUpdateRequired() && await UpsertSQLStructuresAsync(schemaDescriptor, cancellationToken, dryRun: true);

                    if (schemaCheck.IsUpdateRequired() || dryRunSchemaCheck)
                    {
                        bool updateNecessary = true;

                        do
                        {
                            (var lockState, string? leaseId) = await ObtainLockAsync(messageProperties.objectType, messageProperties.dataVersion, TargetStorageEnum.SQL);
                            if (lockState == LockState.LockObtained)
                            {
                                await UpsertSQLStructuresAsync(schemaDescriptor, cancellationToken);

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

                    await SinkDataAsyncInner(sqlConnection, sqlTransaction, payload, schemaDescriptor,
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

    private static async Task SinkDataAsyncInner(
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
                await SinkDataAsyncInner(sqlConnection, sqlTransaction, arrayElement, schemaDescriptor,
                    keyDescriptor with { ArrayIndex = arrayIndexCurrent }, cancellationToken);
                arrayIndexCurrent++;
            }
        }
        else if (element.ValueKind == JsonValueKind.Object)
        {
            await SinkJsonObjectAsync(sqlConnection, sqlTransaction, schemaDescriptor.SqlTableName, element, schemaDescriptor,
                new KeyDescriptor { RootKey = primaryKey, ForeignKey = keyDescriptor.ForeignKey }, cancellationToken);
            foreach (var nonScalar in element.GetNonScalarProperties())
            {
                var childTable = schemaDescriptor.GetChildTableDescriptor(nonScalar.Name);
                await SinkDataAsyncInner(sqlConnection, sqlTransaction, nonScalar.Value, childTable!, keyDescriptor with { ForeignKey = primaryKey },
                    cancellationToken: cancellationToken);
            }
        }
        else
        {
            throw new InvalidOperationException("Unexpected JSON structure - only objects and arrays are expected to be processed");
        }
    }

    private static async Task SinkJsonObjectAsync(
        SqlConnection sqlConnection,
        SqlTransaction sqlTransaction,
        string tableName,
        JsonElement payload,
        SQLTableDescriptor schemaDescriptor,
        KeyDescriptor keyDescriptor,
        CancellationToken cancellationToken = default)
    {
        //get fields
        List<(string columnName, string value)> columns = [];

        foreach (var column in payload.GetScalarProperties())
        {
            columns.Add(new(column.Name, column.Value.ToString()));
        }

        if (schemaDescriptor.Depth > 0)
        {
            columns.Add((Consts.SQLPKColumnName, keyDescriptor.RootKey));
        }

        if (!string.IsNullOrWhiteSpace(keyDescriptor.ForeignKey))
        {
            columns.Add((Consts.SQLFKColumnName, keyDescriptor.ForeignKey));
        }

        var sqlText = EmitTableInsertStatement(tableName, columns.Select(x => x.columnName).ToList(),
            schemaDescriptor.Depth > 0 ? Consts.SQLPKColumnName : Consts.MessageObjectKeyPropertyName);
        SqlCommand sqlCommand = new(sqlText, sqlConnection, sqlTransaction);

        foreach (var (columnName, value) in columns)
        {
            var columnDescriptor = schemaDescriptor.GetIgnoreCaseColumnDescriptor(columnName) ??
                                   throw new InvalidOperationException($"Column {columnName} not found in schema descriptor, this is unexpected");
            var translatedColumnName = columnDescriptor!.ColumnName;

            sqlCommand.Parameters.AddWithValue($"@{translatedColumnName}", value ?? (object)DBNull.Value);
        }

        await sqlCommand.ExecuteNonQueryAsync(cancellationToken);
    }

    private static string EmitTableInsertStatement(string tableName, IList<string> columns, string pkColumnName)
    {
        if (columns.Count == 0)
            return string.Empty;

        StringBuilder insertSB = new();
        bool addComma = false;

        insertSB.AppendLine($"INSERT INTO {tableName} (");

        StringBuilder insertColumnNamesSB = new();
        StringBuilder insertColumnValuesSB = new();

        string pkColumnValue = columns.First(x => x == pkColumnName);

        foreach (var columnName in columns)
        {
            if (addComma)
            {
                insertColumnNamesSB.Append(',');
                insertColumnValuesSB.Append(',');
            }

            insertColumnNamesSB.Append($"{columnName}");
            insertColumnValuesSB.Append($"@{columnName}");

            addComma = true;
        }

        insertSB.Append(insertColumnNamesSB);
        insertSB.Append(')');
        insertSB.AppendLine(" SELECT ");
        insertSB.Append(insertColumnValuesSB);

        insertSB.Append($" WHERE NOT EXISTS (SELECT {pkColumnName} FROM {tableName} where {pkColumnName}=@{pkColumnName})");

        return insertSB.ToString();
    }

    private async Task<bool> UpsertSQLStructuresAsync(SQLTableDescriptor schemaDescriptor, CancellationToken cancellationToken = default, bool dryRun = false)
    {
        await using var connection = serviceProvider.GetRequiredService<SqlConnection>();
        await connection.OpenAsync(cancellationToken);
        var transaction = connection.BeginTransaction();

        try
        {
            var schemaChangeDetected = await UpsertSqlTableAsync(schemaDescriptor, connection, transaction, dryRun: dryRun);

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

    private static async Task<bool> UpsertSqlTableAsync(SQLTableDescriptor schemaDescriptor, SqlConnection connection, SqlTransaction transaction,
        SQLTableDescriptor? parent = null,
        int depth = 0, bool dryRun = false)
    {
        if (schemaDescriptor.IsEmpty)
            return false;

        string tableName = schemaDescriptor.SqlTableName;

        (var exists, var columns) = await CheckTableExistsAsync(tableName, connection, transaction);

        string sqlCommandText;
        if (exists)
        {
            sqlCommandText = EmitTableUpdateCommand(tableName, columns, schemaDescriptor);
        }
        else
        {
            sqlCommandText = EmitTableCreateCommand(tableName, schemaDescriptor, parent);
        }

        bool schemaChanged = !string.IsNullOrWhiteSpace(sqlCommandText);

        if (schemaChanged && dryRun)
        {
            return true;
        }

        if (schemaChanged && !dryRun)
        {
            SqlCommand sqlCommand = new(sqlCommandText, connection, transaction);
            await sqlCommand.ExecuteNonQueryAsync();
        }

        bool childSchemaChanged = false;

        foreach (var childTable in schemaDescriptor.ChildTables)
        {
            var childSchemaResult = await UpsertSqlTableAsync(childTable, connection, transaction, schemaDescriptor, depth + 1, dryRun: dryRun);
            childSchemaChanged = childSchemaResult || childSchemaChanged;
        }

        return schemaChanged || childSchemaChanged;
    }

    private static string EmitTableCreateCommand(string tableName, SQLTableDescriptor schemaDescriptor, SQLTableDescriptor? parent)
    {
        StringBuilder tableUpsertSqlSB = new();

        tableUpsertSqlSB.AppendLine($"CREATE TABLE {tableName} (");
        //root has implied PK - ObjectKey		

        bool addComma = false;
        int depth = schemaDescriptor.Depth;

        if (depth > 0)
        {
            ArgumentNullException.ThrowIfNull(parent);

            var pkColumn = schemaDescriptor.GetIgnoreCaseColumnDescriptor(Consts.SQLPKColumnName) ??
                           throw new InvalidOperationException($"Column {Consts.SQLPKColumnName} not found in schema descriptor, this is unexpected");
            var fkColumn = schemaDescriptor.GetIgnoreCaseColumnDescriptor(Consts.SQLFKColumnName) ??
                           throw new InvalidOperationException($"Column {Consts.SQLFKColumnName} not found in schema descriptor, this is unexpected");

            tableUpsertSqlSB.AppendLine($"{pkColumn.ColumnName} {pkColumn.SQLDataType} NOT NULL PRIMARY KEY");
            tableUpsertSqlSB.AppendLine(
                $",{fkColumn.ColumnName} {fkColumn.SQLDataType} FOREIGN KEY REFERENCES {parent.SqlTableName}({(depth == 1 ? Consts.MessageObjectKeyPropertyName : "PK")})");

            addComma = true;
        }

        //project data columns
        foreach (var column in schemaDescriptor.Columns.Where(x => !x.IsSchemaColumn))
        {
            if (addComma)
                tableUpsertSqlSB.Append(',');

            tableUpsertSqlSB.AppendLine(
                $"{column.ColumnName} {column.SQLDataType} {(depth == 0 && Consts.MessageObjectKeyPropertyName == column.ColumnName ? "NOT NULL PRIMARY KEY" : "")}");
            addComma = true;
        }

        tableUpsertSqlSB.AppendLine(");");

        return tableUpsertSqlSB.ToString();
    }

    private static string EmitTableUpdateCommand(string tableName, IEnumerable<string> columns, SQLTableDescriptor schemaDescriptor)
    {
        StringBuilder tableUpdateSB = new();

        var schemaDescriptorColumnNames = schemaDescriptor.Columns.Select(x => x.ColumnName);

        var addedColumns = schemaDescriptorColumnNames
            .Except(columns, StringComparer.OrdinalIgnoreCase); //find added columns - only additive schema changes are applied - ignore casing

        //for updates, we must consider casing changes so best to update schema object to use previously seen casing
        var differentCasingColumns = schemaDescriptorColumnNames
            .Except(addedColumns, StringComparer.OrdinalIgnoreCase)
            .Except(columns);

        foreach (var diffCasingColumn in differentCasingColumns)
        {
            var columnToRename = schemaDescriptor.GetIgnoreCaseColumnDescriptor(diffCasingColumn)!;
            columnToRename.ColumnName = columns.First(x => x.Equals(diffCasingColumn, StringComparison.OrdinalIgnoreCase));
        }

        if (!addedColumns.Any())
            return string.Empty;

        tableUpdateSB.AppendLine($"ALTER TABLE {tableName} ADD");

        var columnString = string.Join(',', addedColumns.Select(x =>
        {
            var column = schemaDescriptor.GetIgnoreCaseColumnDescriptor(x)!;
            return $"{column.ColumnName} {column.SQLDataType} NULL";
        }));

        tableUpdateSB.Append(columnString);

        return tableUpdateSB.ToString();
    }

    private static async Task<(bool exists, IEnumerable<string> columns)> CheckTableExistsAsync(string tableName, SqlConnection connection, SqlTransaction transaction)
    {
        List<string> columnList = [];
        bool exists = false;

        var sqlCommand = new SqlCommand($"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{tableName}'", connection, transaction);
        var res = await sqlCommand.ExecuteReaderAsync();


        while (res.Read())
        {
            exists = true;
            columnList.Add(res.GetString(0));
        }

        await res.CloseAsync();

        return (exists, columnList);
    }

    private static async Task<SQLTableDescriptor> GenerateSchemaDescriptorAsync(
        SqlConnection sqlConnection,
        SqlTransaction sqlTransaction,
        string tableName, JsonElement item)
    {
        var rootTable = tableName;
        Dictionary<string, int> tableNamingCtx = [];
        bool tableNamingCtxChanged = false;

        await PrefillSchemaTableAsync(rootTable, sqlConnection, sqlTransaction); //TODO: consider caching based on schema version check

        var schema = GenerateSchemaDescriptorInner(tableName, item);

        AugmentSchema(schema, "$");

        if (tableNamingCtxChanged)
        {
            await UpdateSchemaTableAsync(tableName, sqlConnection!, sqlTransaction);
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

        async Task PrefillSchemaTableAsync(string rootCtx, SqlConnection sqlConnection, SqlTransaction sqlTransaction)
        {
            string schemaTableName = $"{rootCtx}_SchemaTable";
            (bool exists, var _) = await CheckTableExistsAsync(schemaTableName, sqlConnection, sqlTransaction);
            if (exists)
            {
                var sqlCommand = new SqlCommand($"SELECT * FROM {schemaTableName}", sqlConnection, sqlTransaction);
                var res = await sqlCommand.ExecuteReaderAsync();

                while (res.Read())
                {
                    tableNamingCtx.Add(res.GetString(0), res.GetInt32(1));
                }

                res.Close();
            }
            else
            {
                await CreateSchemaTableAsync(schemaTableName, sqlConnection, sqlTransaction);
            }
        }

        async Task CreateSchemaTableAsync(string tableName, SqlConnection sqlConnection, SqlTransaction sqlTransaction)
        {
            var sqlCommand = new SqlCommand($"CREATE TABLE {tableName} (Path {Consts.SQLKeyColumnDefaultDataType} PRIMARY KEY, TableIndex INT)", sqlConnection, sqlTransaction);
            await sqlCommand.ExecuteNonQueryAsync();
        }

        async Task UpdateSchemaTableAsync(string rootCtx, SqlConnection sqlConnection, SqlTransaction sqlTransaction)
        {
            string schemaTableName = $"{rootCtx}_SchemaTable";

            foreach (var item in tableNamingCtx)
            {
                var sqlCommand = new SqlCommand(
                    $"INSERT INTO {schemaTableName} (Path, TableIndex) SELECT '{item.Key}', {item.Value} WHERE NOT EXISTS(SELECT Path from {schemaTableName} WHERE Path='{item.Key}')",
                    sqlConnection, sqlTransaction);
                await sqlCommand.ExecuteNonQueryAsync();
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

        SQLTableDescriptor GenerateSchemaDescriptorInner(string tableName, JsonElement item, int depth = 0)
        {
            var schemaDescriptor = new SQLTableDescriptor() { TableName = tableName, Depth = depth };

            if (depth > 0)
            {
                //add PK and FK as explicit columns
                schemaDescriptor.Columns.Add(new SQLColumnDescriptor()
                    { ColumnName = Consts.SQLPKColumnName, SQLDataType = Consts.SQLKeyColumnDefaultDataType, IsSchemaColumn = true });
                schemaDescriptor.Columns.Add(new SQLColumnDescriptor()
                    { ColumnName = Consts.SQLFKColumnName, SQLDataType = Consts.SQLKeyColumnDefaultDataType, IsSchemaColumn = true });
            }

            if (item.ValueKind == JsonValueKind.Array)
            {
                foreach (var element in item.EnumerateArray())
                {
                    if (element.ValueKind == JsonValueKind.Object)
                    {
                        ProcessJSONObject(schemaDescriptor, tableName, element, item.ValueKind, depth);
                    }
                    else
                    {
                        throw new InvalidOperationException("Only JSON objects are expected to be iterated over");
                    }
                }
            }
            else if (item.ValueKind == JsonValueKind.Object)
            {
                //find all nested types
                foreach (var child in item.EnumerateObject())
                {
                    if (child.Value.ValueKind == JsonValueKind.Object || child.Value.ValueKind == JsonValueKind.Array)
                    {
                        ProcessJSONObject(schemaDescriptor, child.Name, child.Value, item.ValueKind, depth + 1);
                    }
                    else
                    {
                        schemaDescriptor.Columns.Add(new SQLColumnDescriptor() { ColumnName = child.Name, SQLDataType = Consts.SQLDefaultDataType });
                    }
                }
            }
            else
            {
                throw new InvalidOperationException("Unexpected JSON structure - only objects and arrays are expected to be processed");
            }

            return schemaDescriptor;
        }

        void ProcessJSONObject(SQLTableDescriptor currentLevel, string tableName, JsonElement element, JsonValueKind jsonValueKind, int depth)
        {
            var levelDesc = GenerateSchemaDescriptorInner(tableName, element, depth);
            //merge vs new
            var existingNode = jsonValueKind == JsonValueKind.Array
                ? currentLevel
                : currentLevel.ChildTables.FirstOrDefault(x => x.TableName == tableName);

            if (existingNode != null)
            {
                MergeTableDescriptors(existingNode, levelDesc);
            }
            else
            {
                //new child table
                currentLevel.ChildTables.Add(levelDesc);
            }
        }
    }

    private static void MergeTableDescriptors(SQLTableDescriptor currentLevel, SQLTableDescriptor levelDesc)
    {
        foreach (var column in levelDesc.Columns)
        {
            if (currentLevel.Columns.All(x => x.ColumnName != column.ColumnName))
            {
                currentLevel.Columns.Add(column);
            }
        }

        foreach (var child in levelDesc.ChildTables)
        {
            var existingChild = currentLevel.ChildTables.FirstOrDefault(x => x.TableName == child.TableName);
            if (existingChild != null)
            {
                MergeTableDescriptors(existingChild, child);
            }
            else
            {
                currentLevel.ChildTables.Add(child);
            }
        }
    }

    private record KeyDescriptor
    {
        public required string RootKey { get; init; }
        public required string ForeignKey { get; init; }
        public int? ArrayIndex { get; init; }
    }
}