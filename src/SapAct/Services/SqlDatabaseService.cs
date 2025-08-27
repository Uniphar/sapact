namespace SapAct.Services;

public class SqlDatabaseService : ISqlDatabaseService
{
    public async Task<(bool exists, IEnumerable<string> columns)> CheckTableExistsAsync(
        string tableName,
        SqlConnection connection,
        SqlTransaction transaction,
        CancellationToken cancellationToken = default)
    {
        List<string> columnList = [];
        bool exists = false;

        var sqlCommand = new SqlCommand($"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{tableName}'", connection, transaction);
        var res = await sqlCommand.ExecuteReaderAsync(cancellationToken);


        while (await res.ReadAsync(cancellationToken))
        {
            exists = true;
            columnList.Add(res.GetString(0));
        }

        await res.CloseAsync();

        return (exists, columnList);
    }

    public async Task<Dictionary<string, int>> PrefillSchemaTableAsync(string rootCtx, SqlConnection sqlConnection, SqlTransaction sqlTransaction, CancellationToken cancellationToken = default)
    {
        string schemaTableName = $"{rootCtx}_SchemaTable";
        Dictionary<string, int> result = new();
        (bool exists, var _) = await CheckTableExistsAsync(schemaTableName, sqlConnection, sqlTransaction, cancellationToken);
        if (exists)
        {
            var sqlCommand = new SqlCommand($"SELECT * FROM {schemaTableName}", sqlConnection, sqlTransaction);
            var res = await sqlCommand.ExecuteReaderAsync(cancellationToken);

            while (await res.ReadAsync(cancellationToken))
            {
                result.Add(res.GetString(0), res.GetInt32(1));
            }

            res.Close();
        }
        else
        {
            await CreateSchemaTableAsync(schemaTableName, sqlConnection, sqlTransaction, cancellationToken);
        }

        return result;
    }

    public async Task UpdateSchemaTableAsync(string rootCtx, SqlConnection sqlConnection, SqlTransaction sqlTransaction, Dictionary<string, int> tableNamingCtx, CancellationToken cancellationToken = default)
    {
        string schemaTableName = $"{rootCtx}_SchemaTable";

        foreach (var item in tableNamingCtx)
        {
            var sqlCommand = new SqlCommand(
                $"INSERT INTO {schemaTableName} (Path, TableIndex) SELECT '{item.Key}', {item.Value} WHERE NOT EXISTS(SELECT Path from {schemaTableName} WHERE Path='{item.Key}')",
                sqlConnection, sqlTransaction);
            await sqlCommand.ExecuteNonQueryAsync(cancellationToken);
        }
    }

    public async Task SinkJsonObjectAsync(
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

    public async Task<bool> UpsertSQLTableAsync(
        SQLTableDescriptor schemaDescriptor,
        SqlConnection connection,
        SqlTransaction transaction,
        SQLTableDescriptor? parent = null,
        int depth = 0,
        bool dryRun = false,
        CancellationToken cancellationToken = default)
    {
        if (schemaDescriptor.IsEmpty)
            return false;

        string tableName = schemaDescriptor.SqlTableName;

        (var exists, var columns) = await CheckTableExistsAsync(tableName, connection, transaction, cancellationToken);

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
            var childSchemaResult = await UpsertSQLTableAsync(childTable, connection, transaction, schemaDescriptor, depth + 1, dryRun: dryRun, cancellationToken);
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

    private static async Task CreateSchemaTableAsync(string tableName, SqlConnection sqlConnection, SqlTransaction sqlTransaction, CancellationToken cancellationToken = default)
    {
        var sqlCommand = new SqlCommand($"CREATE TABLE {tableName} (Path {Consts.SQLKeyColumnDefaultDataType} PRIMARY KEY, TableIndex INT)", sqlConnection, sqlTransaction);
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
}
