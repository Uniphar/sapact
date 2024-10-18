namespace SapAct.Services;

//TODO: consider splitting schema upsert and data sink into separate services for readability
public class SQLService(IServiceProvider serviceProvider, ILockService lockService, ILogger<SQLService> logger) : VersionedSchemaBaseService(lockService)
{	

	private SqlConnection sqlConnection { get; set; }
	private SqlTransaction sqlTransaction { get; set; }

	public async Task IngestMessageAsync(JsonElement payload, CancellationToken cancellationToken= default)
	{
		ExtractKeyMessageProperties(payload, out var objectKey, out var objectType, out var dataVersion);
		if (!string.IsNullOrWhiteSpace(objectType) && !string.IsNullOrWhiteSpace(dataVersion) && !string.IsNullOrWhiteSpace(objectKey))
		{
			var schemaDescriptor = GenerateSchemaDescriptor(objectType, payload);
			await UpsertSQLStructuresAsync(schemaDescriptor, cancellationToken);
			await SinkDataAsync(payload, schemaDescriptor, objectKey);
		}
	}

	private async Task SinkDataAsync(JsonElement payload, SQLTableDescriptor schemaDescriptor, string rootKey)
	{
		sqlConnection = serviceProvider.GetRequiredService<SqlConnection>();
		using (sqlConnection)
		{
			sqlConnection.Open();
			sqlTransaction = sqlConnection.BeginTransaction();
			using (sqlTransaction)
			{
				try
				{
					await SinkDataAsyncInner(payload, schemaDescriptor, rootKey);
					await sqlTransaction.CommitAsync();
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

	//TODO: refactor the whole rootkey, PK, FK handling
	private async Task SinkDataAsyncInner(JsonElement element, SQLTableDescriptor schemaDescriptor, string rootKey, int? arrayIndex=null, string? foreignKey=null, CancellationToken cancellationToken=default)
	{
		if (schemaDescriptor.IsEmpty)
			return;

		var primaryKey = schemaDescriptor.Depth == 0 ? rootKey : $"{rootKey}_{schemaDescriptor.SqlTableName}";
		if (arrayIndex.HasValue)
		{
			primaryKey = $"{primaryKey}_{arrayIndex.Value.ToString()}";
		}

		if (element.ValueKind == JsonValueKind.Array)
		{
			int arrayIndexCurrent = 0;
			foreach (var arrayElement in element.EnumerateArray())
			{
				await SinkDataAsyncInner(arrayElement, schemaDescriptor, rootKey, arrayIndexCurrent, foreignKey: foreignKey);
				arrayIndexCurrent++;
			}
		}
		else if (element.ValueKind == JsonValueKind.Object)
		{
			
			await SinkJsonObjectAsync(schemaDescriptor.SqlTableName, element, schemaDescriptor, primaryKey, foreignKey);
			foreach (var nonScalar in element.GetNonScalarProperties())
			{
				await SinkDataAsyncInner(nonScalar.Value, schemaDescriptor.GetChildTableDescriptor(nonScalar.Name), rootKey, arrayIndex, foreignKey: primaryKey);
			}
		}
		else
		{
			throw new InvalidOperationException("Unexpected JSON structure - only objects and arrays are expected to be processed");
		}
	}

	private async Task SinkJsonObjectAsync(string tableName, JsonElement payload, SQLTableDescriptor schemaDescriptor, string primaryKey, string? foreignKey, CancellationToken cancellationToken=default)
	{
		//get fields
		List<(string columnName, string value)> columns = [];

		foreach (var column in payload.GetScalarProperties())
		{
			columns.Add(new(column.Name, column.Value.ToString()));
		}

		if (schemaDescriptor.Depth>0)
		{
			columns.Add(("PK", primaryKey));
		}

		if (!string.IsNullOrWhiteSpace(foreignKey))
		{
			columns.Add(("FK", foreignKey));
		}

		var sqlText = EmitTableInsertStatement(tableName, columns);
		SqlCommand sqlCommand = new(sqlText, sqlConnection, sqlTransaction);
		await sqlCommand.ExecuteNonQueryAsync();

	}

	private string EmitTableInsertStatement(string tableName, List<(string columnName, string value)> columns)
	{
		if (columns.Count == 0)
			return string.Empty;

		StringBuilder insertSB = new();
		bool addComma = false;

		insertSB.AppendLine($"INSERT INTO {tableName} (");

		StringBuilder insertColumnNamesSB = new();
		StringBuilder insertColumnValuesSB = new();

		foreach (var (columnName, value) in columns)
		{
			if (addComma)
			{
				insertColumnNamesSB.Append(',');
				insertColumnValuesSB.Append(',');
			}

			insertColumnNamesSB.Append($"{columnName}");
			insertColumnValuesSB.Append(!string.IsNullOrWhiteSpace(value)? $"'{value}'" : "NULL");

			addComma = true;
		}

		insertSB.Append(insertColumnNamesSB);
		insertSB.Append(')');
		insertSB.AppendLine(" VALUES (");
		insertSB.Append(insertColumnValuesSB);
		
		insertSB.Append(");");

		return insertSB.ToString();
	}

	public async Task ProjectSchemaFlowAsync(string tableName, JsonElement item, CancellationToken cancellationToken = default)
	{
		var schemaDescriptor = GenerateSchemaDescriptor(tableName, item);
		await UpsertSQLStructuresAsync(schemaDescriptor, cancellationToken);
	}

	private async Task UpsertSQLStructuresAsync(SQLTableDescriptor schemaDescriptor, CancellationToken cancellationToken)
	{
		using var connection = serviceProvider.GetRequiredService<SqlConnection>();
		connection.Open();
		var transaction = connection.BeginTransaction();

		try
		{
			await UpsertSQLTableAsync(schemaDescriptor, connection, transaction);
			await transaction.CommitAsync();
		}
		catch (Exception ex)
		{
			logger.LogError(ex, "Error upserting SQL structures"); 

			transaction.Rollback();
			throw;
		}

		transaction = null;
	}

	private async Task UpsertSQLTableAsync(SQLTableDescriptor schemaDescriptor, SqlConnection connection, SqlTransaction transaction, SQLTableDescriptor? parent=null, int depth=0)
	{		
		if (schemaDescriptor.IsEmpty)
			return;

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

		if (!string.IsNullOrWhiteSpace(sqlCommandText))
		{
			SqlCommand sqlCommand = new(sqlCommandText, connection, transaction);
			await sqlCommand.ExecuteNonQueryAsync();
		}

		foreach (var childTable in schemaDescriptor.ChildTables)
		{
			await UpsertSQLTableAsync(childTable, connection, transaction, schemaDescriptor, depth + 1);
		}
	}

	private string EmitTableCreateCommand(string tableName, SQLTableDescriptor schemaDescriptor, SQLTableDescriptor? parent)
	{
		StringBuilder tableUpsertSqlSB = new();	

		tableUpsertSqlSB.AppendLine($"CREATE TABLE {tableName} (");
		//root has implied PK - ObjectKey		

		bool addComma = false;
		int depth = schemaDescriptor.Depth;

		if (depth > 0)
		{
			tableUpsertSqlSB.AppendLine($"PK NVARCHAR(255) NOT NULL PRIMARY KEY");
			addComma = true;
		}

		if (depth > 0)
		{
			ArgumentNullException.ThrowIfNull(parent);

			if (addComma)
				tableUpsertSqlSB.Append(',');		

			tableUpsertSqlSB.AppendLine($"FK NVARCHAR(255) FOREIGN KEY REFERENCES {parent.SqlTableName}({(depth == 1 ? "ObjectKey" : "PK")})");
			addComma = true;
		}
		foreach (var column in schemaDescriptor.Columns)
		{
			if (addComma)
				tableUpsertSqlSB.Append(',');			

			tableUpsertSqlSB.AppendLine($"{column.ColumnName} {column.SQLDataType} {(depth == 0 && "objectKey" == column.ColumnName ? "NOT NULL PRIMARY KEY" : "")}");
			addComma = true;
		}

		tableUpsertSqlSB.AppendLine(");");

		return tableUpsertSqlSB.ToString();
	}

	private string EmitTableUpdateCommand(string tableName, IEnumerable<string> columns, SQLTableDescriptor schemaDescriptor)
	{
		StringBuilder tableUpdateSB = new();

		List<string> addedColumns = schemaDescriptor.Columns.Select(x => x.ColumnName).Except(columns).ToList(); //find added columns - only additive schema changes are applied
					
		if (addedColumns.Count == 0)
			return string.Empty;

		bool addComma = false;
		tableUpdateSB.AppendLine($"ALTER TABLE {tableName} ADD");
		foreach (var column in addedColumns)
		{
			if (addComma)
				tableUpdateSB.Append(',');

			tableUpdateSB.AppendLine($"{column} NVARCHAR(255) NULL");
			addComma = true;
		}

		return tableUpdateSB.ToString();
	}

	private async Task<(bool exists, IEnumerable<string> columns)> CheckTableExistsAsync(string tableName, SqlConnection connection, SqlTransaction transaction)
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

	public SQLTableDescriptor GenerateSchemaDescriptor(string tableName, JsonElement item)
	{
		var rootTable = tableName;
		Dictionary<string, int> tableNamingCtx = []; //TODO: load this from schema table eventually - mapping between JSON Path and SQL Table name

		var schema =  GenerateSchemaDescriptorInner(tableName, item, 0);

		AugmentSchema(schema, "$");

		return schema;

		void AugmentSchema(SQLTableDescriptor schema, string parentPrefix, SQLTableDescriptor? parentSchema = null)
		{
			schema.SqlTableName = GetSQLTableName(parentPrefix);
			foreach (var child in schema.ChildTables)
			{
				var  childPrefix = $"{parentPrefix}.{child.TableName}";
				AugmentSchema(child, childPrefix, schema);
			}
		}


		string GetSQLTableName(string jsonPath)
		{
			if ("$"==jsonPath)
				return rootTable;

			if (!tableNamingCtx.TryGetValue(jsonPath, out int index))
			{
				index = tableNamingCtx.Count;		

				tableNamingCtx.Add(jsonPath, index);
			}

			string value = $"{rootTable}{index}";
		
			return value;
		}

		SQLTableDescriptor GenerateSchemaDescriptorInner(string tableName, JsonElement item, int depth)
		{			
			
			var schemaDescriptor = new SQLTableDescriptor() { TableName = tableName, Depth=depth };

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
						ProcessJSONObject(schemaDescriptor, child.Name, child.Value, item.ValueKind, depth+1);
					}
					else
					{
						schemaDescriptor.Columns.Add(new SQLColumnDescriptor() { ColumnName = child.Name, SQLDataType = "NVARCHAR(255)" }); //TODO: provide type mapping
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
			if (levelDesc != null)
			{
				SQLTableDescriptor? existingNode = null;
				existingNode = jsonValueKind == JsonValueKind.Array
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
	}	

	private void MergeTableDescriptors(SQLTableDescriptor currentLevel, SQLTableDescriptor levelDesc)
	{
		foreach (var column in levelDesc.Columns)
		{
			if (!currentLevel.Columns.Any(x => x.ColumnName == column.ColumnName))
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
}
