namespace SapAct.Services;

public class SQLService(IServiceProvider serviceProvider, ILogger<SQLService> logger)
{
	
	public void CheckConnection()
	{
		using var connection = serviceProvider.GetRequiredService<SqlConnection>();

		connection.Open();
		var sqlCommand = new SqlCommand("SELECT 1", connection);
		var res = sqlCommand.ExecuteScalar();
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
	}

	private async Task UpsertSQLTableAsync(SQLTableDescriptor schemaDescriptor, SqlConnection connection, SqlTransaction transaction, SQLTableDescriptor? parent=null, int depth=0)
	{
		StringBuilder tableUpsertSqlSB = new();
		tableUpsertSqlSB.AppendLine($"CREATE TABLE {schemaDescriptor.SqlTableName} (");
		//root has implied PK - ObjectKey		

		bool addComma=false;
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
			else
				addComma = true;

			tableUpsertSqlSB.AppendLine($"FK_{parent.SqlTableName} NVARCHAR(255) FOREIGN KEY REFERENCES {parent.SqlTableName}({(depth==1 ? "ObjectKey": "PK")})");
			addComma = true;
		}
		foreach (var column in schemaDescriptor.Columns)
		{
			if (addComma)
				tableUpsertSqlSB.Append(',');
			else
				addComma = true;

			tableUpsertSqlSB.AppendLine($"{column.ColumnName} {column.SQLDataType} {(depth==0 && "objectKey"==column.ColumnName ? "NOT NULL PRIMARY KEY": "")}");

		}

		tableUpsertSqlSB.AppendLine(");");


		string sqlStatement = tableUpsertSqlSB.ToString();
		SqlCommand sqlCommand = new(sqlStatement, connection, transaction);
		await sqlCommand.ExecuteNonQueryAsync();
		foreach (var childTable in schemaDescriptor.ChildTables)
		{
			await UpsertSQLTableAsync(childTable, connection, transaction, schemaDescriptor, depth + 1);
		}
	}

	public SQLTableDescriptor GenerateSchemaDescriptor(string tableName, JsonElement item)
	{
		var rootTable = tableName;
		Dictionary<string, string> tableNamingCtx = []; //TODO: load this from schema table eventually - mapping between JSON Path and SQL Table name

		var schema =  GenerateSchemaDescriptorInner(tableName, item);

		AugmentSchema(schema, "$");

		return schema;

		void AugmentSchema(SQLTableDescriptor schema, string parentPrefix)
		{
			schema.SqlTableName = GetSQLTableName(parentPrefix);
			foreach (var child in schema.ChildTables)
			{
				var  childPrefix = $"{parentPrefix}.{child.TableName}";
				AugmentSchema(child, childPrefix);
			}
		}


		string GetSQLTableName(string jsonPath)
		{
			if ("$"==jsonPath)
				return rootTable;

			if (!tableNamingCtx.TryGetValue(jsonPath, out string? value))
			{
				value = $"{rootTable}_{tableNamingCtx.Count}";
				tableNamingCtx.Add(jsonPath, value);
			}

			return value;
		}

		SQLTableDescriptor GenerateSchemaDescriptorInner(string tableName, JsonElement item)
		{			
			var schemaDescriptor = new SQLTableDescriptor() { TableName = tableName };

			if (item.ValueKind == JsonValueKind.Array)
			{
				foreach (var element in item.EnumerateArray())
				{
					if (element.ValueKind == JsonValueKind.Object)
					{
						ProcessJSONObject(schemaDescriptor, tableName, element, item.ValueKind);
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
						ProcessJSONObject(schemaDescriptor, child.Name, child.Value, item.ValueKind);
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

		void ProcessJSONObject(SQLTableDescriptor currentLevel, string tableName, JsonElement element, JsonValueKind jsonValueKind)
		{
			var levelDesc = GenerateSchemaDescriptorInner(tableName, element);
			//merge vs new
			if (levelDesc != null && (levelDesc.Columns.Count > 0 || levelDesc.ChildTables.Count > 0))
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
