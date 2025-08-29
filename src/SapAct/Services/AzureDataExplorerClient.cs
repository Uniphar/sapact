namespace SapAct.Services;

/// <summary>
/// Azure Data Explorer client.
/// </summary>
/// <param name="logger">The <see cref="ILogger"/> instance.</param>
/// <param name="cslAdminProvider">The <see cref="ICslAdminProvider"/> instance that allows to execute Azure Data Explorer control commands.</param>
/// <param name="kustoIngestClient">The <see cref="IKustoIngestClient"/> instance that allows to ingest data into Azure Data Explorer.</param>
public class AzureDataExplorerClient(
	ILogger<AzureDataExplorerClient> logger,
	ICslAdminProvider cslAdminProvider,
	ICslQueryProvider cslQueryProvider,
	IKustoIngestClient kustoIngestClient,
	IKustoQueuedIngestClient kustoQueuedIngestClient) : IAzureDataExplorerClient
{
	public async Task IngestDataAsync(string tableName, JsonElement item, CancellationToken cancellationToken = default)
	{
		try
		{
			var data = item.ExportToFlattenedDictionary();

			await IngestDataInternalAsync(tableName, data, $"{tableName}JSONMapping");		
		}
		catch (Exception ex)
		{
			logger.LogError(ex, $"Failed to ingest data: {ex.Message}");
		}
	}

	private async Task IngestDataInternalAsync(string tableName, Dictionary<string, string> dataFields, string tableMapping)
	{
		try
		{
			await IngestDataWithClientAsync(kustoIngestClient, dataFields, tableName, tableMapping);

			return;
		}
		catch (DirectIngestClientException ex) when (ex.Error.Contains("429-TooManyRequests"))
		{
			logger.LogWarning("Too many requests for direct ingestion. Switching to queued ingestion.");
		}

		await IngestDataWithClientAsync(kustoQueuedIngestClient, dataFields, tableName, tableMapping);
	}	


	public async Task<IEnumerable<(string name, string type)>> GetCurrentColumnListAsync(string tableName, CancellationToken cancellationToken = default)
	{
		var getSchemaCommand = $"{tableName} | getschema";

		List<(string name, string type)> currentColumns = [];
		try
		{
			var result = await cslQueryProvider.ExecuteQueryAsync(cslAdminProvider.DefaultDatabaseName, getSchemaCommand, new ClientRequestProperties(), cancellationToken);

			while (result.Read())
			{
				var name = result.GetString(0);
				var type = result.GetString(2);
				currentColumns.Add(new(name, type));
			}
		}
		catch (SemanticException)
		{
			//likely table does not exist
			return [];
		}

		return currentColumns.AsEnumerable();
	}

	/// <summary>
	/// Creates or updates table in Azure Data Explorer.
	/// </summary>
	/// <param name="table">Table to create or update.</param>
	/// <param name="cancellationToken">The <see cref="CancellationToken"/> instance to signal the request to cancel the operation.</param>
	/// <returns>The <see cref="Task"/> representing the asynchronous operation.</returns>
	public async Task CreateOrUpdateTableAsync(string tableName, List<ColumnDefinition> targetSchema, CancellationToken cancellationToken = default)
	{
		TableSchema tableSchema = new() { Name = tableName };

		var currentSchema = await GetCurrentColumnListAsync(tableName, cancellationToken);

		foreach (var column in currentSchema)
		{
			tableSchema.AddColumnIfMissing(new()
			{
				Name = column.name,
				Type = column.type
			});
		}

		foreach (var column in targetSchema)
		{
			tableSchema.AddColumnIfMissing(new()
			{
				Name = column.Name,
				Type = column.Type.TranslateToKustoType()
			});
		}

		var createTableCommand = CslCommandGenerator.GenerateTableCreateMergeCommand(tableSchema);

		await cslAdminProvider.ExecuteControlCommandAsync(cslAdminProvider.DefaultDatabaseName, createTableCommand);

		
		var mapping = GetMapping(targetSchema);
		var createTableMappingCommand =
			CslCommandGenerator.GenerateTableMappingCreateOrAlterCommand(IngestionMappingKind.Json, tableName, $"{tableName}JSONMapping", mapping);

		await cslAdminProvider.ExecuteControlCommandAsync(cslAdminProvider.DefaultDatabaseName, createTableMappingCommand);
	}

	private static List<ColumnMapping> GetMapping(List<ColumnDefinition> targetSchema)
	{
		List<ColumnMapping> columnMapping = [];

		foreach (var column in targetSchema)
		{
			columnMapping.Add(new()
			{
				ColumnName = column.Name,
				Properties = new()
					{
						{ MappingConsts.Path, $"$.{column.Name}" }
					}
			});
		};

		return columnMapping;
	}	

	/// <summary>
	/// Ingests data into Azure Data Explorer using provided Kusto ingest client.
	/// </summary>
	/// <typeparam name="TData"></typeparam>
	/// <param name="ingestClient">The <see cref="IKustoIngestClient"/> instance that allows to ingest data into Azure Data Explorer.</param>
	/// <param name="data">The data to ingest.</param>
	/// <param name="tableName">The name of the table to ingest data into.</param>
	/// <param name="tableMapping">The name of the table mapping to use for ingestion.</param>
	/// <returns></returns>
	private async Task IngestDataWithClientAsync<TData>(IKustoIngestClient ingestClient, TData data, string tableName, string tableMapping)
	{
		using MemoryStream dataStream = new(JsonSerializer.SerializeToUtf8Bytes(data));

		await ingestClient.IngestFromStreamAsync(dataStream, new(cslAdminProvider.DefaultDatabaseName, tableName)
		{
			Format = DataSourceFormat.json,
			IngestionMapping = new() { IngestionMappingReference = tableMapping }
		});
	}
}
