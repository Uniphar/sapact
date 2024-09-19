namespace SapAct.Services;

public class ADXService (IAzureDataExplorerClient adxClient) : VersionedSchemaBaseService
{
	public async Task IngestMessage(JsonElement payload, CancellationToken cancellationToken)
	{
		//get key properties
		var objectKey = payload.GetProperty("objectKey").GetString();
		var objectType = payload.GetProperty("objectType").GetString();
		var dataVersion = payload.GetProperty("dataVersion").GetString();

		if (!string.IsNullOrWhiteSpace(objectType) && !string.IsNullOrWhiteSpace(dataVersion) && !string.IsNullOrWhiteSpace(objectKey))
		{
			List<ColumnDefinition> columnsList = payload.GenerateColumnList();

			//schema check
			var schemaCheck = CheckObjectTypeSchema(objectKey!, dataVersion!);
			if (schemaCheck == SchemaCheckResultState.Older || schemaCheck == SchemaCheckResultState.Unknown)
			{
				await adxClient.CreateOrUpdateTableAsync(objectType!, columnsList, cancellationToken);
				UpdateObjectTypeSchema(objectType!, dataVersion!);
			}
			
			await adxClient.IngestDataAsync(objectType!, payload, cancellationToken);
		}
	}
}
