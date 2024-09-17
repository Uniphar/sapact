namespace SapAct.Services;

/// <summary>
/// Interface for an Azure Data Explorer client.
/// </summary>
public interface IAzureDataExplorerClient
{	
	public Task CreateOrUpdateTableAsync(string tableName, List<ColumnDefinition> schema, CancellationToken cancellationToken = default);

	
	public Task IngestDataAsync(string tableName, JsonElement item, CancellationToken cancellationToken = default);
}
