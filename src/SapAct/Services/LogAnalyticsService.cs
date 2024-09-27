namespace SapAct.Services;

public class LogAnalyticsService(LogAnalyticsServiceConfiguration configuration, DefaultAzureCredential defaultAzureCredential, IHttpClientFactory httpClientFactory, LogsIngestionClient logsIngestionClient, LockService lockService) : VersionedSchemaBaseService(lockService)
{
    private readonly ConcurrentDictionary<string, string> _dcrMapping = new();

    private string EndpointResourceId => $"/subscriptions/{configuration.SubscriptionId}/resourceGroups/{configuration.ResourceGroupName}/providers/Microsoft.Insights/dataCollectionEndpoints/{configuration.EndpointName}";
    private string WorkspaceResourceId => $"/subscriptions/{configuration.SubscriptionId}/resourceGroups/{configuration.ResourceGroupName}/providers/Microsoft.OperationalInsights/workspaces/{configuration.WorkspaceName}";

	private string GetDCRUrl(string tableName) => $"https://management.azure.com/subscriptions/{configuration.SubscriptionId}/resourceGroups/{configuration.ResourceGroupName}/providers/Microsoft.Insights/dataCollectionRules/{tableName}DCR?api-version=2023-03-11";
    private string GetTableUrl(string tableName)=> $"https://management.azure.com/subscriptions/{configuration.SubscriptionId}/resourceGroups/{configuration.ResourceGroupName}/providers/Microsoft.OperationalInsights/workspaces/{configuration.WorkspaceName}/tables/{tableName}_CL?api-version=2022-10-01";

	private async Task<string> SyncTableSchema(string tableName, JsonElement payload, SchemaCheckResultState schemaCheckResult, CancellationToken cancellationToken)
	{
		using HttpClient httpClient = await GetHttpClient();

		List<ColumnDefinition> columnsList = payload.GenerateColumnList(TargetStorageEnum.LogAnalytics);

		//table upsert
		await SyncTableAsync(tableName, columnsList, schemaCheckResult, httpClient, cancellationToken);
		//DCR upsert
		var dcrId = await SyncDCRAsync(tableName, columnsList, httpClient, cancellationToken);
		//update schema state		

		return dcrId;

	}

	private async Task<HttpClient> GetHttpClient()
	{
		var token = await GetManagementTokenAsync();
		var httpClient = httpClientFactory.CreateClient();

		httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", token.Token);
		return httpClient;
	}

	private async Task<string> SyncDCRAsync(string tableName, List<ColumnDefinition> columnsList, HttpClient httpClient, CancellationToken cancellationToken)
    {
        var tableSchema = new
        {
            location = "northeurope",
            properties = new
            {
                dataCollectionEndpointId = EndpointResourceId,
                streamDeclarations = new Dictionary<string, dynamic>(),
                destinations = new
                {
                    logAnalytics = new[] {
                        new {
                            workspaceResourceId = WorkspaceResourceId,
                            name = "LogAnalyticsDest",
                        }
                    }
                },
                dataFlows = new[] {
                    new {
                        streams = new[] { $"Custom-{tableName}_CL" },
                        destinations = new[] { "LogAnalyticsDest" },
                        transformKql = "source",
                        outputStream = $"Custom-{tableName}_CL"
                    }
                }
            }
        };

        tableSchema.properties.streamDeclarations.Add($"Custom-{tableName}_CL", new
        {
            columns = columnsList.ToArray()
        });


        var renderredTableSchema = JsonSerializer.Serialize(tableSchema);

        // Serialize the table schema to JSON
        var content = new StringContent(renderredTableSchema, Encoding.UTF8, "application/json");

        // Send the PUT/PATCH request to create the table
        string dcrUrl = GetDCRUrl(tableName);

		HttpResponseMessage response;       

        //delete first, this immediately sinks any changes
        response = await httpClient.DeleteAsync(dcrUrl, cancellationToken);
        response.EnsureSuccessStatusCode();

		// Update the DCR
		response = await httpClient.PutAsync(dcrUrl, content, cancellationToken);        

        var responseContent = await response.Content.ReadAsStringAsync(cancellationToken);
        response.EnsureSuccessStatusCode();

        return JsonSerializer.Deserialize<JsonElement>(responseContent).ExportDCRImmutableId();
    }

	public async Task SinkToLogAnalytics(string tableName, string dcrImmutableId, JsonElement fullJson)
    {
        var data = fullJson.GenerateBinaryData();

		var response = await logsIngestionClient.UploadAsync(dcrImmutableId, $"Custom-{tableName}_CL", RequestContent.Create(data)).ConfigureAwait(false);
#if (DEBUG)
        var content = response.Content.ToString();
#endif
    }

	public async Task IngestMessage(JsonElement payload, CancellationToken cancellationToken)
	{
		//get key properties
		ExtractKeyMessageProperties(payload, out var objectKey, out var objectType, out var dataVersion);

		if (!string.IsNullOrWhiteSpace(objectKey) && !string.IsNullOrWhiteSpace(objectType) && !string.IsNullOrWhiteSpace(dataVersion))
		{
			var schemaCheckResult = await CheckObjectTypeSchemaAsync(objectType, dataVersion, TargetStorageEnum.LogAnalytics);

			string dcrId = "";

			if (schemaCheckResult == SchemaCheckResultState.Unknown || schemaCheckResult == SchemaCheckResultState.Older)
			{			
				bool updateNeccessary = true;
				do
				{
					(var lockState, string? leaseId) = await ObtainLockAsync(objectType!, dataVersion!, TargetStorageEnum.LogAnalytics);
					if (lockState == LockStateEnum.LockObtained)
					{
						dcrId = await SyncTableSchema(objectType, payload, schemaCheckResult, cancellationToken);
						UpdateSchema(objectType, dataVersion, dcrId);
						await ReleaseLock(objectType!, dataVersion!, TargetStorageEnum.LogAnalytics, leaseId!);

						updateNeccessary = false;
					}
					else if (lockState == LockStateEnum.Available)
					{
						//schema was updated by another instance but let's check against persistent storage
						var status = await CheckObjectTypeSchemaAsync(objectType!, dataVersion!, TargetStorageEnum.LogAnalytics);
						updateNeccessary = status != SchemaCheckResultState.Current;
                        if (!updateNeccessary)
                        {
                            dcrId = await RefreshDCRIdAsync(objectType, cancellationToken);
							UpdateSchema(objectType, dataVersion, dcrId);
						}    
					}
				} while (updateNeccessary);
			}
			else
			{
				dcrId = await RefreshDCRIdAsync(objectType, cancellationToken); //TODO: maybe store as another metadata piece in the blob
			}


			//send to log analytics
			await SinkToLogAnalytics(objectType, dcrId!, payload);
		}
	}

	private async Task<string> RefreshDCRIdAsync(string tableName, CancellationToken cancellationToken)
	{
        using var httpClient = await GetHttpClient();

		string dcrUrl = GetDCRUrl(tableName);

		HttpResponseMessage response;

		// Update the DCR
		response = await httpClient.GetAsync(dcrUrl, cancellationToken);

		var responseContent = await response.Content.ReadAsStringAsync(cancellationToken);
		response.EnsureSuccessStatusCode();

		return JsonSerializer.Deserialize<JsonElement>(responseContent).ExportDCRImmutableId();
	}

	private void UpdateSchema(string tableName, string version, string dcrId)
    {
		UpdateObjectTypeSchema(tableName, version);

		_dcrMapping.AddOrUpdate(tableName, dcrId, (key, oldValue) => dcrId);
	}
    
    internal async Task DeleteTableAsync(string tableName, CancellationToken cancellationToken)
	{
		var endpoint = GetTableUrl(tableName);

		using HttpClient httpClient = await GetHttpClient();

		await httpClient.DeleteAsync(endpoint, cancellationToken);
	}

	private async Task SyncTableAsync(string tableName, List<ColumnDefinition> columnsList, SchemaCheckResultState tableStatus, HttpClient httpClient, CancellationToken cancellationToken)
    {
        var tableSchema = new
        {
            properties = new
            {
                schema = new
                {
                    name = GetTableName(tableName),
                    columns = columnsList.ToArray()
                }
            }
        };

        // Serialize the table schema to JSON
        string jsonContent = JsonSerializer.Serialize(tableSchema);
        var content = new StringContent(jsonContent, Encoding.UTF8, "application/json");

        // Send the PUT or PATCH to the API
        var endpoint = GetTableUrl(tableName);

		if (tableStatus == SchemaCheckResultState.Unknown)
        {
            // Create the table
            var response = await httpClient.PutAsync(endpoint, content, cancellationToken);
#if (DEBUG)
            string responseContent = await response.Content.ReadAsStringAsync(cancellationToken);
#endif
            response.EnsureSuccessStatusCode();
        }
        else
        {
            // Update the table
            var response = await httpClient.PatchAsync(endpoint, content, cancellationToken);
#if (DEBUG)
            string responseContent = await response.Content.ReadAsStringAsync(cancellationToken);
#endif
            response.EnsureSuccessStatusCode();
        }
    }

    private async Task<AccessToken> GetManagementTokenAsync()
    {
        var tokenRequestContext = new TokenRequestContext(["https://management.azure.com/.default"]);
        return await defaultAzureCredential.GetTokenAsync(tokenRequestContext);
    }

    public static string GetTableName(string objectName) => $"{objectName}_CL"; 
}

