namespace SapAct.Services;

public class LogAnalyticsService(
    LogAnalyticsServiceConfiguration configuration, 
    DefaultAzureCredential defaultAzureCredential, 
    IHttpClientFactory httpClientFactory, 
    LogsIngestionClient logsIngestionClient, 
    ILockService lockService)
        : VersionedSchemaBaseService(lockService)
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

	private async Task SinkToLogAnalytics(string tableName, string dcrImmutableId, JsonElement fullJson)
    {
        var data = fullJson.GenerateBinaryData();

		var response = await logsIngestionClient.UploadAsync(dcrImmutableId, $"Custom-{tableName}_CL", RequestContent.Create(data)).ConfigureAwait(false);
    }

	public async Task IngestMessage(JsonElement payload, CancellationToken cancellationToken)
	{
		//get key properties
		var messageProperties = ExtractMessageRootProperties(payload);
		if (Consts.DeltaEventType == messageProperties.eventType)
			return;

		var schemaCheckResult = await CheckObjectTypeSchemaAsync(messageProperties.objectType, messageProperties.dataVersion, TargetStorageEnum.LogAnalytics);

		string dcrId = "";

		if (schemaCheckResult == SchemaCheckResultState.Unknown || schemaCheckResult == SchemaCheckResultState.Older)
		{			
			bool updateNecessary = true;
			do
			{
				(var lockState, string? leaseId) = await ObtainLockAsync(messageProperties.objectType, messageProperties.dataVersion, TargetStorageEnum.LogAnalytics);
				if (lockState == LockState.LockObtained)
				{
					dcrId = await SyncTableSchema(messageProperties.objectType, payload, schemaCheckResult, cancellationToken);
					UpdateSchema(messageProperties.objectType, messageProperties.dataVersion, dcrId);
					await ReleaseLockAsync(messageProperties.objectType, messageProperties.dataVersion, TargetStorageEnum.LogAnalytics, leaseId!);

					updateNecessary = false;
				}
				else if (lockState == LockState.Available)
				{
					//schema was updated by another instance but let's check against persistent storage
					var status = await CheckObjectTypeSchemaAsync(messageProperties.objectType, messageProperties.dataVersion, TargetStorageEnum.LogAnalytics);
					updateNecessary = status != SchemaCheckResultState.Current;
                    if (!updateNecessary)
                    {
                        dcrId = await RefreshDCRIdAsync(messageProperties.objectType, cancellationToken);
						UpdateSchema(messageProperties.objectType, messageProperties.dataVersion, dcrId);
					}    
				}
			} while (updateNecessary);
		}
		else
		{
			dcrId = await RefreshDCRIdAsync(messageProperties.objectType, cancellationToken); //TODO: maybe store as another metadata piece in the blob
		}			

		//send to log analytics
		await SinkToLogAnalytics(messageProperties.objectType, dcrId!, payload);		
	}

	private async Task<string> RefreshDCRIdAsync(string tableName, CancellationToken cancellationToken)
	{
        using var httpClient = await GetHttpClient();

		string dcrUrl = GetDCRUrl(tableName);

			// Update the DCR
		var response = await httpClient.GetAsync(dcrUrl, cancellationToken);

		var responseContent = await response.Content.ReadAsStringAsync(cancellationToken);
		response.EnsureSuccessStatusCode();

		return JsonSerializer.Deserialize<JsonElement>(responseContent).ExportDCRImmutableId();
	}

	private void UpdateSchema(string tableName, string version, string dcrId)
    {
		UpdateObjectTypeSchema(tableName, version);

		_dcrMapping.AddOrUpdate(tableName, dcrId, (key, oldValue) => dcrId);
	}

	private async Task SyncTableAsync(string tableName, List<ColumnDefinition> columnsList, SchemaCheckResultState tableStatus, HttpClient httpClient, CancellationToken cancellationToken)
    {
		//get current schema if available
		var schema = await GetCurrentColumnListAsync(tableName, httpClient);
		if (schema == null)
		{
			schema = columnsList;
		}
		else
		{
			foreach (var item in columnsList)
			{
				if (schema.Any(c => c.Name == item.Name))
				{
					continue;
				}
				else
				{
					schema.Add(item);
				}
			}
		}

		var tableSchema = new
        {
            properties = new
            {
                schema = new
                {
                    name = GetTableName(tableName),
                    columns = schema.ToArray()
                }
            }
        };

        // Serialize the table schema to JSON
        string jsonContent = JsonSerializer.Serialize(tableSchema);
        var content = new StringContent(jsonContent, Encoding.UTF8, "application/json");

        // Send the PUT or PATCH to the API
        var endpoint = GetTableUrl(tableName);

            // Create/update the table
            var response = await httpClient.PutAsync(endpoint, content, cancellationToken);
#if (DEBUG)
            string responseContent = await response.Content.ReadAsStringAsync(cancellationToken);
#endif
            response.EnsureSuccessStatusCode();
    }

	private async Task<List<ColumnDefinition>?> GetCurrentColumnListAsync(string tableName, HttpClient httpClient)
	{
		var endpoint = GetTableUrl(tableName);

		try
		{
			HttpResponseMessage response = await httpClient.GetAsync(endpoint);
			response.EnsureSuccessStatusCode();

			var responseJson = await response.Content.ReadAsStringAsync();
			var responseElement = JsonSerializer.Deserialize<GetLATableResponseType>(responseJson);

			return responseElement?.Properties.Schema.Columns.Select(c => new ColumnDefinition { Name = c.Name, Type = c.Type }).ToList();
		}
		catch (HttpRequestException ex) when (ex.StatusCode == HttpStatusCode.NotFound) //new table
		{
			return null;
		}
	}

	private async Task<AccessToken> GetManagementTokenAsync()
    {
        var tokenRequestContext = new TokenRequestContext(["https://management.azure.com/.default"]);
        return await defaultAzureCredential.GetTokenAsync(tokenRequestContext);
    }

    public static string GetTableName(string objectName) => $"{objectName}_CL"; 
}

