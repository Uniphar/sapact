namespace SapAct.Services;

public class LogAnalyticsService(LogAnalyticsServiceConfiguration configuration, DefaultAzureCredential defaultAzureCredential, IHttpClientFactory httpClientFactory, LogsIngestionClient logsIngestionClient) : VersionedSchemaBaseService
{
    private readonly ConcurrentDictionary<string, string> _dcrMapping = new();

    private string EndpointId => $"/subscriptions/{configuration.SubscriptionId}/resourceGroups/{configuration.ResourceGroupName}/providers/Microsoft.Insights/dataCollectionEndpoints/{configuration.EndpointName}";
    private string WorkspaceId => $"/subscriptions/{configuration.SubscriptionId}/resourceGroups/{configuration.ResourceGroupName}/providers/Microsoft.OperationalInsights/workspaces/{configuration.WorkspaceName}";

	private string GetDCRUrl(string tableName) => $"https://management.azure.com/subscriptions/{configuration.SubscriptionId}/resourceGroups/{configuration.ResourceGroupName}/providers/Microsoft.Insights/dataCollectionRules/{tableName}DCR?api-version=2023-03-11";
    private string GetTableUrl(string tableName)=> $"https://management.azure.com/subscriptions/{configuration.SubscriptionId}/resourceGroups/{configuration.ResourceGroupName}/providers/Microsoft.OperationalInsights/workspaces/{configuration.WorkspaceName}/tables/{tableName}_CL?api-version=2022-10-01";

	private async Task<string> SyncTableSchema(string tableName, JsonElement payload, SchemaCheckResultState schemaCheckResult)
    {
        var token = await GetManagementTokenAsync();
        using var httpClient = httpClientFactory.CreateClient();

        httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", token.Token);

        List<ColumnDefinition> columnsList = payload.GenerateColumnList();

        //table upsert
        await SyncTableAsync(tableName, columnsList, schemaCheckResult, httpClient);
        //DCR upsert
        var dcrId = await SyncDCRAsync(tableName, columnsList, httpClient);
		//update schema state		

        return dcrId;

    }

    private async Task<string> SyncDCRAsync(string tableName, List<ColumnDefinition> columnsList, HttpClient httpClient)
    {
        var tableSchema = new
        {
            location = "northeurope",
            properties = new
            {
                dataCollectionEndpointId = EndpointId,
                streamDeclarations = new Dictionary<string, dynamic>(),
                destinations = new
                {
                    logAnalytics = new[] {
                        new {
                            workspaceResourceId = WorkspaceId,
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

        // Update the DCR
        response = await httpClient.PutAsync(dcrUrl, content);        

        var responseContent = await response.Content.ReadAsStringAsync();
        response.EnsureSuccessStatusCode();

        return JsonSerializer.Deserialize<JsonElement>(responseContent).ExportDCRImmutableId();
    }

	public async Task SinkToLogAnalytics(string tableName, string dcrImmutableId, JsonElement fullJson)
    {
        Dictionary<string, string> dataFields = [];

        //translate top level fields
        foreach (var field in fullJson.EnumerateObject().Where(x => x.Name != "data"))
        {
            dataFields.Add(field.Name, field.Value.ToString());
        }

        //translate data fields
        if (fullJson.TryGetProperty("data", out var dataField))
        {
            foreach (var field in dataField.EnumerateObject())
            {
                dataFields.Add(field.Name, field.Value.ToString());
            }
        }

        var data = BinaryData.FromObjectAsJson(new[] { dataFields });

        var response = await logsIngestionClient.UploadAsync(dcrImmutableId, $"Custom-{tableName}_CL", RequestContent.Create(data)).ConfigureAwait(false);
#if (DEBUG)
        var content = response.Content.ToString();
#endif
    }

    public async Task IngestMessage(JsonElement payload)
    {
        //get key properties
        var objectKey = payload.GetProperty("objectKey").GetString();
        var objectType = payload.GetProperty("objectType").GetString();
        var dataVersion = payload.GetProperty("dataVersion").GetString();

        if (!string.IsNullOrWhiteSpace(objectKey) && !string.IsNullOrWhiteSpace(objectType) && !string.IsNullOrWhiteSpace(dataVersion))
        {
            var schemaCheckResult = CheckObjectTypeSchema(objectType, dataVersion);
            string dcrId;
            if (schemaCheckResult == SchemaCheckResultState.Unknown || schemaCheckResult == SchemaCheckResultState.Older)
            {

                dcrId = await SyncTableSchema(objectType, payload, schemaCheckResult);
                
                UpdateSchema(objectKey, dataVersion, dcrId);
            }
            else
            {
                dcrId = _dcrMapping[objectType];
			}

            //send to log analytics
            await SinkToLogAnalytics(objectType, dcrId, payload);
        }
    }

    private void UpdateSchema(string tableName, string version, string dcrId)
    {
		UpdateObjectTypeSchema(tableName, version);

		_dcrMapping.AddOrUpdate(tableName, dcrId, (key, oldValue) => dcrId);

	}

	private async Task SyncTableAsync(string tableName, List<ColumnDefinition> columnsList, SchemaCheckResultState tableStatus, HttpClient httpClient)
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
            var response = await httpClient.PutAsync(endpoint, content);
#if (DEBUG)
            string responseContent = await response.Content.ReadAsStringAsync();
#endif
            response.EnsureSuccessStatusCode();
        }
        else
        {
            // Update the table
            var response = await httpClient.PatchAsync(endpoint, content);
#if (DEBUG)
            string responseContent = await response.Content.ReadAsStringAsync();
#endif
            response.EnsureSuccessStatusCode();
        }
    }


    private async Task<AccessToken> GetManagementTokenAsync()
    {
        var tokenRequestContext = new TokenRequestContext(["https://management.azure.com/.default"]);
        return await defaultAzureCredential.GetTokenAsync(tokenRequestContext);
    }

    private static string GetTableName(string objectName) => $"{objectName}_CL"; 
}

