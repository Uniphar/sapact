namespace SapAct.Services;

public class LogAnalyticsService(
    LogAnalyticsServiceConfiguration configuration,
    DefaultAzureCredential defaultAzureCredential,
    IHttpClientFactory httpClientFactory,
    LogsIngestionClient logsIngestionClient,
    DistributedLockService distributedLockService,
    ISchemaVersionStore schemaVersionStore,
    ICustomEventTelemetryClient telemetryClient
)
    : VersionedSchemaBaseService(distributedLockService, schemaVersionStore)
{
    private readonly ConcurrentDictionary<string, string> _dcrMapping = new();

    private string EndpointResourceId => $"/subscriptions/{configuration.SubscriptionId}/resourceGroups/{configuration.ResourceGroupName}/providers/Microsoft.Insights/dataCollectionEndpoints/{configuration.EndpointName}";
    private string WorkspaceResourceId => $"/subscriptions/{configuration.SubscriptionId}/resourceGroups/{configuration.ResourceGroupName}/providers/Microsoft.OperationalInsights/workspaces/{configuration.WorkspaceName}";

    private string GetDCRUrl(string tableName) => $"https://management.azure.com/subscriptions/{configuration.SubscriptionId}/resourceGroups/{configuration.ResourceGroupName}/providers/Microsoft.Insights/dataCollectionRules/{tableName}DCR?api-version=2024-03-11";
    private string GetTableUrl(string tableName) => $"https://management.azure.com/subscriptions/{configuration.SubscriptionId}/resourceGroups/{configuration.ResourceGroupName}/providers/Microsoft.OperationalInsights/workspaces/{configuration.WorkspaceName}/tables/{tableName}_CL?api-version=2025-07-01";

    private async Task<string> SyncTableSchema(string tableName, JsonElement payload, CancellationToken cancellationToken)
    {
        using var httpClient = await GetHttpClient();

        var columnsList = payload.GenerateColumnList(TargetStorageEnum.LogAnalytics);

        //table upsert
        await SyncTableAsync(tableName, columnsList, httpClient, cancellationToken);
        //DCR upsert
        var dcrId = await SyncDCRAsync(tableName, columnsList, httpClient, cancellationToken);
        //update schema state		

        return dcrId;
    }

    private async Task<HttpClient> GetHttpClient()
    {
        var token = await GetManagementTokenAsync();
        var httpClient = httpClientFactory.CreateClient();

        httpClient.DefaultRequestHeaders.Authorization = new("Bearer", token.Token);
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
                    logAnalytics = new[]
                    {
                        new
                        {
                            workspaceResourceId = WorkspaceResourceId,
                            name = "LogAnalyticsDest"
                        }
                    }
                },
                dataFlows = new[]
                {
                    new
                    {
                        streams = new[] { $"Custom-{tableName}_CL" },
                        destinations = new[] { "LogAnalyticsDest" },
                        transformKql = "source",
                        outputStream = $"Custom-{tableName}_CL"
                    }
                }
            }
        };

        tableSchema.properties.streamDeclarations.Add($"Custom-{tableName}_CL",
            new
            {
                columns = columnsList.ToArray()
            });


        var renderedTableSchema = JsonSerializer.Serialize(tableSchema);

        // Serialize the table schema to JSON
        var content = new StringContent(renderedTableSchema, Encoding.UTF8, "application/json");

        // Send the PUT/PATCH request to create the table
        var dcrUrl = GetDCRUrl(tableName);

        var response =
            //delete first, this immediately sinks any changes
            await httpClient.DeleteAsync(dcrUrl, cancellationToken);
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

    public async Task IngestMessage(string topic, JsonElement payload, CancellationToken cancellationToken)
    {
        string dcrId;
        //get key properties
        var messageProperties = ExtractMessageRootProperties(payload);
        //will be used for the table name, cleanup up so it will work for the table
        var objectType = (messageProperties?.objectType ?? topic).MakeTableFriendly();
        var dataVersion = messageProperties?.dataVersion ?? "1";
        if (Consts.DeltaEventType == messageProperties?.eventType) return;


        var schemaCheckResult = await CheckObjectTypeSchemaAsync(objectType, dataVersion, TargetStorageEnum.LogAnalytics);
        // keep checking, might be the other cluster that resolves it
        while (schemaCheckResult is SchemaCheckResultState.Unknown or SchemaCheckResultState.Older)
        {
            schemaCheckResult = await CheckObjectTypeSchemaAsync(objectType, dataVersion, TargetStorageEnum.LogAnalytics);
            var lockAcquired = await AcquireSchemaLockAsync(objectType, TargetStorageEnum.LogAnalytics);
            if (lockAcquired)
            {
                dcrId = await SyncTableSchema(objectType, payload, cancellationToken);
                _dcrMapping.AddOrUpdate(objectType, dcrId, (key, oldValue) => dcrId);
                // commit does update too
                await CommitSchemaVersionAsync(objectType, dataVersion, TargetStorageEnum.LogAnalytics);
                //TODO release lock
                break;
            }

            await Task.Delay(1000, cancellationToken);
        }

        dcrId = await RefreshDCRIdAsync(objectType, cancellationToken);

        //send to log analytics
        await SinkToLogAnalytics(objectType, dcrId, payload);
    }

    private async Task<string> RefreshDCRIdAsync(string tableName, CancellationToken cancellationToken)
    {
        using var httpClient = await GetHttpClient();

        var dcrUrl = GetDCRUrl(tableName);

        // Update the DCR
        var response = await httpClient.GetAsync(dcrUrl, cancellationToken);

        var responseContent = await response.Content.ReadAsStringAsync(cancellationToken);
        response.EnsureSuccessStatusCode();

        return JsonSerializer.Deserialize<JsonElement>(responseContent).ExportDCRImmutableId();
    }



    private async Task SyncTableAsync(string tableName, List<ColumnDefinition> columnsList, HttpClient httpClient, CancellationToken cancellationToken)
    {
        //get current schema if available
        var schema = await GetCurrentColumnListAsync(tableName, httpClient);
        // new table
        if (schema == null)
            schema = columnsList;
        else
        {
            foreach (var item in columnsList)
            {
                if (schema.Any(c => c.Name == item.Name)) { }
                else
                    schema.Add(item);
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
        var jsonContent = JsonSerializer.Serialize(tableSchema);
        var content = new StringContent(jsonContent, Encoding.UTF8, "application/json");

        // Send the PUT or PATCH to the API
        var endpoint = GetTableUrl(tableName);

        // Create/update the table
        var response = await httpClient.PutAsync(endpoint, content, cancellationToken);
        if (!response.IsSuccessStatusCode)
        {
            var responseContent = await response.Content.ReadAsStringAsync(cancellationToken);
            telemetryClient.TrackEvent("ErrorResponse", new() { { "ResponseContent", responseContent } });
        }

        response.EnsureSuccessStatusCode();
    }

    private async Task<List<ColumnDefinition>?> GetCurrentColumnListAsync(string tableName, HttpClient httpClient)
    {
        var endpoint = GetTableUrl(tableName);

        try
        {
            var response = await httpClient.GetAsync(endpoint);
            if (!response.IsSuccessStatusCode)
            {
                var responseContent = await response.Content.ReadAsStringAsync();
                telemetryClient.TrackEvent("ErrorResponse", new() { { "ResponseContent", responseContent } });
            }

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