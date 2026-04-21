var configKVUrl = Environment.GetEnvironmentVariable(Consts.KEYVAULT_CONFIG_URL) ?? throw new NoNullAllowedException(Consts.KEYVAULT_CONFIG_URL);

var builder = Host.CreateApplicationBuilder(args);

var credential = new DefaultAzureCredential();
builder.Services.AddSingleton(credential);

// Load configuration sources early so secrets are available for service registration
builder.Configuration.AddAzureKeyVault(new(configKVUrl), credential);
builder.Configuration.AddEnvironmentVariables(); //potentially overwrite KV (even for local dev)

var environment = builder.Environment.EnvironmentName.ToLower();
var envPrefix = environment == "local" ? "dev" : environment;
var cosmosAccountEndpoint = $"https://uni-devops-{envPrefix}-cosmos.documents.azure.com:443/";

var cosmosMasterKey = builder.Configuration["Cosmos:MasterKey"] ?? throw new NoNullAllowedException("Cosmos:MasterKey configuration has to be set.");
var cosmosMasterKey = builder.Configuration.GetLockServiceCosmosMasterKey() ?? throw new NoNullAllowedException(Consts.LockServiceCosmosMasterKeyConfigKey);
var cosmosConnectionString = $"AccountEndpoint={cosmosAccountEndpoint};AccountKey={cosmosMasterKey}";

builder.Services.AddSingleton<ISchemaVersionStore, BlobSchemaVersionStore>();
builder.Services.AddCosmosLockService(cosmosConnectionString);
builder.Services.AddSingleton<LogAnalyticsService>();
builder.Services.AddSingleton<ADXService>();
builder.Services.AddSingleton<SQLService>();

builder.Services.AddSingleton<ResourceInitializerService>();

builder.Services.AddHttpClient();

builder.Configuration.CheckConfiguration();

builder.SetupWorkers();

builder.Services.AddAzureClients(clientBuilder =>
{
    clientBuilder.AddLogsIngestionClient(new(builder.Configuration.GetLogAnalyticsIngestionUrl()!));
    clientBuilder.AddBlobServiceClient(new Uri(builder.Configuration.GetLockServiceBlobConnectionString()!));
    clientBuilder.UseCredential(credential);
});

var KustoConnectionStringBuilder = new KustoConnectionStringBuilder(builder.Configuration.GetADXClusterHostUrl(), builder.Configuration.GetADXClusterDBNameOrDefault())
    .WithAadTokenProviderAuthentication(async () => (await credential.GetTokenAsync(new([Consts.KustoTokenScope]))).Token);
builder.Services.AddSingleton<SapActMetrics>();
builder.Services.AddSingleton(KustoClientFactory.CreateCslQueryProvider(KustoConnectionStringBuilder));
builder.Services.AddSingleton(KustoClientFactory.CreateCslAdminProvider(KustoConnectionStringBuilder));
builder.Services.AddSingleton(KustoIngestFactory.CreateDirectIngestClient(KustoConnectionStringBuilder));
builder.Services.AddSingleton(KustoIngestFactory.CreateQueuedIngestClient(KustoConnectionStringBuilder));
builder.Services.AddSingleton<IAzureDataExplorerClient, AzureDataExplorerClient>();
builder.Services.AddTransient(sp => new SqlConnection(builder.Configuration.GetSQLConnectionString()));
builder.Services.AddTransient<SQLService>(); //per topic as it is stateless - (connection, transaction)
builder.Services.AddTransient<ISqlDatabaseService, SqlDatabaseService>();

builder.Services.AddSingleton(new LogAnalyticsServiceConfiguration
{
    SubscriptionId = builder.Configuration.GetLogAnalyticsSubscriptionId()!,
    ResourceGroupName = builder.Configuration.GetLogAnalyticsResourceGroupName()!,
    WorkspaceName = builder.Configuration.GetLogAnalyticsWorkspaceName()!,
    EndpointName = builder.Configuration.GetLogAnalyticsEndpointName()!
});
builder.RegisterOpenTelemetry("sapact").Build();
var host = builder.Build();

var regionCode = builder.Configuration.GetRegionCode() ?? throw new InvalidOperationException("REGION_CODE configuration is required.");
var cosmosDatabase = builder.Configuration.GetLockServiceCosmosDatabase() ?? throw new InvalidOperationException("SapAct:LockService:CosmosDatabase configuration is required.");
var cosmosContainer = builder.Configuration.GetLockServiceCosmosContainer() ?? throw new InvalidOperationException("SapAct:LockService:CosmosLockContainer configuration is required.");
await host.Services.InitializeDistributedLockAsync(regionCode, cosmosDatabase, cosmosContainer);

await host.InitializeResourcesAsync();

host.Run();