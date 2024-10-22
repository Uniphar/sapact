var configKVUrl = Environment.GetEnvironmentVariable(Consts.KEYVAULT_CONFIG_URL) ?? throw new NoNullAllowedException(Consts.KEYVAULT_CONFIG_URL);

HostApplicationBuilder builder = Host.CreateApplicationBuilder(args);

var credential = new DefaultAzureCredential(); //TODO: customize chain of auth (ie remove unused)
builder.Services.AddSingleton(credential);

builder.Services.AddApplicationInsightsTelemetryWorkerService(options => options.EnableAdaptiveSampling = false);

builder.Services.AddSingleton<ILockService, LockService>();
builder.Services.AddSingleton<LogAnalyticsService>();
builder.Services.AddSingleton<ADXService>();
builder.Services.AddSingleton<SQLService>();

builder.Services.AddSingleton<ResourceInitializerService>();

builder.Services.AddHttpClient();

builder.Configuration.AddAzureKeyVault(new(configKVUrl), credential);

builder.Configuration.CheckConfiguration();

builder.SetupWorkers();

builder.Services.AddAzureClients((clientBuilder) => 
{
	clientBuilder.AddLogsIngestionClient(new Uri(builder.Configuration.GetLogAnalyticsIngestionUrl()!));
	clientBuilder.AddBlobServiceClient(new Uri(builder.Configuration.GetLockServiceBlobConnectionString()!));
	clientBuilder.UseCredential(credential);
});

var KustoConnectionStringBuilder = new KustoConnectionStringBuilder(builder.Configuration.GetADXClusterHostUrl(), builder.Configuration.GetADXClusterDBNameOrDefault())
		   .WithAadTokenProviderAuthentication(async () => (await credential.GetTokenAsync(new([Consts.KustoTokenScope]))).Token);

builder.Services.AddSingleton(KustoClientFactory.CreateCslQueryProvider(KustoConnectionStringBuilder));
builder.Services.AddSingleton(KustoClientFactory.CreateCslAdminProvider(KustoConnectionStringBuilder));
builder.Services.AddSingleton(KustoIngestFactory.CreateDirectIngestClient(KustoConnectionStringBuilder));
builder.Services.AddSingleton(KustoIngestFactory.CreateQueuedIngestClient(KustoConnectionStringBuilder));
builder.Services.AddSingleton<IAzureDataExplorerClient, AzureDataExplorerClient>();
builder.Services.AddTransient((sp)=> new SqlConnection(builder.Configuration.GetSQLConnectionString()));
builder.Services.AddSingleton<SQLService>();

builder.Services.AddSingleton(new LogAnalyticsServiceConfiguration {
	SubscriptionId = builder.Configuration.GetLogAnalyticsSubscriptionId()!,
	ResourceGroupName = builder.Configuration.GetLogAnalyticsResourceGroupName()!,
	WorkspaceName = builder.Configuration.GetLogAnalyticsWorkspaceName()!,
	EndpointName = builder.Configuration.GetLogAnalyticsEndpointName()!	
});

IHost host = builder.Build();

await host.InitializeResourcesAsync();

host.Run();