var configKVUrl = Environment.GetEnvironmentVariable(Consts.KEYVAULT_CONFIG_URL) ?? throw new NoNullAllowedException(Consts.KEYVAULT_CONFIG_URL);

HostApplicationBuilder builder = Host.CreateApplicationBuilder(args);

builder.Services.AddHostedService<LogAnalyticsWorker>();
builder.Services.AddHostedService<ADXWorker>();

var credential = new DefaultAzureCredential();
builder.Services.AddSingleton(credential);

builder.Services.AddApplicationInsightsTelemetryWorkerService(options => options.EnableAdaptiveSampling = false);

builder.Services.AddSingleton<LockService>();
builder.Services.AddSingleton<LogAnalyticsService>();
builder.Services.AddSingleton<ADXService>();
builder.Services.AddHttpClient();

builder.Configuration.AddAzureKeyVault(new(configKVUrl), credential);

builder.Configuration.CheckConfiguration();

builder.Services.AddAzureClients((clientBuilder) => 
{
	clientBuilder.AddServiceBusAdministrationClientWithNamespace(builder.Configuration.GetServiceBusConnectionString());
	clientBuilder.AddServiceBusClientWithNamespace(builder.Configuration.GetServiceBusConnectionString());
	clientBuilder.AddLogsIngestionClient(new Uri(builder.Configuration.GetLogAnalyticsIngestionUrl()!));
	clientBuilder.AddBlobServiceClient(new Uri(builder.Configuration.GetLockServiceBlobConnectionString()!));
});

var kcsb = new KustoConnectionStringBuilder(builder.Configuration.GetADXClusterHostUrl(), builder.Configuration.GetADXClusterDBNameOrDefault())
		   .WithAadTokenProviderAuthentication(async () => (await credential.GetTokenAsync(new([Consts.KustoTokenScope]))).Token);

builder.Services.AddSingleton(KustoClientFactory.CreateCslAdminProvider(kcsb));
builder.Services.AddSingleton(KustoIngestFactory.CreateDirectIngestClient(kcsb));
builder.Services.AddSingleton(KustoIngestFactory.CreateQueuedIngestClient(kcsb));
builder.Services.AddSingleton<IAzureDataExplorerClient, AzureDataExplorerClient>();

builder.Services.AddSingleton(new LogAnalyticsServiceConfiguration {
	SubscriptionId = builder.Configuration.GetLogAnalyticsSubscriptionId()!,
	ResourceGroupName = builder.Configuration.GetLogAnalyticsResourceGroupName()!,
	WorkspaceName = builder.Configuration.GetLogAnalyticsWorkspaceName()!,
	EndpointName = builder.Configuration.GetLogAnalyticsEndpointName()!	
});

IHost host = builder.Build();
host.Run();