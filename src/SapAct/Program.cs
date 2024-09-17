var configKVUrl = Environment.GetEnvironmentVariable("SAPACT_CONFIGURATION_URL") ?? throw new NoNullAllowedException("CONFIGURATION_URL");

HostApplicationBuilder builder = Host.CreateApplicationBuilder(args);

builder.Services.AddHostedService<LogAnalyticsWorker>();
//builder.Services.AddHostedService<ADXWorker>();

var credential = new DefaultAzureCredential();
builder.Services.AddSingleton(credential);

builder.Services.AddApplicationInsightsTelemetryWorkerService(options => options.EnableAdaptiveSampling = false);

builder.Services.AddSingleton<LogAnalyticsService>();
builder.Services.AddSingleton<ADXService>();
builder.Services.AddHttpClient();

builder.Configuration.AddAzureKeyVault(new(configKVUrl), credential);

builder.Services.AddAzureClients((clientBuilder) => 
{
	clientBuilder.AddServiceBusClientWithNamespace(builder.Configuration[Consts.ServiceBusConnectionStringConfigKey]);
	clientBuilder.AddLogsIngestionClient(new Uri(builder.Configuration[Consts.LogAnalyticsIngestionUrl]!));
});

var kcsb = new KustoConnectionStringBuilder(builder.Configuration[Consts.ADXClusterHostUrl], "devops")
		   .WithAadTokenProviderAuthentication(async () => (await credential.GetTokenAsync(new([Consts.KustoTokenScope]))).Token);

builder.Services.AddSingleton(KustoClientFactory.CreateCslAdminProvider(kcsb));
builder.Services.AddSingleton(KustoIngestFactory.CreateDirectIngestClient(kcsb));
builder.Services.AddSingleton(KustoIngestFactory.CreateQueuedIngestClient(kcsb));
builder.Services.AddSingleton<IAzureDataExplorerClient, AzureDataExplorerClient>();
builder.Services.AddSingleton(new LogAnalyticsServiceConfiguration {
	SubscriptionId = builder.Configuration[Consts.LogAnalyticsSubscriptionId]!,
	ResourceGroupName = builder.Configuration[Consts.LogAnalyticsResourceGroup]!,
	WorkspaceName = builder.Configuration[Consts.LogAnalyticsWorkspaceName]!,
	EndpointName = builder.Configuration[Consts.LogAnalyticsEndpointName]!
});

IHost host = builder.Build();
host.Run();