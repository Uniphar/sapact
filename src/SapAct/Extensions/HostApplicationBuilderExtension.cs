namespace SapAct.Extensions;

public static class HostApplicationBuilderExtension
{
    public static void SetupWorkers(this HostApplicationBuilder builder)
    {
        var serviceBusTopics = builder.Configuration.GetServiceBusTopicConfiguration();

        foreach (var serviceBusTopic in serviceBusTopics)
        {
            if (serviceBusTopic is { ADXSinkDisabled: true, LASinkDisabled: true, SQLSinkDisabled: true }) continue;

            var name = $"{serviceBusTopic.ConnectionString}-{serviceBusTopic.TopicName}";

            builder.Services.AddAzureClients(clientBuilder =>
            {
                clientBuilder
                    .AddServiceBusAdministrationClientWithNamespace(serviceBusTopic.ConnectionString!)
                    .WithName(name);

                clientBuilder
                    .AddServiceBusClientWithNamespace(serviceBusTopic.ConnectionString!)
                    .WithName(name);
            });

            //see https://github.com/dotnet/runtime/issues/38751

            if (!serviceBusTopic.ADXSinkDisabled) builder.Services.AddSingleton<IHostedService, ADXWorker>(sp => new(name, serviceBusTopic, sp.GetRequiredService<IAzureClientFactory<ServiceBusClient>>(), sp.GetRequiredService<IAzureClientFactory<ServiceBusAdministrationClient>>(), sp.GetRequiredService<ADXService>(), sp.GetRequiredService<ILogger<ADXWorker>>(), sp.GetRequiredService<ICustomEventTelemetryClient>(), sp.GetRequiredService<SapActMetrics>(), builder.Configuration));

            if (!serviceBusTopic.LASinkDisabled) builder.Services.AddSingleton<IHostedService, LogAnalyticsWorker>(sp => new(name, serviceBusTopic, sp.GetRequiredService<IAzureClientFactory<ServiceBusClient>>(), sp.GetRequiredService<IAzureClientFactory<ServiceBusAdministrationClient>>(), sp.GetRequiredService<LogAnalyticsService>(), sp.GetRequiredService<ILogger<LogAnalyticsWorker>>(), sp.GetRequiredService<ICustomEventTelemetryClient>(), sp.GetRequiredService<SapActMetrics>(), builder.Configuration));

            if (!serviceBusTopic.SQLSinkDisabled) builder.Services.AddSingleton<IHostedService, SQLWorker>(sp => new(name, serviceBusTopic, sp.GetRequiredService<IAzureClientFactory<ServiceBusClient>>(), sp.GetRequiredService<IAzureClientFactory<ServiceBusAdministrationClient>>(), sp.GetRequiredService<SQLService>(), sp.GetRequiredService<ILogger<SQLWorker>>(), sp.GetRequiredService<ICustomEventTelemetryClient>(), sp.GetRequiredService<SapActMetrics>(), builder.Configuration));
        }
    }
}