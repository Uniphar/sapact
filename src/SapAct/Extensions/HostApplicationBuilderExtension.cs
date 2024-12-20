﻿namespace SapAct.Extensions;

public static class HostApplicationBuilderExtension
{
	public static void SetupWorkers(this HostApplicationBuilder builder)
	{
		var serviceBusTopics = builder.Configuration.GetServiceBusTopicConfiguration();

		foreach (var serviceBusTopic in serviceBusTopics)
		{
			if (serviceBusTopic.ADXSinkDisabled && serviceBusTopic.LASinkDisabled && serviceBusTopic.SQLSinkDisabled)
			{
				continue;
			}

			string name = $"{serviceBusTopic.ConnectionString}-{serviceBusTopic.TopicName}";

			builder.Services.AddAzureClients((clientBuilder) =>
			{
				clientBuilder
					.AddServiceBusAdministrationClientWithNamespace(serviceBusTopic.ConnectionString!)
					.WithName(name);

				clientBuilder
					.AddServiceBusClientWithNamespace(serviceBusTopic.ConnectionString!)
					.WithName(name);
			});

			//see https://github.com/dotnet/runtime/issues/38751

			if (!serviceBusTopic.ADXSinkDisabled)
			{
				builder.Services.AddSingleton<IHostedService, ADXWorker>((sp) =>
				{
					return new ADXWorker(name, serviceBusTopic, sp.GetRequiredService<IAzureClientFactory<ServiceBusClient>>(), sp.GetRequiredService<IAzureClientFactory<ServiceBusAdministrationClient>>(), sp.GetRequiredService<ADXService>(), sp.GetRequiredService<ILogger<ADXWorker>>(), sp.GetRequiredService<TelemetryClient>(), builder.Configuration);
				});
			}

			if (!serviceBusTopic.LASinkDisabled)
			{

				builder.Services.AddSingleton<IHostedService, LogAnalyticsWorker>((sp) =>
				{
					return new LogAnalyticsWorker(name, serviceBusTopic, sp.GetRequiredService<IAzureClientFactory<ServiceBusClient>>(), sp.GetRequiredService<IAzureClientFactory<ServiceBusAdministrationClient>>(), sp.GetRequiredService<LogAnalyticsService>(), sp.GetRequiredService<ILogger<LogAnalyticsWorker>>(), sp.GetRequiredService<TelemetryClient>(), builder.Configuration);
				});
			}

			if (!serviceBusTopic.SQLSinkDisabled)
			{

				builder.Services.AddSingleton<IHostedService, SQLWorker>((sp) =>
				{
					return new SQLWorker(name, serviceBusTopic, sp.GetRequiredService<IAzureClientFactory<ServiceBusClient>>(), sp.GetRequiredService<IAzureClientFactory<ServiceBusAdministrationClient>>(), sp.GetRequiredService<SQLService>(), sp.GetRequiredService<ILogger<SQLWorker>>(), sp.GetRequiredService<TelemetryClient>(), builder.Configuration);
				});
			}
		}
	}
}
