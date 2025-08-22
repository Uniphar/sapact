namespace SapAct.Extensions;

public static class IConfigurationExtensions
{
	public static void CheckConfiguration(this IConfiguration configuration)
	{
		configuration.ShouldBeValid();
	}

	public static IEnumerable<ServiceBusTopicConfiguration> GetServiceBusTopicConfiguration(this IConfiguration configuration)
	{
		var topics = new List<ServiceBusTopicConfiguration>();
		foreach (var section in configuration.GetSection(Consts.ServiceBusConfigurationSectionName).GetChildren())
		{
			topics.Add(new ServiceBusTopicConfiguration
			{
				ConnectionString = section[Consts.ServiceBusConnectionStringConfigKey]!,
				TopicName = section[Consts.ServiceBusTopicNameConfigKey]!,
				ADXSinkDisabled = section.GetADXSinkDisabled(),
				LASinkDisabled = section.GetLASinkDisabled(),
				SQLSinkDisabled = section.GetSQLSinkDisabled()
			});
		}
		return topics;
	}
	public static string? GetLogAnalyticsEndpointName(this IConfiguration configuration) => configuration[Consts.LogAnalyticsEndpointNameConfigKey];

	public static string GetTopicSubscriptionNameOrDefault<T>(this IConfiguration configuration) => configuration[$"{Consts.ServiceBusTopicSubscriptionNamePrefixConfigKey}{typeof(T).Name}"] ?? $"SapAct{typeof(T).Name}";

	public static string? GetLogAnalyticsSubscriptionId(this IConfiguration configuration) => configuration[Consts.LogAnalyticsSubscriptionIdConfigKey];

	public static string? GetLogAnalyticsResourceGroupName(this IConfiguration configuration) => configuration[Consts.LogAnalyticsResourceGroupConfigKey];

	public static string? GetLogAnalyticsWorkspaceName(this IConfiguration configuration) => configuration[Consts.LogAnalyticsWorkspaceNameConfigKey];

	public static string? GetLogAnalyticsWorkspaceId(this IConfiguration configuration) => configuration[Consts.LogAnalyticsWorkspaceIdConfigKey];

	public static string? GetLogAnalyticsIngestionUrl(this IConfiguration configuration) => configuration[Consts.LogAnalyticsIngestionUrlConfigKey];

	public static string? GetADXClusterHostUrl(this IConfiguration configuration) => configuration[Consts.ADXClusterHostUrlConfigKey];

	public static string GetADXClusterDBNameOrDefault(this IConfiguration configuration) => configuration[Consts.ADXClusterDBConfigKey] ?? "devops";

	public static string? GetLockServiceBlobConnectionString(this IConfiguration configuration) => configuration[Consts.LockServiceBlobConnectionStringConfigKey];

	public static string GetLockServiceBlobContainerNameOrDefault(this IConfiguration configuration) => configuration[Consts.LockServiceBlobContainerNameConfigKey] ?? "sapact";

	public static string? GetSQLConnectionString(this IConfiguration configuration) => configuration[Consts.SQLConnectionStringConfigKey];
}