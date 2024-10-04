namespace SapAct.Extensions;

public static class IConfigurationExtensions
{
	public static void CheckConfiguration(this IConfiguration configuration)
	{
		new ConfigurationValidator().Validate(configuration);
	}

	public static string? GetServiceBusConnectionString(this IConfiguration configuration) => configuration[Consts.ServiceBusConnectionStringConfigKey];

	public static string? GetServiceBusTopicName(this IConfiguration configuration) => configuration[Consts.ServiceBusTopicNameConfigKey];

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
}