namespace SapAct;

public static class Consts
{
	public const string KEYVAULT_CONFIG_URL = "SAPACT_CONFIGURATION_URL";

	public const string ServiceBusConnectionStringConfigKey = "SapAct:ServiceBus:ConnectionString";
	public const string ServiceBusTopicNameConfigKey = "SapAct:ServiceBus:TopicName";
	public const string ServiceBusTopicSubscriptionNameConfigKeyPrefix = "SapAct:ServiceBus:TopicSubscriptionName";

	public const string LogAnalyticsSubscriptionIdConfigKey = "SapAct:LogAnalytics:SubscriptionId";
	public const string LogAnalyticsResourceGroupConfigKey = "SapAct:LogAnalytics:ResourceGroup";
	public const string LogAnalyticsWorkspaceNameConfigKey = "SapAct:LogAnalytics:WorkspaceName";
	public const string LogAnalyticsWorkspaceIdConfigKey = "SapAct:LogAnalytics:WorkspaceId";
	public const string LogAnalyticsEndpointNameConfigKey = "SapAct:LogAnalytics:EndpointName";
	public const string LogAnalyticsIngestionUrlConfigKey = "SapAct:LogAnalytics:EndpointIngestionUrl";

	public const string ADXClusterHostUrlConfigKey = "SapAct:Adx:HostUrl";
	public const string ADXClusterDBConfigKey = "SapAct:Adx:Database";

	public const string LockServiceBlobConnectionStringConfigKey = "SapAct:LockService:BlobConnectionString";
	public const string LockServiceBlobContainerNameConfigKey = "SapAct:LockService:BlobContainerName";

	public const string KustoTokenScope = "https://kusto.kusto.windows.net/.default";

	public const string SyncedSchemaLockBlobMetadataKey = "SyncedSchema";
}
