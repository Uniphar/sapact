namespace SapAct;

public static class Consts
{
	public const string KEYVAULT_CONFIG_URL = "SAPACT_CONFIGURATION_URL";

	public const string ServiceBusConfigurationSectionName = "SapAct:ServiceBus:Topic";
	public const string ServiceBusConnectionStringConfigKey = "ConnectionString";
	public const string ServiceBusTopicNameConfigKey = "Name";
	public const string ServiceBusTopicADXSinkDisabledConfigKey = "ADXSinkDisabled";
	public const string ServiceBusTopicLASinkDisabledConfigKey = "LASinkDisabled";
	public const string ServiceBusTopicSQLSinkDisabledConfigKey = "SQLSinkDisabled";

	public const string ServiceBusTopicSubscriptionNamePrefixConfigKey = "SapAct:ServiceBus:TopicSubscriptionNamePrefix";

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

	public const string SyncedSchemaVersionLockBlobMetadataKey = "SyncedSchemaVersion";

	public const string TelemetrySinkTypeDimensionName = "SinkType";
	public const string TelemetryMessageIdDimensionName = "MessageId";
	public const string TelemetryWorkerNameDimensionName = "WorkerName";

	public const string MessageObjectKeyPropertyName = "objectKey";
	public const string MessageObjectTypePropertyName = "objectType";
	public const string MessageDataVersionPropertyName = "dataVersion";
}
