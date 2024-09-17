namespace SapAct;

public static class Consts
{
	public const string ServiceBusConnectionStringConfigKey = "SapAct:ServiceBus:ConnectionString";
	public const string LogAnalyticsSubscriptionId = "SapAct:LogAnalytics:SubscriptionId";
	public const string LogAnalyticsResourceGroup = "SapAct:LogAnalytics:ResourceGroup";
	public const string LogAnalyticsWorkspaceName = "SapAct:LogAnalytics:WorkspaceName";
	public const string LogAnalyticsEndpointName = "SapAct:LogAnalytics:EndpointName";
	public const string LogAnalyticsIngestionUrl = "SapAct:LogAnalytics:EndpointIngestionUrl";

	public const string ADXClusterHostUrl = "SapAct:Adx:HostUrl";

	public const string KustoTokenScope = "https://kusto.kusto.windows.net/.default";
}
