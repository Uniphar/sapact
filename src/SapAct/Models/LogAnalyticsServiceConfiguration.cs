namespace SapAct.Models;

public record LogAnalyticsServiceConfiguration
{
	public required string SubscriptionId { get; init; }
	public required string ResourceGroupName { get; init; }
	public required string WorkspaceName { get; init; }
	public required string EndpointName { get; init; }
}
