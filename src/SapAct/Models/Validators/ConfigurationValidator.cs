namespace SapAct.Models.Validators;

public class ConfigurationValidator :AbstractValidator<IConfiguration>
{
    public ConfigurationValidator()
    {
        RuleFor(configuration => configuration.GetServiceBusConnectionString())
			.NotNull()
			.NotEmpty()
			.WithMessage("Service Bus connection string is missing in configuration");


		RuleFor(configuration => configuration.GetServiceBusTopicName())
			.NotNull()
			.NotEmpty()
			.WithMessage("Service Bus Topic Name is missing in configuration");

		RuleFor(configuration => configuration.GetLogAnalyticsSubscriptionId())
			.NotNull()
			.NotEmpty()
			.WithMessage("Log Analytics Subscription Id is missing in configuration");

		RuleFor(configuration => configuration.GetLogAnalyticsResourceGroupName())
			.NotNull()
			.NotEmpty()
			.WithMessage("Log Analytics Resource Group Name is missing in configuration");

		RuleFor(configuration => configuration.GetLogAnalyticsWorkspaceName())
			.NotNull()
			.NotEmpty()
			.WithMessage("Log Analytics Workspace Name is missing in configuration");

		RuleFor(configuration => configuration.GetLogAnalyticsIngestionUrl())
			.NotNull()
			.NotEmpty()
			.WithMessage("Log Analytics Ingestion Url is missing in configuration");

		RuleFor(configuration => configuration.GetLogAnalyticsEndpointName())
			.NotNull()
			.NotEmpty()
			.WithMessage("Log Analytics Endpoint is missing in configuration");

		RuleFor(configuration => configuration.GetLockServiceBlobConnectionString())
			.NotNull()
			.NotEmpty()
			.WithMessage("Log Service Blob Connection String is missing in configuration");

		RuleFor(configuration => configuration.GetADXClusterHostUrl())
			.NotNull()
			.NotEmpty()
			.WithMessage("ADX Cluster Host Url is missing in configuration");

	}
}
