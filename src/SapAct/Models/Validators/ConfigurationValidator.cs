namespace SapAct.Models.Validators;

public class ConfigurationValidator :AbstractValidator<IConfiguration>
{
    public ConfigurationValidator()
    {
		RuleFor(configuration => configuration).Custom((configuration, context) =>
		{
			if (!configuration.GetSection(Consts.ServiceBusConfigurationSectionName).Exists())
			{
				context.AddFailure($"{Consts.ServiceBusConfigurationSectionName} section is missing");
			}

			if (!configuration.GetSection(Consts.ServiceBusConfigurationSectionName).GetChildren().Any())
			{
				context.AddFailure("No Service Bus Topics are configured");
			}

			if (configuration.GetSection(Consts.ServiceBusConfigurationSectionName).GetChildren().Any(section => string.IsNullOrEmpty(section[Consts.ServiceBusConnectionStringConfigKey])))
			{
				context.AddFailure("Service Bus Connection String is missing in configuration");
			}

			if (configuration.GetSection(Consts.ServiceBusConfigurationSectionName).GetChildren().Any(section => string.IsNullOrEmpty(section[Consts.ServiceBusTopicNameConfigKey])))
			{
				context.AddFailure("Service Bus Topic Name is missing in configuration");
			}
		});

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

		RuleFor(configuration => configuration.GetSQLConnectionString())
			.NotNull()
			.NotEmpty()
			.WithMessage("SQL Connection String is missing in configuration");

	}
}
