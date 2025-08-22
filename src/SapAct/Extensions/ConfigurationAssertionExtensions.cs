using AwesomeAssertions.Execution;

namespace SapAct.Extensions;

public static class ConfigurationAssertionExtensions
{
    public static void ShouldBeValid(this IConfiguration cfg)
    {
        using (new AssertionScope())
        {
            var sbSection = cfg.GetSection(Consts.ServiceBusConfigurationSectionName);

            sbSection.Exists().Should().BeTrue($"{Consts.ServiceBusConfigurationSectionName} section is missing");

            var sbChildren = sbSection.GetChildren().ToList();

            sbChildren.Any().Should().BeTrue("No Service Bus Topics are configured");

            sbChildren.All(s => !string.IsNullOrEmpty(s[Consts.ServiceBusConnectionStringConfigKey]))
                      .Should().BeTrue("Service Bus Connection String is missing in configuration");

            sbChildren.All(s => !string.IsNullOrEmpty(s[Consts.ServiceBusTopicNameConfigKey]))
                      .Should().BeTrue("Service Bus Topic Name is missing in configuration");

            void MustHave(string? value, string message)
                => value.Should().NotBeNullOrWhiteSpace(message);

            MustHave(cfg.GetLogAnalyticsSubscriptionId(), "Log Analytics Subscription Id is missing in configuration");
            MustHave(cfg.GetLogAnalyticsResourceGroupName(), "Log Analytics Resource Group Name is missing in configuration");
            MustHave(cfg.GetLogAnalyticsWorkspaceName(), "Log Analytics Workspace Name is missing in configuration");
            MustHave(cfg.GetLogAnalyticsIngestionUrl(), "Log Analytics Ingestion Url is missing in configuration");
            MustHave(cfg.GetLogAnalyticsEndpointName(), "Log Analytics Endpoint is missing in configuration");
            MustHave(cfg.GetLockServiceBlobConnectionString(), "Log Service Blob Connection String is missing in configuration");
            MustHave(cfg.GetADXClusterHostUrl(), "ADX Cluster Host Url is missing in configuration");
            MustHave(cfg.GetSQLConnectionString(), "SQL Connection String is missing in configuration");
        }
    }
}
