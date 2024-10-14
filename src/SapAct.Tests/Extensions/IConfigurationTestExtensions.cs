namespace SapAct.Tests.Extensions;

public static class IConfigurationTestExtensions
{
    public static ServiceBusTopicConfiguration GetIntTestsServiceBusConfig(this IConfiguration configuration)
    {
        var sbTopicConfigs = configuration.GetServiceBusTopicConfiguration();
        return sbTopicConfigs.First(c => c.TopicName.Contains("inttest", StringComparison.OrdinalIgnoreCase));
    }
}
