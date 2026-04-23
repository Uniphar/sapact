namespace SapAct.Extensions;

public static class IConfigurationExtensions
{
    extension(IConfiguration configuration)
    {
        internal void CheckConfiguration()
        {
            new ConfigurationValidator().Validate(configuration);
        }

        public IEnumerable<ServiceBusTopicConfiguration> GetServiceBusTopicConfiguration()
        {
            return configuration
                .GetSection(Consts.ServiceBusConfigurationSectionName)
                .GetChildren()
                .Select(section => new ServiceBusTopicConfiguration()
                {
                    ConnectionString = section[Consts.ServiceBusConnectionStringConfigKey]!,
                    TopicName = section[Consts.ServiceBusTopicNameConfigKey]!,
                    ADXSinkDisabled = section.GetADXSinkDisabled(),
                    LASinkDisabled = section.GetLASinkDisabled(),
                    SQLSinkDisabled = section.GetSQLSinkDisabled()
                })
                .ToList();
        }

        internal string GetLockServiceBlobConnectionString() => configuration[Consts.LockServiceBlobConnectionStringConfigKey] ?? throw new ArgumentNullException(Consts.LockServiceBlobConnectionStringConfigKey);
        public string GetLockServiceBlobContainerNameOrDefault() => configuration[Consts.LockServiceBlobContainerNameConfigKey] ?? "sapact";
        internal string GetLogAnalyticsEndpointName() => configuration[Consts.LogAnalyticsEndpointNameConfigKey] ?? throw new ArgumentNullException(Consts.LogAnalyticsEndpointNameConfigKey);
        public string GetTopicSubscriptionNameOrDefault<T>() => configuration[$"{Consts.ServiceBusTopicSubscriptionNamePrefixConfigKey}{typeof(T).Name}"] ?? $"SapAct{typeof(T).Name}";
        internal string GetLogAnalyticsSubscriptionId() => configuration[Consts.LogAnalyticsSubscriptionIdConfigKey] ?? throw new ArgumentNullException(Consts.LogAnalyticsSubscriptionIdConfigKey);
        internal string GetLogAnalyticsResourceGroupName() => configuration[Consts.LogAnalyticsResourceGroupConfigKey] ?? throw new ArgumentNullException(Consts.LogAnalyticsResourceGroupConfigKey);
        internal string GetLogAnalyticsWorkspaceName() => configuration[Consts.LogAnalyticsWorkspaceNameConfigKey] ?? throw new ArgumentNullException(Consts.LogAnalyticsWorkspaceNameConfigKey);
        public string GetLogAnalyticsWorkspaceId() => configuration[Consts.LogAnalyticsWorkspaceIdConfigKey] ?? throw new ArgumentNullException(Consts.LogAnalyticsWorkspaceIdConfigKey);
        internal string GetLogAnalyticsIngestionUrl() => configuration[Consts.LogAnalyticsIngestionUrlConfigKey] ?? throw new ArgumentNullException(Consts.LogAnalyticsIngestionUrlConfigKey);
        internal string GetADXClusterHostUrl() => configuration[Consts.ADXClusterHostUrlConfigKey] ?? throw new ArgumentNullException(Consts.ADXClusterHostUrlConfigKey);
        public string GetADXClusterDBNameOrDefault() => configuration[Consts.ADXClusterDBConfigKey] ?? "devops";
        internal string GetSQLConnectionString() => configuration[Consts.SQLConnectionStringConfigKey] ?? throw new ArgumentNullException(Consts.SQLConnectionStringConfigKey);
    }
}