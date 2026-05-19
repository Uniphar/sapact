namespace SapAct.Extensions;

public static class IConfigurationSectionExtensions
{
    extension(IConfigurationSection section)
    {
        internal bool GetADXSinkDisabled() => section.GetBoolConfig(Consts.ServiceBusTopicADXSinkDisabledConfigKey);
        internal bool GetLASinkDisabled() => section.GetBoolConfig(Consts.ServiceBusTopicLASinkDisabledConfigKey);
        internal bool GetSQLSinkDisabled() => section.GetBoolConfig(Consts.ServiceBusTopicSQLSinkDisabledConfigKey);

        private bool GetBoolConfig(string configKey)
        {
            var value = section[configKey];
            return !string.IsNullOrEmpty(value) && value.Equals(bool.TrueString, StringComparison.OrdinalIgnoreCase);
        }
    }
}