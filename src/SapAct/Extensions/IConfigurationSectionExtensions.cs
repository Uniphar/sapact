namespace SapAct.Extensions;

public static class IConfigurationSectionExtensions
{
	public static bool GetADXSinkDisabled(this IConfigurationSection section)
	{
		return section.GetBoolConfig(Consts.ServiceBusTopicADXSinkDisabledConfigKey);
	}

	public static bool GetLASinkDisabled(this IConfigurationSection section)
	{
		return section.GetBoolConfig(Consts.ServiceBusTopicLASinkDisabledConfigKey);
	}

	public static bool GetSQLSinkDisabled(this IConfigurationSection section)
	{
		return section.GetBoolConfig(Consts.ServiceBusTopicSQLSinkDisabledConfigKey);
	}

	public static bool GetBoolConfig(this IConfigurationSection section, string configKey)
	{
		string? value = section[configKey];
		return !string.IsNullOrEmpty(value) && value.Equals(bool.TrueString, StringComparison.OrdinalIgnoreCase);
	}
}
