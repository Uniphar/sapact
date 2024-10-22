namespace SapAct.Models;

public record ServiceBusTopicConfiguration
{
	public required string ConnectionString;
	public required string TopicName;
	public bool ADXSinkDisabled;
	public bool LASinkDisabled;
	public bool SQLSinkDisabled;
}
