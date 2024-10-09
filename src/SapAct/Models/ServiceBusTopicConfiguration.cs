namespace SapAct.Models;

public record ServiceBusTopicConfiguration
{
	public required string ConnectionString;
	public required string TopicName;
}
