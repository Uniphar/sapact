namespace SapAct.Models;

public class ColumnDefinition
{
	[JsonPropertyName("name")]
	public required string Name { get; set; }
	[JsonPropertyName("type")]
	public required string Type { get; set; }
}