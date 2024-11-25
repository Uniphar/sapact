namespace SapAct.Models;

public record MessageRootProperties
{
	public required string objectKey;
	public required string objectType;
	public required string dataVersion;
	public string? eventType;
}
