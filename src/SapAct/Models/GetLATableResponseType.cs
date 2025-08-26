namespace SapAct.Models;

public record GetLATableResponseType
{
	[JsonPropertyName("properties")]
	public required LATablePropertiesType Properties { get; set; }

	public record LATablePropertiesType
	{
		[JsonPropertyName("schema")]
		public required LATableSchemaType Schema { get; set; }

		public record LATableSchemaType
		{
			[JsonPropertyName("columns")]
			public required IEnumerable<ColumnDefinition> Columns { get; set; }
		}
	}
}
