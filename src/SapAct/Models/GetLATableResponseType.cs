namespace SapAct.Models;

public class GetLATableResponseType
{
	[JsonPropertyName("properties")]
	public required LATablePropertiesType Properties { get; set; }

	public class LATablePropertiesType
	{
		[JsonPropertyName("schema")]
		public required LATableSchemaType Schema { get; set; }

		public class LATableSchemaType
		{
			[JsonPropertyName("columns")]
			public required IEnumerable<ColumnDefinition> Columns { get; set; }
		}
	}
}
