namespace SapAct.Extensions;

public static class JsonElementExtensions
{
	private const string DataColumnName = "data";

	public static List<ColumnDefinition> GenerateColumnList(this JsonElement payload, TargetStorageEnum targetStorage)
	{
		var propDefinitions = payload.EnumerateObject()
			.SelectMany(p => p.Name == "data" ? p.Value.EnumerateObject().ToArray() : [p])
			.Select(p => p.Name)
			.Distinct()
			.Select(name => new ColumnDefinition { Name = name, Type = "string" });

		if (targetStorage is TargetStorageEnum.LogAnalytics)
		{
			return propDefinitions
			  .Append(new ColumnDefinition { Name = "TimeGenerated", Type = "datetime" })
			  .ToList();
		}
		else
		{
			return propDefinitions.ToList();
		}		
	}

	public static bool TryGetDataProperty(this JsonElement payload, out JsonElement dataProperty) => payload.TryGetProperty(DataColumnName, out dataProperty);

	public static IEnumerable<JsonProperty> GetNonDataObjects(this JsonElement jsonElement) => jsonElement.EnumerateObject().Where(x => x.Name != DataColumnName);

	public static Dictionary<string, string> ExportToFlattenedDictionary(this JsonElement payload)
	{
		Dictionary<string, string> dataFields = [];

	
		//translate data fields
		if (payload.TryGetDataProperty(out var dataField))
		{
			foreach (var field in dataField.EnumerateObject())
			{
				dataFields.Add(field.Name, field.Value.ToString());
			}
		}

		//translate top level fields - potentially overwrite data fields - top level wins
		foreach (var field in payload.EnumerateObject().Where(x => x.Name != DataColumnName))
		{
			dataFields[field.Name] = field.Value.ToString();
		}

		return dataFields;
	}

	public static BinaryData GenerateBinaryData(this JsonElement jsonElement) =>
		BinaryData.FromObjectAsJson(new[] { jsonElement.ExportToFlattenedDictionary() });

	public static string ExportDCRImmutableId(this JsonElement jsonElement)
	{
		return jsonElement.GetProperty("properties").GetProperty("immutableId").GetString()!;
	}

	public static IEnumerable<JsonProperty> GetScalarProperties(this JsonElement element)
	{
		return element.EnumerateObject().Where(x => x.Value.ValueKind != JsonValueKind.Object && x.Value.ValueKind != JsonValueKind.Array);
	}

	public static IEnumerable<JsonProperty> GetNonScalarProperties(this JsonElement element)
	{
		return element.EnumerateObject().Where(x => x.Value.ValueKind == JsonValueKind.Object || x.Value.ValueKind == JsonValueKind.Array);
	}
}