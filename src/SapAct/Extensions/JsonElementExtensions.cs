namespace SapAct.Extensions;

public static class JsonElementExtensions
{
	private const string DataColumnName = "data";

	public static List<ColumnDefinition> GenerateColumnList(this JsonElement payload, TargetStorageEnum targetStorage)
	{
		List<ColumnDefinition> columnsList = [];

		if (targetStorage==TargetStorageEnum.LogAnalytics)
			columnsList.Add(new ColumnDefinition { Name = "TimeGenerated", Type = "datetime" });

		foreach (var property in payload.EnumerateObject().Where(x=>x.Name!=DataColumnName))
		{
			var column = new ColumnDefinition
			{
				Name = property.Name,
				Type = "string"
			};

			columnsList.Add(column);
		}

		//translate data fields
		if (payload.TryGetDataProperty(out var dataField))
		{
			foreach (var field in dataField.EnumerateObject())
			{
				var column = new ColumnDefinition
				{
					Name = field.Name,
					Type = "string"
				};

				if (columnsList.Any(x => x.Name == column.Name))
					continue;

				columnsList.Add(column);
			}
		}

		return columnsList;
	}

	public static bool TryGetDataProperty(this JsonElement payload, out JsonElement dataProperty) => payload.TryGetProperty(DataColumnName, out dataProperty);

	public static IEnumerable<JsonProperty> GetNonDataObjects(this JsonElement jsonElement) => jsonElement.EnumerateObject().Where(x => x.Name != DataColumnName);

	public static Dictionary<string, string> ExportToFlattenedDictionary(this JsonElement payload)
	{
		Dictionary<string, string> dataFields = [];

		//translate top level fields
		foreach (var field in payload.EnumerateObject().Where(x => x.Name != DataColumnName))
		{
			dataFields.Add(field.Name, field.Value.ToString());
		}

		//translate data fields
		if (payload.TryGetDataProperty(out var dataField))
		{
			foreach (var field in dataField.EnumerateObject())
			{
				if (!dataFields.ContainsKey(field.Name))
					dataFields.Add(field.Name, field.Value.ToString());
			}
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