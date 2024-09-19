namespace SapAct.Extensions;

public static class JsonElementExtensions
{
	public static List<ColumnDefinition> GenerateColumnList(this JsonElement payload)
	{
		List<ColumnDefinition> columnsList =
			[
				new() { Name = "TimeGenerated", Type = "datetime" } //compulsory field
			];

		foreach (var property in payload.EnumerateObject())
		{
			var column = new ColumnDefinition
			{
				Name = property.Name,
				Type = "string"
			};

			columnsList.Add(column);
		}

		//translate data fields
		if (payload.TryGetProperty("data", out var dataField))
		{
			foreach (var field in dataField.EnumerateObject())
			{
				var column = new ColumnDefinition
				{
					Name = field.Name,
					Type = "string"
				};

				columnsList.Add(column);
			}
		}

		return columnsList;
	}
	
	public static Dictionary<string, string> ExportToFlattenedDictionary(this JsonElement payload)
	{
		Dictionary<string, string> dataFields = [];

		//translate top level fields
		foreach (var field in payload.EnumerateObject().Where(x => x.Name != "data"))
		{
			dataFields.Add(field.Name, field.Value.ToString());
		}

		//translate data fields
		if (payload.TryGetProperty("data", out var dataField))
		{
			foreach (var field in dataField.EnumerateObject())
			{
				dataFields.Add(field.Name, field.Value.ToString());
			}
		}

		return dataFields;
	}

	public static string ExportDCRImmutableId(this JsonElement jsonElement)
	{
		return jsonElement.GetProperty("properties").GetProperty("immutableId").GetString()!;
	}
}