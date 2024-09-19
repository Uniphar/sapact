namespace SapAct.Services;

public abstract class VersionedSchemaBaseService
{
	private readonly ConcurrentDictionary<string, string> _tableVersionMapping = new();

	protected SchemaCheckResultState CheckObjectTypeSchema(string objectType, string version)
	{
		bool found = _tableVersionMapping.TryGetValue(objectType, out string? schemaVersion);

		if (!found)
		{
			return SchemaCheckResultState.Unknown;
		}
		else
		{
			var schemaCompareResult = string.Compare(version, schemaVersion) switch
			{
				> 0 => SchemaCheckResultState.Older,
				0 => SchemaCheckResultState.Current,
				< 0 => SchemaCheckResultState.Current,
			};

			return schemaCompareResult;
		}
	}

	protected void UpdateObjectTypeSchema(string objectType, string version)
	{
		_tableVersionMapping.AddOrUpdate(objectType, version, (key, oldValue) => version);
	}
}
