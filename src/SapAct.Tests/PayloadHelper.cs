namespace SapAct.Tests;

public class PayloadHelper
{
	public const string ExtendedSchemaColumnName = "extendedSchemaColumn";

	public static string GetPayload(string objectType, string objectKey, string version, bool extendedSchema = false)
	{
		if (!extendedSchema)
			return $"[{{\"objectType\":\"{objectType}\",\"objectKey\":\"{objectKey}\", \"dataVersion\":\"{version}\"}}]";
		else
			return $"[{{\"objectType\":\"{objectType}\",\"objectKey\":\"{objectKey}\", \"dataVersion\":\"{version}\", \"{ExtendedSchemaColumnName}\":\"value\"}}]";
	}
}
