namespace SapAct.Extensions;

public static class EnumExtensions
{
	public static bool IsUpdateRequired(this SchemaCheckResultState schemaCheckResultState)
	{
		return schemaCheckResultState == SchemaCheckResultState.Older || schemaCheckResultState == SchemaCheckResultState.Unknown;
	}
}
