namespace SapAct.Extensions;

public static class StringExtensions
{
	public static string TranslateToKustoType(this string type) => type.ToLowerInvariant() switch
	{
		"string" => typeof(string).FullName!,
		"datetime" => typeof(DateTime).FullName!,
		_ => throw new ArgumentException($"Unsupported type: {type}")
	};
}
