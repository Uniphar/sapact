namespace SapAct.Extensions;

/// <summary>
/// Log Analytics custom table name rules:
/// - Must start with a letter (A-Z, a-z)
/// - Can only contain letters (A-Z, a-z), digits (0-9), and underscores (_)
/// - No hyphens or other special characters
/// - Maximum 45 characters for the base name (excluding the _CL suffix)
/// - The _CL suffix is added separately by GetTableName
/// - Total table name (including _CL) must not exceed 63 characters
/// </summary>
public static class StringExtensions
{
    public static string MakeTableFriendly(this string input)
    {
        if (string.IsNullOrWhiteSpace(input))
            throw new ArgumentException("Input cannot be null or empty.", nameof(input));

        var result = string.Concat(
            // some common chars that can be used to split
            input.Split(['-', '_', '.', '@'], StringSplitOptions.RemoveEmptyEntries)
                .Select(segment =>
                {
                    var cleaned = new string(segment.Where(char.IsLetterOrDigit).ToArray());
                    if (cleaned.Length == 0) return "";
                    return char.ToUpperInvariant(cleaned[0]) + cleaned[1..];
                })
        );

        if (result.Length == 0 || !char.IsLetter(result[0]))
            throw new ArgumentException("Table name must start with a letter after sanitization.", nameof(input));

        if (result.Length > 45)
            throw new ArgumentException("Table name must not exceed 45 characters (excluding _CL suffix).", nameof(input));

        return result;
    }

    public static string TranslateToKustoType(this string type) =>
        type.ToLowerInvariant() switch
        {
            "string" => typeof(string).FullName!,
            "datetime" => typeof(DateTime).FullName!,
            _ => throw new ArgumentException($"Unsupported type: {type}")
        };
}