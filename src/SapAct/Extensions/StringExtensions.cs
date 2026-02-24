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

        // Replace hyphens and other special characters (except underscores) with underscores
        var normalized = new string(
            input
                .Select(c => (char.IsLetterOrDigit(c) || c == '_') ? c : '_')
                .ToArray()
        );

        // Remove double underscores until there are none left
        while (normalized.Contains("__"))
            normalized = normalized.Replace("__", "_");

        // Strip leading and trailing underscores
        normalized = normalized.Trim('_');

        if (normalized.Length == 0 || !char.IsLetter(normalized[0]))
            throw new ArgumentException("Table name must start with a letter", nameof(input));

        if (normalized.Length > 45)
            throw new ArgumentException("Table name must not exceed 45 characters (excluding _CL suffix).", nameof(input));

        // Capitalize first letter if it's lowercase
        return normalized;
    }

    public static string TranslateToKustoType(this string type) =>
        type.ToLowerInvariant() switch
        {
            "string" => typeof(string).FullName!,
            "datetime" => typeof(DateTime).FullName!,
            _ => throw new ArgumentException($"Unsupported type: {type}")
        };
}