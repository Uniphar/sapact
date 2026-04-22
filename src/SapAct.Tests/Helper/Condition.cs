using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace SapAct.Tests.Helper;

internal static class Condition
{
    public static async Task WaitUntilAsync(Func<Task<bool>> condition, TimeSpan? timeout, [CallerArgumentExpression(nameof(condition))] string? conditionStr = default)
    {
        var delay = TimeSpan.FromSeconds(2);
        timeout ??= TimeSpan.FromSeconds(30);

        var stopwatch = Stopwatch.StartNew();
        while (stopwatch.Elapsed < timeout && !await condition()) await Task.Delay(delay);

        if (stopwatch.Elapsed < timeout) return;

        conditionStr = conditionStr?.Trim() ?? string.Empty;
        conditionStr = conditionStr.StartsWith("async () =>", StringComparison.Ordinal)
            ? conditionStr[11..].Trim()
            : conditionStr;
        conditionStr = conditionStr.StartsWith("await", StringComparison.Ordinal)
            ? conditionStr[5..].Trim()
            : conditionStr;

        throw new TimeoutException(
            $"Condition not met after {timeout.Value.TotalSeconds:0}s: {conditionStr}");
    }
}
