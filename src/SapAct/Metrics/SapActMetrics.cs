using System.Diagnostics.Metrics;

namespace SapAct.Metrics;

public class SapActMetrics : IDisposable
{
    // has to be same as appPathPrefix in Program.cs
    public const string Name = "sapact";
    public const string EventHubEventsProcessedMetricName = "sapact.SapActMessageIngestion";
    private readonly Meter _meter;

    private readonly Counter<int> _sapActMessageIngestionCounter;

    public SapActMetrics(IMeterFactory meterFactory)
    {
        _meter = meterFactory.Create(Name);

        _sapActMessageIngestionCounter = _meter.CreateCounter<int>(EventHubEventsProcessedMetricName,
            "counter",
            "Number of Sap Act messages Ingested");
    }

    public void Dispose() => _meter.Dispose();


    internal void TrackMetricIngestion(string name, string messageId, string workerName)
    {
        _sapActMessageIngestionCounter.Add(1,
        [
            new(Consts.TelemetrySinkTypeDimensionName, name),
            new(Consts.TelemetryMessageIdDimensionName, messageId),
            new(Consts.TelemetryWorkerNameDimensionName, workerName)
        ]);
    }
}