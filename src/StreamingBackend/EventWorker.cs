namespace StreamingBackend;

public sealed class RuntimeMetrics
{
    private long _processingBatchFailures;
    public long ProcessingBatchFailures => Interlocked.Read(ref _processingBatchFailures);
    public void RecordProcessingBatchFailure() => Interlocked.Increment(ref _processingBatchFailures);
}

public sealed class EventWorker(Database database, RuntimeMetrics metrics, ILogger<EventWorker> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var count = await database.ProcessBatch(stoppingToken);
                if (count == 0) await Task.Delay(25, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { }
            catch (Exception ex)
            {
                metrics.RecordProcessingBatchFailure();
                logger.LogError(ex, "Event processing batch failed");
                await Task.Delay(500, stoppingToken);
            }
        }
    }
}
