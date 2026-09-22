using System.Threading.Channels;

namespace StreamingBackend;

public sealed class IngestionQueue(Database database, ILogger<IngestionQueue> logger) : BackgroundService
{
    private readonly Channel<PendingEvent> _channel = Channel.CreateBounded<PendingEvent>(new BoundedChannelOptions(100_000)
    {
        FullMode = BoundedChannelFullMode.Wait,
        SingleReader = true
    });

    public async Task Store(DeviceEvent value, CancellationToken ct)
    {
        var pending = new PendingEvent(value);
        await _channel.Writer.WriteAsync(pending, ct);
        await pending.Stored.Task.WaitAsync(ct);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var batch = new List<PendingEvent>(1_000);
        while (await _channel.Reader.WaitToReadAsync(stoppingToken))
        {
            batch.Clear();
            batch.Add(await _channel.Reader.ReadAsync(stoppingToken));
            await Task.Delay(2, stoppingToken);
            while (batch.Count < batch.Capacity && _channel.Reader.TryRead(out var item)) batch.Add(item);

            try
            {
                await database.StoreEvents(batch.Select(x => x.Value), stoppingToken);
                foreach (var item in batch) item.Stored.TrySetResult();
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Durable ingestion batch failed");
                foreach (var item in batch) item.Stored.TrySetException(ex);
            }
        }
    }

    private sealed class PendingEvent(DeviceEvent value)
    {
        public DeviceEvent Value { get; } = value;
        public TaskCompletionSource Stored { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }
}
