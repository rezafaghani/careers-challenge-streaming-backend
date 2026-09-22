using System.Text.Json;
using Scalar.AspNetCore;
using StreamingBackend;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddSingleton<Database>();
builder.Services.AddSingleton<RuntimeMetrics>();
builder.Services.AddSingleton<IngestionQueue>();
builder.Services.AddHostedService(service => service.GetRequiredService<IngestionQueue>());
builder.Services.AddHostedService<EventWorker>();
builder.Services.AddOpenApi();
var app = builder.Build();
var database = app.Services.GetRequiredService<Database>();
var ingestion = app.Services.GetRequiredService<IngestionQueue>();
await database.Initialize();

app.MapOpenApi();
app.MapScalarApiReference();
app.MapGet("/", () => Results.Redirect("/scalar/v1")).ExcludeFromDescription();
app.MapGet("/favicon.ico", () => Results.NoContent()).ExcludeFromDescription();

app.MapPost("/events", async (HttpRequest request, CancellationToken ct) =>
{
    DeviceEvent? e;
    try { e = await request.ReadFromJsonAsync<DeviceEvent>(cancellationToken: ct); }
    catch (JsonException) { return Results.BadRequest(new { error = "invalid json" }); }
    if (e is null) return Results.BadRequest(new { error = "invalid json" });
    var error = EventRules.Validate(e, DateTimeOffset.UtcNow);
    if (error is not null) return Results.BadRequest(new { error });
    await ingestion.Store(e, ct);
    return Results.Accepted(value: new { ok = true });
});

app.MapGet("/devices/{deviceId}/health", async (string deviceId, CancellationToken ct) =>
{
    var health = await database.GetHealth(deviceId, ct);
    return Results.Ok(new
    {
        last_heartbeat_ts = health.LastHeartbeat,
        availability_5m = Availability.Calculate(health.RecentHeartbeats)
    });
});

app.MapGet("/rooms/{roomId}/occupancy", async (string roomId, string? window, CancellationToken ct) =>
{
    var seconds = window switch { "1m" => 60, null or "5m" => 300, "1h" => 3600, _ => 0 };
    if (seconds == 0) return Results.BadRequest(new { error = "window must be 1m, 5m, or 1h" });
    var end = DateTimeOffset.UtcNow;
    var start = end.AddSeconds(-seconds);
    var rows = await database.GetPresence(roomId, start, end, ct);
    var initial = rows.LastOrDefault(x => x.Ts < start)?.InRoom ?? false;
    var transitions = rows.Where(x => x.Ts >= start).Select(x => (x.Ts, x.InRoom));
    return Results.Ok(new
    {
        in_room = rows.Count > 0 && rows[^1].InRoom,
        occupied_pct = Occupancy.Calculate(start, end, initial, transitions),
        window_seconds = seconds
    });
});

app.MapGet("/alarms", async (string? since, CancellationToken ct) =>
{
    DateTimeOffset threshold;
    if (string.IsNullOrEmpty(since) || since == "0") threshold = DateTimeOffset.MinValue;
    else if (!DateTimeOffset.TryParse(since, out threshold)) return Results.BadRequest(new { error = "since must be 0 or an ISO-8601 timestamp" });
    return Results.Ok(new { alarms = await database.GetAlarms(threshold, ct) });
});

app.MapGet("/alarms/stream", async (HttpContext context, string? since) =>
{
    DateTimeOffset threshold;
    if (string.IsNullOrEmpty(since) || since == "0") threshold = DateTimeOffset.UtcNow;
    else if (!DateTimeOffset.TryParse(since, out threshold))
    {
        context.Response.StatusCode = StatusCodes.Status400BadRequest;
        return;
    }

    context.Response.Headers.ContentType = "text/event-stream";
    context.Response.Headers.CacheControl = "no-cache";
    context.Response.Headers.Connection = "keep-alive";
    var cursor = long.TryParse(context.Request.Headers["Last-Event-ID"], out var lastId) ? lastId : 0;
    var initial = cursor > 0 ? await database.GetAlarmsAfter(cursor, context.RequestAborted) : await database.GetAlarms(threshold, context.RequestAborted);
    await Send(initial);

    try
    {
        while (!context.RequestAborted.IsCancellationRequested)
        {
            await Task.Delay(100, context.RequestAborted);
            await Send(await database.GetAlarmsAfter(cursor, context.RequestAborted));
        }
    }
    catch (OperationCanceledException) when (context.RequestAborted.IsCancellationRequested) { }

    async Task Send(IEnumerable<Alarm> alarms)
    {
        foreach (var alarm in alarms)
        {
            cursor = Math.Max(cursor, alarm.EventId);
            await context.Response.WriteAsync($"id: {alarm.EventId}\nevent: fall_warn\ndata: {JsonSerializer.Serialize(alarm)}\n\n", context.RequestAborted);
        }
        await context.Response.Body.FlushAsync(context.RequestAborted);
    }
});

app.MapGet("/metrics", async (RuntimeMetrics runtime, CancellationToken ct) =>
{
    var metrics = await database.GetMetrics(ct);
    return Results.Text($"""
        events_received_total {metrics.Received}
        events_processed_total {metrics.Processed}
        events_pending {metrics.Pending}
        processing_batch_failures_total {runtime.ProcessingBatchFailures}
        backlog_oldest_seconds {metrics.OldestBacklogSeconds:0.000000}
        processing_latency_p50_seconds {metrics.ProcessingP50:0.000000}
        processing_latency_p95_seconds {metrics.ProcessingP95:0.000000}
        fall_events_deduplicated_total {metrics.DeduplicatedFalls}
        alarm_latency_p50_seconds {metrics.AlarmP50:0.000000}
        alarm_latency_p95_seconds {metrics.AlarmP95:0.000000}
        """ + "\n", "text/plain; version=0.0.4");
});

app.Run();
