using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net.Http.Json;

var targets = (Environment.GetEnvironmentVariable("SERVICE_URLS")
    ?? Environment.GetEnvironmentVariable("SERVICE_URL")
    ?? "http://localhost:8080").Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
var total = int.TryParse(Environment.GetEnvironmentVariable("REQUESTS"), out var requested) ? requested : 50_000;
var concurrency = int.TryParse(Environment.GetEnvironmentVariable("CONCURRENCY"), out var parallelism) ? parallelism : 1_000;
var run = Guid.NewGuid().ToString("N")[..8];
var latencies = new ConcurrentBag<double>();
var failures = 0;
var clients = targets.Select(target => new HttpClient(new SocketsHttpHandler { MaxConnectionsPerServer = concurrency })
    { BaseAddress = new Uri(target), Timeout = TimeSpan.FromMinutes(2) }).ToArray();
using var clientScope = new ClientScope(clients);
var metricsClient = clients[0];

Console.WriteLine($"Sending {total:N0} events to {string.Join(',', targets)} with concurrency {concurrency:N0}");
var totalTimer = Stopwatch.StartNew();
await Parallel.ForEachAsync(Enumerable.Range(0, total), new ParallelOptions { MaxDegreeOfParallelism = concurrency }, async (i, ct) =>
{
    var timer = Stopwatch.StartNew();
    var fall = i % 10_000 == 0;
    var payload = new
    {
        device_id = $"load-{run}-{i % 5_000:D4}",
        room_id = $"load-room-{i % 2_500:D4}",
        type = fall ? "fall_warn" : "heartbeat",
        ts = DateTimeOffset.UtcNow,
        seq = i / 5_000,
        confidence = fall ? .95 : (double?)null
    };
    try
    {
        using var response = await clients[i % clients.Length].PostAsJsonAsync("/events", payload, ct);
        if (!response.IsSuccessStatusCode) Interlocked.Increment(ref failures);
    }
    catch (Exception)
    {
        Interlocked.Increment(ref failures);
    }
    latencies.Add(timer.Elapsed.TotalMilliseconds);
});
totalTimer.Stop();

var ordered = latencies.Order().ToArray();
Console.WriteLine($"requests_per_second {total / totalTimer.Elapsed.TotalSeconds:F1}");
Console.WriteLine($"http_failures {failures}");
Console.WriteLine($"client_latency_p50_ms {Percentile(ordered, .50):F1}");
Console.WriteLine($"client_latency_p95_ms {Percentile(ordered, .95):F1}");

for (var i = 0; i < 1_200; i++)
{
    try
    {
        var metrics = await metricsClient.GetStringAsync("/metrics");
        if (metrics.Contains("events_pending 0\n", StringComparison.Ordinal)) break;
    }
    catch (HttpRequestException) { }
    await Task.Delay(100);
}

Console.WriteLine(await metricsClient.GetStringAsync("/metrics"));

static double Percentile(double[] values, double percentile) => values.Length == 0 ? 0 : values[(int)Math.Ceiling(values.Length * percentile) - 1];

sealed class ClientScope(HttpClient[] clients) : IDisposable
{
    public void Dispose()
    {
        foreach (var client in clients) client.Dispose();
    }
}
