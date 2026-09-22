using System.Text.Json;
using System.Text.Json.Serialization;
using Npgsql;
using NpgsqlTypes;

namespace StreamingBackend;

public sealed record HealthSnapshot(DateTimeOffset? LastHeartbeat, long RecentHeartbeats);
public sealed record PresenceTransition(DateTimeOffset Ts, bool InRoom);
public sealed record Alarm(
    [property: JsonPropertyName("event_id")] long EventId,
    [property: JsonPropertyName("room_id")] string RoomId,
    [property: JsonPropertyName("ts")] DateTimeOffset Ts,
    [property: JsonPropertyName("confidence")] double Confidence,
    [property: JsonPropertyName("device_id")] string DeviceId);
public sealed record StorageMetrics(long Received, long Processed, long Pending, decimal OldestBacklogSeconds,
    double ProcessingP50, double ProcessingP95, long DeduplicatedFalls, double AlarmP50, double AlarmP95);

public sealed class Database
{
    private readonly string _connectionString;

    public Database(IConfiguration configuration) => _connectionString =
        configuration.GetConnectionString("postgres")
        ?? "Host=localhost;Database=streaming;Username=streaming;Password=streaming;Pooling=true;Maximum Pool Size=200";

    public async Task Initialize(CancellationToken ct = default)
    {
        await using var connection = await Open(ct);
        await using var command = new NpgsqlCommand(Schema, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    public async Task StoreEvent(DeviceEvent e, CancellationToken ct)
    {
        await using var connection = await Open(ct);
        await using var command = new NpgsqlCommand("""
            INSERT INTO event_inbox(device_id,room_id,type,event_ts,seq,payload)
            VALUES (@device,@room,@type,@ts,@seq,@payload)
            ON CONFLICT (device_id,seq) DO NOTHING
            """, connection);
        command.Parameters.AddWithValue("device", e.DeviceId!);
        command.Parameters.AddWithValue("room", e.RoomId!);
        command.Parameters.AddWithValue("type", e.Type!);
        command.Parameters.AddWithValue("ts", e.Ts);
        command.Parameters.AddWithValue("seq", e.Seq!.Value);
        command.Parameters.AddWithValue("payload", NpgsqlDbType.Jsonb, JsonSerializer.Serialize(e));
        await command.ExecuteNonQueryAsync(ct);
    }

    public async Task<int> ProcessBatch(CancellationToken ct)
    {
        await using var connection = await Open(ct);
        await using var transaction = await connection.BeginTransactionAsync(ct);
        await using var command = new NpgsqlCommand(ProcessBatchSql, connection, transaction);
        var count = await command.ExecuteNonQueryAsync(ct);
        await transaction.CommitAsync(ct);
        return count;
    }

    public async Task<HealthSnapshot> GetHealth(string deviceId, CancellationToken ct)
    {
        await using var connection = await Open(ct);
        await using var command = new NpgsqlCommand("""
            SELECT max(event_ts), count(*) FILTER (WHERE event_ts >= now() - interval '5 minutes' AND event_ts <= now())
            FROM heartbeats WHERE device_id=@device
            """, connection);
        command.Parameters.AddWithValue("device", deviceId);
        await using var reader = await command.ExecuteReaderAsync(ct);
        await reader.ReadAsync(ct);
        return new(reader.IsDBNull(0) ? null : reader.GetFieldValue<DateTimeOffset>(0), reader.GetInt64(1));
    }

    public async Task<List<PresenceTransition>> GetPresence(string roomId, DateTimeOffset start, DateTimeOffset end, CancellationToken ct)
    {
        await using var connection = await Open(ct);
        await using var command = new NpgsqlCommand("""
            (SELECT event_ts,in_room FROM presence_transitions
             WHERE room_id=@room AND event_ts<@start ORDER BY event_ts DESC LIMIT 1)
            UNION ALL
            (SELECT event_ts,in_room FROM presence_transitions
             WHERE room_id=@room AND event_ts>=@start AND event_ts<=@end)
            """, connection);
        command.Parameters.AddWithValue("room", roomId);
        command.Parameters.AddWithValue("start", start);
        command.Parameters.AddWithValue("end", end);
        var rows = new List<PresenceTransition>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct)) rows.Add(new(reader.GetFieldValue<DateTimeOffset>(0), reader.GetBoolean(1)));
        rows.Sort((a, b) => a.Ts.CompareTo(b.Ts));
        return rows;
    }

    public async Task<List<Alarm>> GetAlarms(DateTimeOffset since, CancellationToken ct)
    {
        await using var connection = await Open(ct);
        await using var command = new NpgsqlCommand("SELECT event_id,room_id,event_ts,confidence,device_id FROM alarms WHERE event_ts>@since ORDER BY event_ts,event_id", connection);
        command.Parameters.AddWithValue("since", since);
        var alarms = new List<Alarm>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct)) alarms.Add(new(reader.GetInt64(0), reader.GetString(1),
            reader.GetFieldValue<DateTimeOffset>(2), reader.GetDouble(3), reader.GetString(4)));
        return alarms;
    }

    public async Task<StorageMetrics> GetMetrics(CancellationToken ct)
    {
        await using var connection = await Open(ct);
        await using var command = new NpgsqlCommand(MetricsSql, connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        await reader.ReadAsync(ct);
        return new(reader.GetInt64(0), reader.GetInt64(1), reader.GetInt64(2), reader.GetDecimal(3),
            reader.GetDouble(4), reader.GetDouble(5), reader.GetInt64(6), reader.GetDouble(7), reader.GetDouble(8));
    }

    private async Task<NpgsqlConnection> Open(CancellationToken ct)
    {
        var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync(ct);
        return connection;
    }

    private const string ProcessBatchSql = """
        WITH claimed AS MATERIALIZED (
          SELECT * FROM event_inbox WHERE processed_at IS NULL
          ORDER BY CASE WHEN type='fall_warn' THEN 0 ELSE 1 END, id LIMIT 500 FOR UPDATE SKIP LOCKED
        ), heartbeats_written AS (
          INSERT INTO heartbeats(device_id,event_ts,event_id)
          SELECT device_id,event_ts,id FROM claimed WHERE type='heartbeat' ON CONFLICT DO NOTHING
        ), presence_written AS (
          INSERT INTO presence_transitions(room_id,event_ts,event_id,in_room)
          SELECT room_id,event_ts,id,(payload->>'in_room')::boolean FROM claimed WHERE type='presence' ON CONFLICT DO NOTHING
        ), alarms_written AS (
          INSERT INTO alarms(event_id,device_id,room_id,event_ts,confidence)
          SELECT id,device_id,room_id,event_ts,(payload->>'confidence')::double precision FROM claimed WHERE type='fall_warn'
          ON CONFLICT (device_id,event_ts) DO NOTHING
        )
        UPDATE event_inbox e SET processed_at=now(),attempts=e.attempts+1 FROM claimed WHERE e.id=claimed.id
        """;

    private const string MetricsSql = """
        SELECT count(*), count(*) FILTER (WHERE processed_at IS NOT NULL), count(*) FILTER (WHERE processed_at IS NULL),
          coalesce(max(extract(epoch FROM now()-received_at)) FILTER (WHERE processed_at IS NULL), 0),
          coalesce(percentile_cont(.50) WITHIN GROUP (ORDER BY extract(epoch FROM processed_at-received_at)) FILTER (WHERE processed_at IS NOT NULL), 0),
          coalesce(percentile_cont(.95) WITHIN GROUP (ORDER BY extract(epoch FROM processed_at-received_at)) FILTER (WHERE processed_at IS NOT NULL), 0),
          (SELECT count(*) FROM event_inbox WHERE type='fall_warn' AND processed_at IS NOT NULL) - (SELECT count(*) FROM alarms),
          coalesce((SELECT percentile_cont(.50) WITHIN GROUP (ORDER BY extract(epoch FROM a.created_at-e.received_at)) FROM alarms a JOIN event_inbox e ON e.id=a.event_id), 0),
          coalesce((SELECT percentile_cont(.95) WITHIN GROUP (ORDER BY extract(epoch FROM a.created_at-e.received_at)) FROM alarms a JOIN event_inbox e ON e.id=a.event_id), 0)
        FROM event_inbox
        """;

    private const string Schema = """
        CREATE TABLE IF NOT EXISTS event_inbox (
          id bigserial PRIMARY KEY, device_id text NOT NULL, room_id text NOT NULL, type text NOT NULL,
          event_ts timestamptz NOT NULL, seq bigint NOT NULL, payload jsonb NOT NULL,
          received_at timestamptz NOT NULL DEFAULT now(), processed_at timestamptz,
          attempts integer NOT NULL DEFAULT 0, last_error text, UNIQUE (device_id, seq)
        );
        DROP INDEX IF EXISTS event_inbox_pending;
        CREATE INDEX IF NOT EXISTS event_inbox_pending_priority ON event_inbox ((CASE WHEN type='fall_warn' THEN 0 ELSE 1 END), id) WHERE processed_at IS NULL;
        CREATE TABLE IF NOT EXISTS heartbeats (device_id text NOT NULL, event_ts timestamptz NOT NULL, event_id bigint NOT NULL UNIQUE, PRIMARY KEY (device_id, event_ts));
        CREATE INDEX IF NOT EXISTS heartbeats_recent ON heartbeats (device_id, event_ts DESC);
        CREATE TABLE IF NOT EXISTS presence_transitions (room_id text NOT NULL, event_ts timestamptz NOT NULL, event_id bigint NOT NULL UNIQUE, in_room boolean NOT NULL, PRIMARY KEY (room_id, event_ts, event_id));
        CREATE INDEX IF NOT EXISTS presence_room_time ON presence_transitions (room_id, event_ts DESC);
        CREATE TABLE IF NOT EXISTS alarms (event_id bigint PRIMARY KEY, device_id text NOT NULL, room_id text NOT NULL, event_ts timestamptz NOT NULL, confidence double precision NOT NULL, created_at timestamptz NOT NULL DEFAULT now(), UNIQUE (device_id, event_ts));
        CREATE INDEX IF NOT EXISTS alarms_time ON alarms (event_ts, event_id);
        """;
}
