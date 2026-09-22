using System.Text.Json.Serialization;

namespace StreamingBackend;

public sealed record DeviceEvent(
    [property: JsonPropertyName("device_id")] string? DeviceId,
    [property: JsonPropertyName("room_id")] string? RoomId,
    [property: JsonPropertyName("type")] string? Type,
    [property: JsonPropertyName("ts")] DateTimeOffset Ts,
    [property: JsonPropertyName("seq")] long? Seq,
    [property: JsonPropertyName("in_room")] bool? InRoom,
    [property: JsonPropertyName("magnitude")] double? Magnitude,
    [property: JsonPropertyName("state")] string? State,
    [property: JsonPropertyName("confidence")] double? Confidence,
    [property: JsonPropertyName("rssi")] int? Rssi);

public static class EventRules
{
    public static string? Validate(DeviceEvent e, DateTimeOffset now)
    {
        if (string.IsNullOrWhiteSpace(e.DeviceId) || string.IsNullOrWhiteSpace(e.RoomId)) return "device_id and room_id are required";
        if (e.Seq is null or < 0) return "seq is required and must be non-negative";
        if (e.Ts == default) return "ts is required";
        if (e.Ts > now.AddHours(1)) return "ts is more than one hour in the future";
        return e.Type switch
        {
            "heartbeat" => null,
            "presence" when e.InRoom is not null => null,
            "motion" when e.Magnitude is >= 0 and <= 1 => null,
            "sleep_state" when e.State is "asleep" or "awake" or "unknown" => null,
            "fall_warn" when e.Confidence is >= 0 and <= 1 => null,
            "net_status" when e.Rssi is not null => null,
            "presence" or "motion" or "sleep_state" or "fall_warn" or "net_status" => $"invalid {e.Type} payload",
            _ => "unknown event type"
        };
    }
}

public static class Availability
{
    public static double Calculate(long heartbeats) => Math.Clamp(heartbeats / 300d, 0, 1);
}

public static class Occupancy
{
    public static double Calculate(DateTimeOffset start, DateTimeOffset end, bool initial,
        IEnumerable<(DateTimeOffset Ts, bool InRoom)> transitions)
    {
        var occupied = TimeSpan.Zero;
        var cursor = start;
        var state = initial;
        foreach (var item in transitions.OrderBy(x => x.Ts))
        {
            var ts = item.Ts < start ? start : item.Ts > end ? end : item.Ts;
            if (state) occupied += ts - cursor;
            cursor = ts;
            state = item.InRoom;
        }
        if (state) occupied += end - cursor;
        return end == start ? 0 : Math.Clamp(occupied.TotalSeconds / (end - start).TotalSeconds, 0, 1);
    }
}
