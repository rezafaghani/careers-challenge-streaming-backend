using StreamingBackend;

var now = DateTimeOffset.Parse("2026-01-01T10:00:00Z");
var baseline = new DeviceEvent("dev", "room", "heartbeat", now, 1, null, null, null, null, null);

Valid(baseline);
Valid(baseline with { Type = "presence", InRoom = false });
Valid(baseline with { Type = "motion", Magnitude = 0 });
Valid(baseline with { Type = "motion", Magnitude = 1 });
Valid(baseline with { Type = "sleep_state", State = "asleep" });
Valid(baseline with { Type = "fall_warn", Confidence = .9 });
Valid(baseline with { Type = "net_status", Rssi = -70 });
Valid(baseline with { Ts = now.AddHours(-20) });
Valid(baseline with { Ts = now.AddHours(1) });

Invalid(baseline with { DeviceId = null });
Invalid(baseline with { RoomId = "" });
Invalid(baseline with { Seq = null });
Invalid(baseline with { Seq = -1 });
Invalid(baseline with { Ts = default });
Invalid(baseline with { Ts = now.AddHours(1).AddTicks(1) });
Invalid(baseline with { Type = "unknown" });
Invalid(baseline with { Type = "presence" });
Invalid(baseline with { Type = "motion", Magnitude = 1.01 });
Invalid(baseline with { Type = "sleep_state", State = "sleeping" });
Invalid(baseline with { Type = "fall_warn", Confidence = -.1 });
Invalid(baseline with { Type = "net_status" });

var hour = now.AddHours(1);
Near(Occupancy.Calculate(now, hour, false,
    [(now.AddMinutes(30), false), (now.AddMinutes(10), true)]), 20d / 60);
Near(Occupancy.Calculate(now, hour, true, []), 1);
Near(Occupancy.Calculate(now, hour, false, []), 0);
Near(Occupancy.Calculate(now, hour, false, [(now, true), (hour, false)]), 1);
Near(Occupancy.Calculate(now, hour, false,
    [(now.AddMinutes(-5), true), (now.AddMinutes(70), false)]), 1);
Near(Occupancy.Calculate(now, now, true, []), 0);

Near(Availability.Calculate(0), 0);
Near(Availability.Calculate(150), .5);
Near(Availability.Calculate(300), 1);
Near(Availability.Calculate(400), 1);

Console.WriteLine("All domain checks passed");

void Valid(DeviceEvent value)
{
    var error = EventRules.Validate(value, now);
    if (error is not null) throw new Exception($"Expected valid event: {error}");
}

void Invalid(DeviceEvent value)
{
    if (EventRules.Validate(value, now) is null) throw new Exception("Expected invalid event");
}

void Near(double actual, double expected)
{
    if (Math.Abs(actual - expected) > .0001) throw new Exception($"Expected {expected}, got {actual}");
}
