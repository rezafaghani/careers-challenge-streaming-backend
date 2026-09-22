# Teton Challenge, Real-time Streaming Backend

This fork contains a small durable implementation of the challenge in ASP.NET Core and PostgreSQL. The original challenge specification follows after the solution notes below.

## Run

Requirements: Docker with Compose, or Podman Compose.

```bash
docker compose up --build
curl http://localhost:8080/metrics
```

The API is exposed on `http://localhost:8080`. PostgreSQL is private to the Compose network and its named volume survives application restarts.

PostgreSQL is also available from the host at `localhost:5432` with database/user/password `streaming`. Override the host port when needed:

```bash
POSTGRES_PORT=5433 docker compose up --build
psql 'postgresql://streaming:streaming@localhost:5432/streaming'
```

Interactive API documentation is available at `http://localhost:8080/scalar/v1`; the OpenAPI document is at `/openapi/v1.json`.

## Design

`POST /events` validates an event and inserts it into a PostgreSQL inbox with a unique `(device_id, seq)` constraint before returning `202`. A background service claims batches with `FOR UPDATE SKIP LOCKED`, processing fall warnings before ordinary events. The same transaction writes the read model and marks inbox rows processed. A crash rolls that transaction back, so restart simply retries the row. Unique source-event keys make retries harmless.

PostgreSQL is both the durable queue and read store. This avoids an extra broker and any unbounded in-process buffer. Orleans was considered but omitted because database constraints and row claims already provide recoverable ownership without a second state model.

The application is stateless and horizontally scalable. Every replica can ingest and process; workers claim disjoint 5,000-row batches with `FOR UPDATE SKIP LOCKED`, and each entire batch is applied with one set-based PostgreSQL statement. Put replicas behind an HTTP load balancer and point them at the same PostgreSQL database. Unique constraints make concurrent/retried processing harmless. PostgreSQL is the intentional coordination point; size its connection limit and storage throughput before raising replica count. Compose remains one app plus one database for the simplest local evaluator setup.

All timestamps use `DateTimeOffset`/`timestamptz`. Presence transitions are stored in event-time order. Occupancy queries find the state immediately before the requested window, merge transitions inside it, and integrate occupied time; a late transition therefore corrects historical windows. Device health similarly uses event-time heartbeat rows.

A physical fall is identified by `(device_id, ts)`. This matches the supplied generator, whose 1–3 jitter messages have different sequence numbers but the same timestamp. Every transport event remains in the inbox, while the alarms table keeps one logical alarm with its original timestamp. `GET /alarms?since=...` is the durable reconnect path.

## API

```text
POST /events
GET  /devices/{device_id}/health
GET  /rooms/{room_id}/occupancy?window=1m|5m|1h
GET  /alarms?since=0|<ISO-8601 timestamp>
GET  /alarms/stream?since=<ISO-8601 timestamp>  (SSE)
GET  /metrics
```

The SSE alarm stream emits only alarms already committed to PostgreSQL, polls at 100 ms, and uses each alarm's durable event ID as the SSE `id`. Browsers reconnect with `Last-Event-ID`, allowing the stream to resume without missing committed alarms. `/alarms?since=` remains the durable catch-up API.

`/metrics` exposes durable Prometheus-style counters and gauges, including backlog age, deduplicated falls, processing latency p50/p95, and alarm latency p50/p95. Latency is calculated from PostgreSQL timestamps, so it survives restarts and reflects the full retained dataset:

```bash
curl http://localhost:8080/metrics
watch -n 1 'curl -s http://localhost:8080/metrics'
```

`alarm_latency_p95_seconds` is the challenge-critical p95 from durable receipt to alarm persistence. `processing_latency_p95_seconds` covers all processed event types; `events_pending` and `backlog_oldest_seconds` show whether a burst is draining.

## Verify

```bash
make test
make smoke
make baseline
make burst
make offline
make adversarial

# Configurable burst summary (defaults: 500 devices, 60 seconds)
DEVICES=1000 DURATION=120 make stress

# 50,000 total requests, 1,000 in flight by default
make load-50k
CONCURRENCY=2000 REQUESTS=100000 make load-50k

# Requires the Compose service to be running
make restart-check
```

`make test` treats compiler/analyzer warnings as errors (including unused `using` directives) and checks every event type's valid/invalid payload rules, required and future timestamps, out-of-order occupancy, boundary handling, and availability clamping. PostgreSQL behavior, API response shapes, deduplication, backlog drain, and restart recovery are exercised through the supplied generator/evaluator workflows above.

Correctness, scenario, stress, and restart workflows call the repository's existing `event_generator/generate.py`. The separate `load-50k` command exists only to create concurrency the synchronous supplied generator cannot produce. Use `events_pending`, `backlog_oldest_seconds`, and the p95 latency metrics to decide when PostgreSQL or replica capacity needs increasing.

### Concurrent load result

Run against a clean database:

```bash
docker compose down -v
docker compose up -d --build
CONCURRENCY=5000 REQUESTS=100000 make load-50k
```

The final local single-node result was 100,000/100,000 accepted and processed, 0 failures, 14,775 requests/second, client p50/p95 303/491 ms, processing p50/p95 99/237 ms, alarm persistence p50/p95 82/208 ms, and zero final backlog. This does **not** prove the 50,000 events/second target; three local app replicas still reached only about 15,700 requests/second because they shared the same local PostgreSQL and load host.

The improvement from the initial 3,224 requests/second came from a bounded ingestion channel that coalesces up to 1,000 requests into one durable PostgreSQL insert, a 50-connection pool that applies backpressure, 5,000-row set-based read-model batches, fall-first claiming, and disabling per-request information logs. HTTP success is completed only after the ingestion batch commits.

Additional verification used only the supplied generator: the 95-second offline scenario accepted 4,603/4,603 events with 0 failures and returned all 65 logical falls exactly once; a 5,001-device run accepted 5,064/5,064 events without registration or redeployment. SSE resume was verified by reconnecting with `Last-Event-ID` and receiving the next persisted alarm.

Run scored scenarios against a fresh database. The generator restarts device sequence numbers at 1, so rerunning it against retained data is correctly treated as transport redelivery. To discard local test data: `docker compose down -v`, then `docker compose up --build`.

## Tradeoffs

- The service deliberately runs without Orleans, a broker, or a migration framework. SSE and durable alarm polling share the same persisted alarm table.
- Availability is `min(heartbeats in the previous five event-time minutes / 300, 1)` based on the specified ~1 Hz expectation.
- Fall deduplication depends on the generator's exact shared timestamp for jitter copies. If hardware assigns slightly different timestamps, add a documented device-specific quiet interval.
- Read-model history is not pruned. Add retention/partitioning only when stored volume warrants it.
- Local measurements describe this development machine and workload only; they are not a 50k requests/second capacity claim.

---

> **No prior experience required. The solution is the signal.**
> Every submission gets feedback within 7 days. → `info@teton.ai`

---

A Teton sensor sits in a care room. It streams events, heartbeats, presence, motion, sleep state, fall warnings, network status, to our backend, all day, every day. We have a few thousand of those right now. We will have a few hundred thousand. Each event matters; some matter more than others; all of them have to land in the right place quickly.

This challenge is about building the part of our backend that **takes in those events, makes sense of them in real time, and stays correct under pressure**.

## The problem

Build a service that ingests events from **5,000 simulated devices**, each streaming multiple event types at variable rates, and produces correct real-time aggregations.

You will receive events that look like:

```json
{ "device_id": "dev_0001", "room_id": "room_14", "type": "heartbeat",   "ts": "2026-05-23T18:53:49.123Z" }
{ "device_id": "dev_0001", "room_id": "room_14", "type": "presence",    "ts": "...", "in_room": true }
{ "device_id": "dev_0001", "room_id": "room_14", "type": "motion",      "ts": "...", "magnitude": 0.81 }
{ "device_id": "dev_0001", "room_id": "room_14", "type": "sleep_state", "ts": "...", "state": "asleep" }
{ "device_id": "dev_0001", "room_id": "room_14", "type": "fall_warn",   "ts": "...", "confidence": 0.92 }
{ "device_id": "dev_0001", "room_id": "room_14", "type": "net_status",  "ts": "...", "rssi": -68 }
```

You receive them via your choice of transport, HTTP/gRPC/WebSocket/MQTT, from the event generator we provide. Your job:

### Required outputs, queryable in real time

1. **Per-device health.** For every device, the latest heartbeat and a rolling availability over the last 5 minutes (heartbeats expected at ~1Hz).
2. **Per-room occupancy.** For every room, current `in_room` boolean (latest presence event wins) and the percentage of time the room was occupied over the last 1 minute, 5 minutes, and 1 hour.
3. **Fall warnings, deduplicated.** A device sometimes sends the same fall warning multiple times within a few seconds (sensor jitter). Emit each distinct fall event exactly once, and persist them with their original timestamp.
4. **Active alarms feed.** A real-time feed (long-poll, SSE, WebSocket, your choice) that consumers can subscribe to and receive new fall warnings as they happen, in order per room, within 1 second of ingestion.

### The complications you must handle

This is where the challenge is.

- **Per-device ordering.** Events from the same device arrive in order most of the time, but not always, devices buffer when offline and replay when reconnecting. Order by `ts`, not by arrival.
- **Clock skew.** Devices have wall clocks that drift. Trust `ts` for ordering and aggregation, but reject events more than 1 hour in the future (clearly broken) and accept events up to 1 hour in the past (offline buffer).
- **Late events.** A device may go offline for 20 minutes and replay every event when it reconnects. Your aggregations must update correctly when historical events arrive.
- **Backpressure.** When ingest spikes 10x for 30 seconds, you must not lose events. You may *delay* them. You may *prioritize* `fall_warn` over `heartbeat`. You must not drop them silently.
- **Restart correctness.** Kill the service; bring it back up. State for per-device health, per-room occupancy, and recent alarms must be recoverable. You decide how (persistent log, periodic snapshot, fresh from replay, whatever, but it must work).

## What "good" looks like

Not "passes the tests once". Good means:

- **At 10x burst rate** (50,000 events/sec sustained for 30s), the alarm feed still emits within 1s of ingest p95.
- **After a hard restart**, every consumer that was subscribed reconnects and resumes without missing alarms generated during the gap.
- **Late events from an offline device** correctly fix up the per-room occupancy history (i.e., if room was actually occupied during that gap, the 1h window reflects it after replay).
- **Adding a 5,001st device** doesn't require redeploying anything.

## What we evaluate

We run a grader that:

- Launches the event generator at baseline rate (5k devices × ~1 event/sec mixed) for 5 minutes.
- Spikes to 10x for 30 seconds, twice.
- Simulates 20% of devices going offline for 60 seconds and replaying their buffered events on reconnect.
- Hard-kills your service after 3 minutes and brings it back.
- At regular intervals queries: per-device health, per-room occupancy windows, dedup counts on fall warnings, and the live alarm feed.
- Checks for correctness against ground truth (we know exactly what the generator emitted).

## Scoring (out of 100)

| Category | Points |
|---|---|
| Correctness of aggregations under all conditions | 30 |
| Behavior under burst load and backpressure | 20 |
| Restart / recovery correctness | 15 |
| Alarm feed latency p95 | 15 |
| Code quality and design clarity | 15 |
| Observability (logs, metrics) | 5 |

**Pass bar:** 75. Below it we won't say yes, but we always reply.

## What's in this repo

```
.
├── README.md          - this file
├── SUBMISSION.md      - fill in with your submission
├── Makefile           - convenience targets (see `make help`)
├── event_generator/   - simulates N devices in baseline / burst / offline / adversarial modes
├── eval/              - scenario runner + scorecard
├── example_solution/  - a deliberately-bad stub service so you can see
│                        the read endpoints we expect. Replace with yours.
└── docs/
    └── event_schema.md  the full event spec
```

## Quickstart

Python 3.10+. No pip dependencies.

```bash
# Terminal 1: start the example stub service (replace with your own later)
make example

# Terminal 2: run the smoke scenario
make smoke
```

The smoke run takes ~30 seconds. The example stub stores everything in memory with no dedup, no late-event handling, no restart correctness — it will score badly on the scorecard. That's the point. Beat it.

Bigger scenarios: `make baseline`, `make burst`, `make offline`, `make adversarial`. Crank devices with `DEVICES=500 make burst`. Point eval at your own service with `make smoke SERVICE_URL=http://localhost:9090`.

The eval expects these read endpoints on your service:

```
GET /devices/{device_id}/health
GET /rooms/{room_id}/occupancy?window=1m|5m|1h
GET /alarms?since=<ts>
```

The shape that `example_solution/service.py` returns is what the scorer reads. If your endpoints look different, the scorer will warn.

## What we are **not** looking for

- Kafka-just-because. Use it if it earns its place. Justify it briefly.
- Hand-rolled distributed consensus.
- A pretty dashboard. Your service exposes HTTP/gRPC endpoints; that's enough.
- Long architectural prose. Show the thinking, not the slideware.

## What to send us

Email **info@teton.ai** with subject **`Solution: Real-time streaming backend`** and:

1. Link to your fork (public) or a tarball.
2. Writeup (under 400 words):
   - Stack and storage choice, why.
   - How you handle late events and ordering.
   - How you handle backpressure.
   - One thing you would change if you had another week.
3. How to run your service against `event_generator/` locally.
4. Your **CV** (attached), plus **LinkedIn** and **GitHub** links so we can put the work in context.

**We reply with feedback within 7 days, every submission, no exceptions.** If your work hits the bar, the next step is a conversation with engineers.

## Notes

- Time: strong candidates spend 10–25 hours on this. Spend more if you want.
- Stack: any language, any database, any message broker. We will run it locally, say so if you need Docker Compose, Nix, or just `make run`.
- LLMs: use them as you would on any other day. We don't care how you got there. We care that you can explain every choice.

Good luck.

The Teton engineering team
