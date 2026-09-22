# Submission, Real-time Streaming Backend

**Your name:** Reza Faghani
**Email:** rezafaghani@live.com
**Link to your fork or solution:** https://github.com/rezafaghani/careers-challenge-streaming-backend

---

## Stack and storage

ASP.NET Core exposes the HTTP contract. PostgreSQL stores the inbox, histories, and alarms while acting as the work queue, avoiding a broker. Orleans would duplicate state ownership here.

## Ordering and late events

`DateTimeOffset` values are stored as `timestamptz`; state uses device event time. Occupancy starts with the transition preceding its window and integrates transitions inside it, so late replay corrects results.

## Backpressure

Ingestion durably batches validated events before returning. PostgreSQL is the bounded-by-disk backlog. Workers claim disjoint 5,000-row batches using `FOR UPDATE SKIP LOCKED`, apply each in one statement, and sort falls first. Accepted events are never discarded. Stateless replicas safely share the database behind a load balancer.

## Restart correctness

The read-model write and inbox completion marker share a transaction. A kill rolls both back, and the pending row is retried after startup. Unique `(device_id, seq)`, source event IDs, and logical fall keys make transport retries and worker retries idempotent. Alarm polling by timestamp recovers alarms missed during a disconnect.

## How to run it locally

```bash
docker compose up --build
make smoke
make load-50k # 60,000 events/sec for 30 seconds
```

## Reported metrics

- Load: `make load-50k` runs a paced 60,000 events/second target for 30 seconds (1.8 million attempted events), with per-second accepted/failure counts and final service p95/backlog metrics. The result is intentionally measured at runtime rather than claimed here; capacity depends on PostgreSQL and host resources.
- Processing p50/p95 was 99/237 ms; alarm persistence p50/p95 was 82/208 ms. Bounded 1,000-event durable inserts, a 50-connection pool, and 5,000-row fall-first processing batches produced this result.
- Hard kill + restart: supplied generator produced 1,482 successful requests and 144 failures while the app was intentionally unavailable. PostgreSQL contained 1,483 committed events (one response reset after commit); all 1,483 processed after restart with 0 pending and 0 batch failures.
- Replay/device correctness: the supplied offline run accepted 4,603/4,603 events and returned all 65 falls exactly once; a 5,001-device run accepted 5,064/5,064 without registration. SSE `Last-Event-ID` resume returned the next persisted alarm.

## With another week

I would test 50k requests/second on representative hardware and tune/partition PostgreSQL; the local database was the measured ceiling.
