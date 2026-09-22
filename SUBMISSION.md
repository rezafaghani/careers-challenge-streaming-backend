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

Ingestion performs validation and one durable insert, then returns. PostgreSQL is the bounded-by-disk backlog. Workers claim disjoint 500-row batches using `FOR UPDATE SKIP LOCKED`, process each batch in one set-based statement, and sort falls first. Accepted events are never discarded. Stateless replicas can safely share the same database behind a load balancer.

## Restart correctness

The read-model write and inbox completion marker share a transaction. A kill rolls both back, and the pending row is retried after startup. Unique `(device_id, seq)`, source event IDs, and logical fall keys make transport retries and worker retries idempotent. Alarm polling by timestamp recovers alarms missed during a disconnect.

## How to run it locally

```bash
docker compose up --build
make smoke
```

## Reported metrics

- Sustained ingest rate: supplied 100-device burst generator, 2,231 requests in 25 seconds (89.2 requests/second average), 0 HTTP failures, and 0 final backlog. This is a local functional measurement, not the 50k/second production target.
- Alarm latency p50 / p95: 12.8 ms / 20.1 ms in that clean run; all 28 expected logical falls persisted exactly once.
- Hard kill + restart: supplied generator produced 1,482 successful requests and 144 failures while the app was intentionally unavailable. PostgreSQL contained 1,483 committed events (one response reset after commit); all 1,483 processed after restart with 0 pending and 0 batch failures.
- Replay correctness: focused tests cover out-of-order timeline insertion and occupancy integration. The smoke evaluator matched its logical-fall ground truth; `make offline` and `make adversarial` run the supplied replay scenarios.

## With another week

I would run a concurrent 50k requests/second workload on representative hardware and tune PostgreSQL and worker concurrency from measurements. If push is required, I would add SSE backed by the durable alarms table.
