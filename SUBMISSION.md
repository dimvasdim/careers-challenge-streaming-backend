# Submission, Real-time Streaming Backend

Code and details: [service/README.md](service/README.md).

---

## Stack and storage

TypeScript, NestJS on Fastify, plain `pg`, events over HTTP (the generator's default).
PostgreSQL is the only state: every read endpoint queries it, and memory holds nothing but open
SSE connections, so there is no read model to rebuild. Indexed reads are fast:
on 18M rows (1 h × 5,000 devices), health ~0.5 ms, 1 h occupancy ~0.3 ms.
`node:cluster` runs up to 4 workers on one port, because one process tops out at ~12k events/s.
No Kafka or Redis: Postgres plus `LISTEN/NOTIFY` covers durability and alarm fan-out.

## Ordering and late events

`ts` decides order, never arrival. `UNIQUE (device_id, ts, seq)` with `ON CONFLICT DO NOTHING` is
the only dedup. Aggregations are computed on read, so a replayed event fixes the 1 h occupancy
history once committed. Availability counts distinct heartbeat seconds
in the device's own last 5 minutes (server time minus its measured clock offset), so drift
doesn't hurt.

A `fall_warn` within 5 s of an incident's first `ts` on the same device joins that incident;
otherwise it is a new alarm with its original `ts`. Alarms are written in the same transaction
as their events.

**Trade-off:** the stub sends jitter copies with an identical `ts`, so in the eval separate falls
of one device < 5 s apart merge (burst: 145 alarms / 160 falls, none lost). Exact-`ts` dedup
would score 160/160 but would alarm twice if a real sensor's copies carry slightly different
`ts`.

## Backpressure

Group commit: each worker keeps one insert in flight, and what arrives meanwhile forms the next
batch, so batches grow with load. A 2xx means "committed". Valid events never get 429/503 (the
generator doesn't retry). `fall_warn` has a priority lane; per-lane queue depth and latency
are in `/metrics`.

## Restart correctness

A hard kill runs no shutdown code, and none is needed. An event is acknowledged only after its
batch commits, events and their alarms commit in one transaction, and no state lives outside
Postgres. Clients reconnect with `Last-Event-ID` and catch up from the alarms table.

## How to run it locally

```bash
make run                                                    # Postgres + service on :8080
python eval/check.py burst --target http://127.0.0.1:8080   # 127.0.0.1: on Windows, localhost adds ~2 s per request
WORKERS=1 make run                                          # single process instead of up to 4 workers
```

On Windows, Docker Desktop's port forwarding adds seconds of latency; for load tests run the
service natively (service/README.md, "Running it").

## Reported metrics

Dev laptop, 4 workers, service native, Postgres in Docker.

- **Sustained ingest rate:** 10k events/s sustained; 10x burst peak ~19.7k events/s, where the
  laptop (load clients + service + Postgres) is the limit, not the batch writer (commits avg
  16 ms). 0 of 990,329 acknowledged events lost.
- **Alarm feed latency** (receipt → SSE push, in the service): avg 129 ms, 100% < 500 ms during
  the burst; < 100 ms in every eval scenario.
- **Behavior under hard kill + restart:** SIGKILL mid-load, back after 7.6 s. All 4 SSE clients
  received all 1,035 alarms, 0 missing / doubled / out of order. Every 2xx event is in Postgres.
- **Aggregation correctness on replayed events:** offline eval accepted 4,906 / 4,906 events;
  e2e tests cover late events, carry-in and clock skew. Healthy simulated devices show ~0.66
  availability: the generator sends one event type per tick, and ~2/3 of them are heartbeats.

## With another week

MQTT with a broker (QoS 1, acknowledged after commit): devices would retry, so events sent during
downtime aren't lost (today the generator drops them), and the broker absorbs bursts.

Events are never deleted today: I'd partition the table by day and archive old days to object
storage, so Postgres stays small and the history stays available.
