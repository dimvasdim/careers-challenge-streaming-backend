# CLAUDE.md — Teton streaming backend challenge

## Challenge context
- Challenge: https://github.com/Teton-ai/careers-challenge-streaming-backend
- Read first: `README.md` (requirements + grading), `docs/event_schema.md` (event contract),
  `eval/check.py` (expected endpoint shapes), `event_generator/generate.py` (actual generator behaviour).
- The generator will be "fleshed out" before grading: code against the spec, not just the stub.
- Grading: aggregation correctness 30, burst/backpressure 20, restart/recovery 15,
  alarm latency p95 15, code quality/design 15, observability 5. Pass bar 75/100.
- Not wanted: Kafka without justification, hand-rolled consensus, dashboards, long architecture prose.
- I must be able to explain every choice — prefer simple, readable code over clever code.

## Stack
- TypeScript, NestJS with the Fastify adapter (`@nestjs/platform-fastify`), single Node process.
- PostgreSQL for durability (plain Postgres, no TimescaleDB).
- Database access: plain `pg` (node-postgres) with a Pool and hand-written SQL; schema in
  `db/schema.sql` mounted into `/docker-entrypoint-initdb.d/`. No ORM (no Prisma/TypeORM).
- Docker Compose for Postgres + service; `make run` target in the existing Makefile.
- `prom-client` for metrics, `pino` for structured JSON logs.
- Do NOT use: Kafka, Redis, `@nestjs/cache-manager`, `@nestjs/microservices`, worker threads,
  multiple workers/cluster mode, per-event OpenTelemetry spans, hosted observability services.

## Git workflow
- Never run `git commit`, `git push`, `git reset`, `git rebase` or anything else that changes history.
- When a logical unit of work is done and tests pass, stop and suggest a commit:
  list the files to stage and propose a short commit message. I will commit myself.
- `git status` and `git diff` are fine to run.

## Code structure
- `src/core/` — plain TypeScript, no decorators, no Nest imports: aggregators, dedup, batching,
  snapshot/recovery. All core functions are pure where possible and unit-tested.
- Nest is a thin shell: modules (Ingest, Health, Occupancy, Alarms, Metrics), lifecycle hooks
  (load snapshot on init, flush on shutdown), the read endpoints and SSE.
- The ingest hot path must not go through guards/interceptors/pipes per event.
- Ingest sits behind an interface so another transport (MQTT) can be added later.
- The service lives in service/; challenge files (event_generator/, eval/, docs/) stay untouched.

## Ingest
- `POST /events`, one JSON event per request (generator default, see `generate.py`).
- Validation: reject malformed/missing fields and `ts` > 1h in the future → 400 + metric.
  Accept `ts` up to 1h in the past. `ts` is authoritative for ordering, never arrival time.
- Never return 429/503 for valid events: the generator does not retry, so a rejection = lost event.
- Group commit: flush when idle, batch while busy. If no insert is running, write the queue on the
  next event-loop turn; while an insert is in flight, new events queue and form the next batch
  (one insert in flight at a time, no fixed flush interval). Reply 2xx to all requests in a batch
  after it commits. A 2xx means "persisted".
- Backpressure is internal: priority lane for `fall_warn`, heartbeats may wait; expose queue depth.
- Duplicates: identity key `(device_id, seq, ts)`. `seq` may have gaps — never wait for missing seqs.
  Postgres is the only dedup: events table has `UNIQUE (device_id, seq, ts)`; the batch insert uses
  `ON CONFLICT DO NOTHING RETURNING`, and rows not returned are duplicates. Only inserted rows
  reach memory (`onCommitted`), so read models never apply a duplicate. No in-memory seen-key set.

## In-memory state rules
- All read models live in memory in ONE Node process:
  - devices: `Map<deviceId, { ring: Uint32Array(300), lastHeartbeatTs, firstSeenTs, lastSeq }>`
    (`lastSeq` only for the seq-gap metric)
  - rooms: `Map<roomId, Presence[]>` (sorted by ts, change points only)
  - falls: `Map<deviceId, { firstTs, maxConfidence, eventId }>` (current incident)
  - recentAlarms: `Alarm[]` (last ~1h, sorted by ts) for `/alarms?since=` and SSE catch-up
- Compute on read. No cron, no per-second recompute, no precomputed aggregates, no cache layer.
  Writes are O(1)/O(log n); reads scan small bounded arrays.
- Memory is bounded by the 1h acceptance window: evict by ts (presence keeps one carry-in event;
  recentAlarms is pruned after 1h).
- Events are applied to memory only after their batch is committed to Postgres,
  so memory never contains anything that would be lost on a crash.
- Snapshots: every N seconds, serialise all maps + the id of the last events-table row included.
  Single-threaded, so a synchronous snapshot is consistent.
- Startup: load latest snapshot → replay events with id > snapshot id → rebuild recentAlarms
  from the alarms table → start accepting traffic. Log and record recovery time.

## Device health
`GET /devices/{device_id}/health` → `{ last_heartbeat_ts, availability_5m }`
- 300-slot ring buffer per device; slot index = `tsSec % 300`; each slot stores the second it holds.
- availability = slots with second in `(now-300, now]` ÷ `min(300, seconds since first seen)`.
- Late heartbeats write their slot if still inside the window; duplicates write the same value.
- New devices appear without redeploy (just a new Map entry).

## Room occupancy
`GET /rooms/{room_id}/occupancy?window=1m|5m|1h` → `{ in_room, occupied_pct }`
- Per room: presence change points sorted by `ts`. Presence is emitted on state change
  (the stub may repeat states — handle it).
- Insert with binary search; skip exact duplicates (same ts + same state). Do not merge
  consecutive equal states (a late opposite event may land between them).
- Evict events older than 1h EXCEPT the newest of them (carry-in state for the window start).
- occupied_pct = sum of "present" intervals clipped to `[now-window, now]` ÷ window;
  the latest state extends to now. `in_room` = state of the latest event by ts.
- Before the first known event, the room counts as absent (document in writeup).
- Keyed by `room_id` — check whether a room can have several presence devices.

## Fall warnings and alarm feed
`GET /alarms?since=<ts>` → `{ alarms: [{ event_id, room_id, ts, confidence }] }`
- No confidence threshold: every fall counts (missing a fall is worse than a false alarm).
- Jitter dedup per device: a `fall_warn` with `|ts - firstTs| <= 10s` joins the current incident
  (update maxConfidence, do not emit again); otherwise it starts a new incident.
  Window is measured from `firstTs`, not from the last event.
- Incident `ts` = first fall_warn's ts (original timestamp). Late earlier copies inside the window merge.
- `event_id = ${device_id}:${firstTs}`, `alarms` table with UNIQUE(event_id).
  Persist to Postgres BEFORE pushing to subscribers.
- Live feed over SSE, ordered per room, within 1s of ingestion. Support `Last-Event-ID`
  for catch-up from recentAlarms, falling back to Postgres for older ranges.
- On startup, load the last ~1h of incidents so dedup survives a restart.

## Observability
- `/metrics` (prom-client): events ingested per type, rejected (by reason), duplicates dropped,
  seq gaps, queue depth per priority, batch size/lag, alarm latency histogram (ingest → SSE, p95),
  SSE subscribers, recovery time, replayed events, default Node metrics (event-loop lag).
- `pino` JSON logs for startup, recovery, backpressure, rejected events. Never log every event.
- Optional Prometheus + Grafana in a Compose profile — not required.

## Testing
- Unit tests for core functions: late events, room quiet for hours (carry-in), duplicate replays,
  fall jitter copies, incident window edges, restart replay from snapshot.
- Run `eval/check.py` (baseline, burst, offline, adversarial) after each milestone.
- Load-test the 10x burst early; watch event-loop lag and alarm p95.

## Build order
1. Ingest + validation + group commit to Postgres
2. Device health
3. Room occupancy
4. Fall dedup + alarms + SSE feed
5. Snapshots + restart recovery
6. Metrics + logs
7. Writeup

