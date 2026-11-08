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
- PostgreSQL is the single source of truth (plain Postgres, no TimescaleDB): every read endpoint
  queries it; nothing but open SSE connections lives in memory.
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
- `src/core/` — plain TypeScript, no decorators, no Nest imports: validation, batching, the
  health/occupancy math, fall dedup, SSE subscription merge. Pure where possible and unit-tested.
- `src/db/` — hand-written SQL. Rule: SQL selects the rows, `core/` does the math.
- Nest is a thin shell: modules (Ingest, Health, Occupancy, Alarms, Metrics), shutdown drain,
  the read endpoints and SSE.
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
- Duplicates: identity key `(device_id, ts, seq)`. `seq` may have gaps — never wait for missing seqs.
  Postgres is the only dedup: events table has `UNIQUE (device_id, ts, seq)`; the batch insert uses
  `ON CONFLICT DO NOTHING RETURNING`, and rows not returned are duplicates. Only inserted
  fall_warns reach the fall dedup. No in-memory seen-key set.
- One transaction per batch (`db/batch-commit.ts`): insert events, then create/update the alarms of
  the new fall_warns, then COMMIT. Events and alarms are never out of sync after a crash.

## State and recovery
- Postgres is the only state. Compute on read: no cron, no precomputed aggregates, no cache layer,
  no in-memory read models. Reads are indexed range queries (measured on 1h x 5000 devices, 18M
  rows: health ~0.5 ms, occupancy 1h ~0.3 ms; `service/scripts/read-bench/`).
- Indexes: `UNIQUE (device_id, ts, seq)` also serves device ranges; partial covering index
  `(room_id, ts) INCLUDE (in_room, id) WHERE type = 'presence'`; `alarms (ts)`, `alarms (device_id, ts)`.
- Restart: nothing to recover — a restarted process serves correct reads immediately. SSE clients
  reconnect with `Last-Event-ID` and catch up from the alarms table.
- Pool: one connection for the batch writer, the rest for reads (`DB_POOL_SIZE`, default 6).

## Device health
`GET /devices/{device_id}/health` → `{ last_heartbeat_ts, availability_5m }`
- One query (`loadHealthInput`) + pure `computeHealth`.
- availability = distinct heartbeat seconds in `(deviceNow-300, deviceNow]` ÷
  `min(300, seconds since first seen)`; deviceNow = server now − clock offset of the newest event
  with a sane clock (|arrival − ts| ≤ 5 min). Heartbeats > 5 min ahead of arrival are ignored.
- Late heartbeats count once they are inserted; duplicates count once (distinct seconds).
- Unknown device → 404. New devices appear without redeploy.

## Room occupancy
`GET /rooms/{room_id}/occupancy?window=1m|5m|1h` → `{ in_room, occupied_pct }`
- One query (`loadPresencePoints`): presence events of the room from the window start on, plus the
  newest one before it (carry-in), ordered by ts then commit order; pure `computeOccupancy`.
- All devices of a room share one timeline (~2 devices per room): latest presence event by ts wins.
  Device ts used as-is; the window ends at server now (no per-device clock correction).
- occupied_pct = sum of "present" intervals clipped to `[now-window, now]` ÷ window;
  the latest state extends to now. `in_room` = state of the latest event by ts.
- Before the first known event the room counts as absent; a room without presence → `{false, 0}`.
- Late events fix up the history automatically (the query reads whatever is committed).

## Fall warnings and alarm feed
`GET /alarms?since=<ts>` → `{ alarms: [{ event_id, room_id, ts, confidence }] }`
- No confidence threshold: every fall counts (missing a fall is worse than a false alarm).
- Jitter dedup per device: a `fall_warn` with `|ts - firstTs| <= 5s` joins the nearest incident
  (update maxConfidence, do not emit again); otherwise it starts a new incident.
  Window is measured from `firstTs`, not from the last event. Matched against every nearby alarm
  in the table, so a late replayed fall can't break dedup of a live one.
  5s = "a few seconds" of jitter (README's 1s is delivery latency, unrelated).
- In the batch transaction: per-device `pg_advisory_xact_lock` (sorted, no deadlocks) → load
  nearby alarms → pure `planAlarms` → one upsert. The lock makes "look, then insert" safe even
  with concurrent transactions (tested in `test/batch-commit.e2e-spec.ts`).
- Incident `ts` = first fall_warn's ts (original timestamp). Late earlier copies inside the window merge.
- `event_id = ${device_id}:${firstTs}`, `alarms` table with UNIQUE(event_id).
  Committed BEFORE being pushed to subscribers (pushed after COMMIT, before the 2xx).
- Live feed over SSE (Nest `@Sse()` controller route), ordered per room, within 1s of ingestion.
  SSE event id = `alarms.id` (commit order), so `Last-Event-ID` catch-up also gets late replayed
  alarms. Catch-up always from Postgres (`id > last`); `AlarmHub` only fans out to open connections.
  Keepalives are SSE comments (no id).
- `GET /alarms?since=` is a plain query on `alarms (ts)`; `since` = epoch seconds or ISO date.

## Observability
- `/metrics` (prom-client): events ingested per type, rejected (by reason), duplicates dropped,
  seq gaps, queue depth per priority, batch size/lag, alarm latency histogram (ingest → SSE, p95),
  SSE subscribers, read query latency, default Node metrics (event-loop lag).
- `pino` JSON logs for startup, backpressure, rejected events. Never log every event.
- Optional Prometheus + Grafana in a Compose profile — not required.

## Testing
- Unit tests for the pure core functions; SQL behaviour (late events, carry-in, clock skew,
  concurrency) in e2e tests against the `teton_test` database.
- Run `eval/check.py` (baseline, burst, offline, adversarial) after each milestone.
- Load-test the 10x burst early; watch event-loop lag and alarm p95.

## Build order
1. Ingest + validation + group commit to Postgres
2. Device health
3. Room occupancy
4. Fall dedup + alarms + SSE feed
5. Restart verification (hard kill + SSE reconnect; no snapshots needed)
6. Metrics + logs
7. Writeup

