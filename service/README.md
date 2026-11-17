# Streaming backend service

Ingests sensor events over HTTP, stores them in PostgreSQL and serves device health, room
occupancy and a deduplicated, real-time fall-alarm feed. TypeScript, NestJS on Fastify,
plain `pg`, `node:cluster`.

**Design in one sentence:** Postgres is the only state. Every event is committed before it is
acknowledged, every read is computed from committed rows, and live alarms are pushed after
commit. A restart therefore has nothing to recover.

| Endpoint | Returns |
|---|---|
| `POST /events` | `200 {"status":"ok"}` once persisted, `200 {"status":"duplicate"}`, `400 {"error":"<reason>"}` |
| `GET /devices/{device_id}/health` | `{ last_heartbeat_ts, availability_5m }`, 404 for an unknown device |
| `GET /rooms/{room_id}/occupancy?window=1m\|5m\|1h` | `{ in_room, occupied_pct }` |
| `GET /alarms?since=<epoch s \| ISO 8601>` | `{ alarms: [{ event_id, room_id, ts, confidence }] }`, sorted by `ts` |
| `GET /alarms/stream` | SSE feed of new alarms; resume with `Last-Event-ID` (or `?last_event_id=`) |
| `GET /metrics` | Prometheus text, merged over all workers |

---

## Running it

**Docker Compose (Postgres + service on :8080)**, from the repo root:

```bash
docker compose up --build        # or: make run
docker compose down -v           # wipe the database (re-applies db/schema.sql on next start)
```

**Natively (Postgres in Docker).** Use this for load tests on Windows, where Docker Desktop's port
forwarding adds seconds of latency:

```bash
docker compose up -d postgres
cd service
npm install
cp .env.example .env             # the start scripts load .env; every variable is optional
npm run build && npm run start:prod
```

**Challenge eval**, from the repo root:

```bash
psql -h localhost -p 5433 -U teton -d teton -c "TRUNCATE events, alarms"   # password: teton
python eval/check.py baseline --target http://127.0.0.1:8080
```

- **Empty the tables first.** `check.py` counts every alarm since `since=0`, so alarms left over
  from earlier runs show up as "extra alarms".
- **Use `127.0.0.1`, not `localhost`, on Windows.** Python tries IPv6 `::1` first and waits
  about 2 s per request before falling back to IPv4.

### Configuration

All settings are environment variables ([src/config.ts](src/config.ts)) and all are optional.
Under Docker Compose only `WORKERS` is passed through from the shell (`WORKERS=1 make run`);
in the container, `cores` means the CPUs Docker gives it.

| Variable | Default | Meaning |
|---|---|---|
| `DATABASE_URL` | `postgres://teton:teton@localhost:5433/teton` | Postgres connection |
| `PORT` | `8080` | HTTP port, shared by all workers |
| `WORKERS` | `min(cores, 4)` | worker processes; `1` = a single process, no cluster |
| `DB_POOL_SIZE` | `4` | connections per worker (min 2): 1 write + the rest for reads; the alarm listener adds 1 more |
| `BATCH_MAX_SIZE` | `2000` | maximum events per insert |
| `METRICS_PORT` | `9464` | primary-only localhost port for the merged metrics |
| `SSE_KEEPALIVE_MS` | `15000` | interval of SSE keepalive comments |
| `BACKPRESSURE_LOG_DEPTH` | `5000` | queue depth that logs a backpressure warning |

---

## Architecture

```
                      ┌────────────── primary (src/cluster.ts) ──────────────┐
                      │ forks workers, restarts crashed ones, forwards       │
                      │ SIGTERM, merges metrics. Runs no app code.           │
                      └──────────────────────────────────────────────────────┘
 POST /events ─┐        worker 1..N (each the full Nest app, sharing :8080)
               ▼
   validate ─► BatchWriter lanes ─► one transaction per batch ─────────────► Postgres
   (core/)     priority: fall_warn   insert events (ON CONFLICT DO NOTHING)    events
               normal:   the rest    + fall dedup → upsert alarms              alarms
                                     + NOTIFY alarms → COMMIT → reply 2xx
                                                         │
               ┌─────────── NOTIFY reaches every worker ─┘
               ▼
   AlarmListener ─► SELECT id > lastSeen ─► AlarmHub ─► each SSE connection
   (LISTEN conn)                                        (catch-up + live, no gaps)

 GET health / occupancy / alarms ─► one indexed SQL query ─► pure function in core/
```

### Ingest and group commit

- **Hot path.** `POST /events` is registered directly on Fastify
  ([http-transport.ts](src/ingest/http-transport.ts)), so no Nest guards, pipes or interceptors
  run per event. `IngestService` implements the `EventIngest` interface that any transport
  uses, so an MQTT transport would be an addition, not a redesign.
- **Validation** ([validate.ts](src/core/validate.ts)) checks the schema and an explicit-zone
  ISO `ts` within ±1 h of server time. It also rejects values Postgres would refuse (IDs over
  128 characters or containing NUL, `rssi` outside int32): one such event would fail its batch
  on every retry. Rejections return 400 and increment `events_rejected_total{reason}`.
- **Group commit** ([batch-writer.ts](src/core/batch-writer.ts)): flush when idle, batch while
  busy. A worker has one insert in flight at a time.
  - **Quiet:** an event is written on the next event-loop turn, with no timer.
  - **Busy:** events that arrive during an insert form the next batch, so batch size follows
    load (1 → up to 2,000). A commit costs about the same for 1 or 2,000 rows.
- **Acknowledgement after commit.** The 2xx is sent only after the batch commits: a 2xx means
  "in Postgres". The generator doesn't retry, so acknowledging earlier would turn a crash into
  silent loss.
- **Failure handling.** A failed batch is retried with capped backoff (50 ms → 1 s), keeping
  its order. Valid events never get 429 or 503.

### Backpressure

There is no rejection, only delay plus priority:

- **Unbounded wait:** while Postgres is the bottleneck, requests wait in the lanes.
- **Priority lane:** `fall_warn` goes to its own lane, and every batch fills from it first, so
  in a 10x burst falls go into the next batch and heartbeats wait.
- **Visible:** queue depth per lane is a metric (`ingest_queue_depth`). A log line marks
  entering and leaving backpressure.
- **Write connection:** the batch writer has its own 1-connection pool (`PG_WRITE_POOL`), so a
  commit never waits behind read queries.

### Duplicates, ordering, late events

- **Identity:** an event's identity is `(device_id, ts, seq)`, a `UNIQUE` constraint. The
  batch insert uses `ON CONFLICT DO NOTHING RETURNING`, and rows not returned are duplicates.
  There is no in-memory "seen" set, so dedup survives restarts and works across workers.
- **Gaps:** seq gaps are never waited for.
- **Ordering:** `ts` decides ordering, never arrival time. Late events (offline replay up to
  1 h old) are simply inserted, and every read sees them on its next query.
- **Deadlock avoidance:** each batch is sorted by the identity key before inserting. Two
  workers whose batches share duplicates then lock index entries in the same order and can't
  deadlock (an e2e test deadlocks without the sort).

### Device health

`availability_5m` = distinct seconds holding a heartbeat in the device's last 300 s ÷
`min(300, seconds since first seen)`. One query ([read-queries.ts](src/db/read-queries.ts)) plus
the pure `computeHealth` ([device-health.ts](src/core/device-health.ts)).

- **Device clock:** "last 5 minutes" is measured on the device's own clock: server now minus
  the device's clock offset, where offset = arrival − `ts` of its newest event with a sane
  clock (|offset| ≤ 5 min). A device with a constant ±30 s skew still scores fully, and a
  silent device keeps aging.
- **Duplicates and replays** count once (distinct seconds). Late heartbeats count once
  inserted.
- **Faulty clocks:** heartbeats more than 5 min *ahead* of their arrival are treated as faulty
  readings and ignored. A device whose clock is constantly more than 5 min fast therefore shows
  as unhealthy, which is deliberate: its timestamps would also distort occupancy and alarms.
- **Simulated devices score about 0.66.** The provided generator sends one event type per tick
  and two-thirds of them are heartbeats, so 0.66 is the true fraction of expected 1 Hz
  heartbeats.

### Room occupancy

`occupied_pct` = time "present" within `[now − window, now]` ÷ window. The query returns the
room's presence points from the window start onward, plus the newest one before it (the state at
the window start). The pure `computeOccupancy` ([room-occupancy.ts](src/core/room-occupancy.ts))
sums the present intervals.

- **Timeline:** all devices of a room share one timeline, and the latest presence event by
  `ts` wins (ties: commit order). `in_room` = state of the latest event.
- **Clocks:** device `ts` is used as-is and the window ends at server now. A room mixes devices
  with different drift, so there is no single clock to correct to (≤ ~30 s error on the 1 m
  window).
- **Edge cases:** before the first known event the room counts as absent. A room with no
  presence events returns `{ in_room: false, occupied_pct: 0 }`.

### Fall dedup

In the batch transaction, and only if the batch has newly inserted `fall_warn`s
([batch-commit.ts](src/db/batch-commit.ts), [fall-incidents.ts](src/core/fall-incidents.ts)):

1. take one global `pg_advisory_xact_lock`;
2. load the alarms of these devices within ±5 s;
3. run `planAlarms` (pure): each fall joins the incident of its device whose first `ts` is
   nearest within 5 s, raising the confidence if higher, or starts a new incident;
4. upsert the alarms, `NOTIFY alarms`, `COMMIT`.

- **Window rules.** The window is measured from the incident's *first* `ts`, so an incident
  can't grow forever. Matching against every nearby incident means a late replayed fall can't
  break dedup of a live one. The alarm keeps the first fall's original `ts`;
  `event_id = device_id:first_ts`.
- **No confidence threshold.** A missed fall is worse than a false alarm.
- **Why the lock.**
  - It makes "look, then insert" safe across workers: two copies of one fall can't both
    create an alarm.
  - It makes `alarms.id` follow commit order, which the SSE catch-up relies on. Falls are rare,
    so serializing them costs nothing measurable.
- **Trade-off.** Two *real* falls of one device less than 5 s apart become one alarm. The stub
  generator sends jitter copies with an identical `ts`, so it reports these merges as "missing
  alarms". The 5 s window is kept because the spec says copies arrive "within a few seconds".

### Alarm feed (SSE)

- **Committed before pushed.** Alarms are pushed only after commit. Postgres delivers
  `NOTIFY` on commit only, in commit order.
- **Every worker pushes every alarm.** Each worker's `AlarmListener`
  ([alarm-listener.ts](src/db/alarm-listener.ts)) holds a `LISTEN` connection. On a notification
  it reads `id > lastSeen` from Postgres and hands the rows to the `AlarmHub`, which calls each
  open SSE connection. So a client gets alarms committed by any worker. The notification
  carries no data, so a missed one (listener reconnect) is healed by the next read.
- **SSE event id = `alarms.id`**, which follows commit order, not `ts`. A client reconnecting
  with `Last-Event-ID` gets every alarm committed since, including late replayed ones.
- **No gaps or doubles on reconnect.** `openSubscription`
  ([alarm-subscription.ts](src/core/alarm-subscription.ts)) subscribes to the hub *before* the
  catch-up query, buffers live alarms until catch-up is sent, and skips ids already sent.
- **Keepalives** are SSE comments (no id), so they never change the client's `Last-Event-ID`.

### Restart and shutdown

- **Hard kill:** there is nothing to recover. Every 2xx event and its alarm are in Postgres in
  the same transaction, and reads compute from Postgres. SSE clients reconnect and catch up
  from the alarms table.
- **SIGTERM:** the primary forwards it to every worker. Each worker closes HTTP, drains its
  ingest queue, stops the listener, then closes the pools.
- **Crashed worker:** the primary starts a replacement after 1 s.

### Cluster mode

One process tops out at about 12k events/s, because one batch insert is in flight (measured
2026-11-09). Workers multiply the inserts in flight: with 4 workers, about 20k/s on the dev
laptop (re-measured 2026-11-16, limited by the laptop itself: load clients + service +
Postgres). Workers share nothing in memory. The only
cross-worker pieces are Postgres (unique key, advisory lock, `LISTEN/NOTIFY`) and the primary's
metric merge.

---

## Data model

[db/schema.sql](../db/schema.sql), applied by the Postgres image on an empty volume:

| Table / index | Purpose |
|---|---|
| `events` + `UNIQUE (device_id, ts, seq)` | every accepted event; the unique index also serves per-device range reads (health) |
| `events_presence_room_ts_idx (room_id, ts) INCLUDE (in_room, id) WHERE type = 'presence'` | occupancy: partial (~5% of rows) and covering (index-only scans) |
| `alarms` + `UNIQUE (event_id)` | one row per deduplicated fall; `id` = SSE event id |
| `alarms (ts)`, `alarms (device_id, ts)` | `GET /alarms?since=`, and finding nearby incidents for dedup |

There are no precomputed aggregates and no cache: every read is computed from these tables.
Measured on 1 h × 5,000 devices (18M rows, [scripts/read-bench/](scripts/read-bench/)): a
health query takes about 0.5 ms and an occupancy 1 h query about 0.3 ms.

## Observability

- **`/metrics`** ([metrics.ts](src/metrics/metrics.ts)): with workers, any worker returns the
  primary's merged view.

  | Metric | Shows |
  |---|---|
  | `events_ingested_total{type}`, `events_rejected_total{reason}`, `events_duplicate_total` | ingest outcomes |
  | `ingest_queue_depth{lane}`, `ingest_persist_seconds{lane}` | backpressure and time to persist |
  | `ingest_batch_size`, `ingest_batch_commit_seconds` | group commit |
  | `alarm_latency_seconds` | ingest → SSE push |
  | `sse_subscribers`, `read_query_seconds{endpoint}` | the read side |
  | Node defaults | event-loop lag, CPU, memory |

- **Logs:** pino JSON ([logger.ts](src/logger.ts)) for startup, worker restarts, backpressure
  on and off, failed batch writes, and one summary of rejected events per 5 s. Never one line
  per event.

## Code map

```
src/
  main.ts, cluster.ts, config.ts, logger.ts   process startup, primary, env config, JSON logs
  core/      plain TypeScript, no Nest, unit-tested
             validate, batch-writer, device-health, room-occupancy,
             fall-incidents, alarm-hub, alarm-subscription, alarm-json, event
  db/        hand-written SQL: batch-commit (the transaction), events-repository,
             alarms-repository (lock, upsert, reads), read-queries, alarm-listener,
             database.module (read + write pools)
  ingest/    HTTP transport (raw Fastify route), IngestService, EventIngest interface
  devices/, rooms/, alarms/   thin Nest controllers/services for the read endpoints and SSE
  metrics/   prom-client registry and the /metrics route
test/        e2e tests against a dedicated teton_test database
scripts/     load-test, restart-test, read-bench, seed-hour.sql
```

The rule throughout: **SQL selects the rows, `core/` does the math.**

## Testing

```bash
npm test                 # unit tests: the pure core/ functions
npm run test:e2e         # against Postgres (recreates the teton_test database; needs Compose Postgres)
npx vitest run --config ./vitest.config.e2e.ts test/cluster   # only the 1/2/4-worker test
npm run load -- --phases 5000:20,50000:30,5000:20    # load test; checks every acked event is in Postgres
node scripts/restart-test.ts                         # SIGKILL mid-load (needs the Compose stack)
```

- **The e2e tests cover the SQL behaviour:** late events and carry-in, clock skew, duplicates
  across batches, concurrent workers (dedup race, deadlock-free sorted inserts, alarm ids in
  commit order) and SSE catch-up.
- **`test/cluster.e2e-spec.ts` runs the built service with `WORKERS=1`, `2` and `4`.** It builds
  `dist/` first. Each request uses a new connection and the cluster assigns them round-robin, so
  requests reach every worker. It checks: duplicates stored once, the same reads from every
  worker, alarms reaching SSE clients on every worker once and in order, merged metrics, and a
  killed worker being replaced.
- **The restart test passes only if:**
  - every SSE consumer received exactly the committed alarms after reconnecting with
    `Last-Event-ID`;
  - every 2xx event is in Postgres.
- **One load client tops out at about 5–9k events/s.** For bursts, run several in parallel
  with `--connections 1024`. With the default 256 connections, the client's own connection
  limit caps the rate (see the results below).

## Measured results

Re-run on 2026-11-16 from a fresh stack (`docker compose down -v`), each test on empty tables.
Dev laptop: Windows, 16 cores, 4 workers, service native, Postgres in Docker.

**Challenge eval (`eval/check.py`, 50 devices):**

| Scenario | Events accepted | Alarms / distinct falls | Why the counts differ |
|---|---|---|---|
| baseline | 2,993 / 2,993 | 47 / 50 | 3 merged by the 5 s window |
| burst | 12,457 / 12,457 | 145 / 160 | 15 merged by the 5 s window |
| offline | 4,906 / 4,906 | 55 / 58 | 3 merged by the 5 s window |
| adversarial | 16,095 / 16,095 | 178 / 198 | 20 merged by the 5 s window |

- **No fall was lost.** Replaying the stored fall timestamps through the dedup rule gives exactly
  the alarm counts above.
- **The differences come from the generator.** It sends jitter copies with an identical `ts`
  and counts every distinct `ts` as a separate fall, so separate falls of one device within 5 s
  show up as "missing".
- **No duplicates, no rejections:** 0 duplicates and 0 rejected events in every scenario.
- **Fast alarms:** every alarm was pushed to SSE within 100 ms.

**Load test, 10x burst** (4 parallel clients, `--phases 2500:20,12500:30,2500:20 --connections 1024`):

| | Result |
|---|---|
| Throughput | 10k events/s sustained. Burst peak ~19.7k events/s, limited by the laptop (load clients + service + Postgres on one machine), not by the batch writer: commits avg 16 ms, batches avg 70 events |
| Lost events | 0. All 990,329 acknowledged events are in Postgres |
| Persist latency (receipt → commit), in the service | 99% < 250 ms, 100% < 500 ms, `fall_warn` the same |
| Alarm latency (receipt → SSE push), in the service | avg 129 ms, 100% < 500 ms |
| Client-side latency | multi-second during the burst: requests queue in the client and in the OS before the service receives them |

With the default 256 connections per client, throughput stopped at about 10.7k events/s. That
is Little's law, not a service limit: 1,024 open requests ÷ ~110 ms per request ≈ 9.6k
requests/s.

**Hard kill** (`scripts/restart-test.ts`: Compose stack, SIGKILL at 15 s of a 2k/s run, back
after 7.6 s): **PASS.**
- **SSE:** all 4 clients received all 1,035 alarms after reconnecting with `Last-Event-ID`,
  with 0 missing, doubled or out of order.
- **Events:** all 76,834 acknowledged events are in Postgres. 3,166 requests sent during the
  downtime failed on the sender side, and 17 events were stored whose reply was lost in the
  kill (the safe direction).

## Known limitations

- **Events sent while the service is down are lost on the sender side.** The generator doesn't
  retry; an MQTT broker (QoS 1, acknowledged after commit) would remove that.
- **An event that passes validation but fails in Postgres is retried forever.** Validation
  covers every known case. Production would classify database errors and dead-letter the
  offending event.
- **No load shedding.** During a long Postgres outage the in-memory queue keeps growing.
- **Devices with a large but constant clock offset show as unhealthy.** Clocks more than 5 min
  fast are treated as faulty rather than learned.
