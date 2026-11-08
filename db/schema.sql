-- Applied once by the postgres image on an empty data volume
-- (/docker-entrypoint-initdb.d). After changing it: `make db-reset`.

-- Every accepted event: the source of truth all read endpoints query.
-- Batches are inserted one at a time, so ids follow commit order.
CREATE TABLE events (
  id          BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  device_id   TEXT        NOT NULL,
  room_id     TEXT        NOT NULL,
  type        TEXT        NOT NULL CHECK (type IN
                ('heartbeat', 'presence', 'motion', 'sleep_state', 'fall_warn', 'net_status')),
  ts          TIMESTAMPTZ NOT NULL,          -- device clock, authoritative for ordering
  seq         BIGINT      NOT NULL,
  received_at TIMESTAMPTZ NOT NULL DEFAULT now(), -- server time at ingest (set by the service)

  -- Type-specific fields, NULL when not applicable to `type`.
  in_room     BOOLEAN,                       -- presence
  magnitude   DOUBLE PRECISION,              -- motion
  state       TEXT,                          -- sleep_state
  confidence  DOUBLE PRECISION,              -- fall_warn
  rssi        INTEGER,                       -- net_status

  -- Event identity: a replayed copy of the same event is a duplicate.
  -- Column order (device_id, ts, ...) lets the same index serve
  -- "events of device X in a ts range" (device health).
  UNIQUE (device_id, ts, seq)
);

-- Room occupancy: presence events of a room by ts (~5% of rows, so partial).
-- INCLUDE makes it covering (index-only scan): a room's points are spread over
-- many heap pages, so heap fetches made the 1h window ~6x slower under load.
CREATE INDEX events_presence_room_ts_idx ON events (room_id, ts) INCLUDE (in_room, id)
  WHERE type = 'presence';

-- One row per deduplicated fall (incident). `id` follows commit order and is
-- the SSE event id, so Last-Event-ID catch-up also returns late replayed falls.
CREATE TABLE alarms (
  id          BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  event_id    TEXT        NOT NULL UNIQUE,   -- `${device_id}:${first ts, ISO 8601}`
  device_id   TEXT        NOT NULL,
  room_id     TEXT        NOT NULL,
  ts          TIMESTAMPTZ NOT NULL,          -- first fall_warn's device ts
  confidence  DOUBLE PRECISION NOT NULL,     -- max over the incident's copies
  received_at TIMESTAMPTZ NOT NULL           -- server time the first copy was ingested
);
CREATE INDEX alarms_ts_idx ON alarms (ts);
-- Fall dedup: existing incidents of a device near a new fall_warn's ts.
CREATE INDEX alarms_device_ts_idx ON alarms (device_id, ts);
