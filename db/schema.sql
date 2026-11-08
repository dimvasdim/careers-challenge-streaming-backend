-- Applied once by the postgres image on an empty data volume
-- (/docker-entrypoint-initdb.d). After changing it: `make db-reset`.

-- Every accepted event. `id` is the replay cursor used by snapshots:
-- batches are inserted one at a time, so ids follow commit order.
CREATE TABLE events (
  id          BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  device_id   TEXT        NOT NULL,
  room_id     TEXT        NOT NULL,
  type        TEXT        NOT NULL CHECK (type IN
                ('heartbeat', 'presence', 'motion', 'sleep_state', 'fall_warn', 'net_status')),
  ts          TIMESTAMPTZ NOT NULL,          -- device clock, authoritative for ordering
  seq         BIGINT      NOT NULL,
  received_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  -- Type-specific fields, NULL when not applicable to `type`.
  in_room     BOOLEAN,                       -- presence
  magnitude   DOUBLE PRECISION,              -- motion
  state       TEXT,                          -- sleep_state
  confidence  DOUBLE PRECISION,              -- fall_warn
  rssi        INTEGER,                       -- net_status

  -- Event identity: a replayed copy of the same event is a duplicate.
  UNIQUE (device_id, seq, ts)
);
