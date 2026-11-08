-- Fills `events` with synthetic traffic for query measurements (EXPLAIN ANALYZE).
-- Default: 5000 devices x 1 event/s for the last hour = 18M rows, inserted in
-- time order like real ingest. Type mix matches event_generator/generate.py.
--
--   docker compose exec -T postgres psql -U teton -v devices=5000 -v seconds=3600 < service/scripts/seed-hour.sql
\if :{?devices} \else \set devices 5000 \endif
\if :{?seconds} \else \set seconds 3600 \endif
\timing on

INSERT INTO events (device_id, room_id, type, ts, seq, received_at,
                    in_room, magnitude, state, confidence, rssi)
SELECT
  'dev_' || lpad(d::text, 4, '0'),
  'room_' || lpad((d / 2)::text, 3, '0'),
  t.type,
  t.ts,
  s + 1,
  t.ts,
  CASE WHEN t.type = 'presence' THEN random() < 0.5 END,
  CASE WHEN t.type = 'motion' THEN round(random()::numeric, 2) END,
  CASE WHEN t.type = 'sleep_state' THEN (ARRAY['asleep', 'awake', 'unknown'])[1 + floor(random() * 3)::int] END,
  CASE WHEN t.type = 'fall_warn' THEN round((0.7 + random() * 0.29)::numeric, 2) END,
  CASE WHEN t.type = 'net_status' THEN -90 + floor(random() * 41)::int END
FROM generate_series(0, :seconds - 1) AS s
CROSS JOIN generate_series(0, :devices - 1) AS d
CROSS JOIN LATERAL (
  SELECT
    now() - make_interval(secs => :seconds - s) + make_interval(secs => random()) AS ts,
    -- weights 50/15/4/3/3/1 of 76, as in generate.py
    CASE
      WHEN r < 50 THEN 'heartbeat'
      WHEN r < 65 THEN 'motion'
      WHEN r < 69 THEN 'presence'
      WHEN r < 72 THEN 'sleep_state'
      WHEN r < 75 THEN 'net_status'
      ELSE 'fall_warn'
    END AS type
  -- Referencing d makes the subquery correlated, so random() runs once per row.
  FROM (SELECT random() * 76 + 0 * d AS r) AS w
) AS t;

ANALYZE events;
SELECT count(*) AS rows, pg_size_pretty(pg_total_relation_size('events')) AS total_size FROM events;
