-- Device health inputs for one device (pgbench script; :dev = device number).
-- Clock offset from the newest sane event, first seen, last heartbeat, and
-- heartbeat timestamps in the last 300s of the device clock. In the service
-- the device id is a query parameter; here it is a constant expression.
\set dev random(0, 4999)
WITH clock AS (
  SELECT received_at - ts AS clock_offset FROM events
  WHERE device_id = 'dev_' || lpad(:dev::text, 4, '0')
    AND received_at - ts BETWEEN interval '-5 min' AND interval '5 min'
  ORDER BY ts DESC LIMIT 1
),
device_now AS (SELECT now() - coalesce((SELECT clock_offset FROM clock), interval '0') AS t)
SELECT
  (SELECT ts FROM events WHERE device_id = 'dev_' || lpad(:dev::text, 4, '0')
    ORDER BY ts LIMIT 1) AS first_seen,
  (SELECT clock_offset FROM clock) AS clock_offset,
  (SELECT ts FROM events
    WHERE device_id = 'dev_' || lpad(:dev::text, 4, '0') AND type = 'heartbeat'
      AND ts <= received_at + interval '5 min'
    ORDER BY ts DESC LIMIT 1) AS last_heartbeat,
  (SELECT array_agg(e.ts ORDER BY e.ts) FROM events e, device_now
    WHERE e.device_id = 'dev_' || lpad(:dev::text, 4, '0') AND e.type = 'heartbeat'
      AND e.ts > device_now.t - interval '300 s' AND e.ts <= device_now.t) AS heartbeats;
