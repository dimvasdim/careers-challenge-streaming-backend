-- EXPLAIN ANALYZE of the read queries for one device / room.
--   docker compose exec -T postgres psql -U teton < service/scripts/read-bench/explain.sql
\echo === health ===
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF)
WITH clock AS (
  SELECT received_at - ts AS clock_offset FROM events
  WHERE device_id = 'dev_' || lpad('1234', 4, '0')
    AND received_at - ts BETWEEN interval '-5 min' AND interval '5 min'
  ORDER BY ts DESC LIMIT 1
),
device_now AS (SELECT now() - coalesce((SELECT clock_offset FROM clock), interval '0') AS t)
SELECT
  (SELECT ts FROM events WHERE device_id = 'dev_' || lpad('1234', 4, '0')
    ORDER BY ts LIMIT 1) AS first_seen,
  (SELECT clock_offset FROM clock) AS clock_offset,
  (SELECT ts FROM events
    WHERE device_id = 'dev_' || lpad('1234', 4, '0') AND type = 'heartbeat'
      AND ts <= received_at + interval '5 min'
    ORDER BY ts DESC LIMIT 1) AS last_heartbeat,
  (SELECT array_agg(e.ts ORDER BY e.ts) FROM events e, device_now
    WHERE e.device_id = 'dev_' || lpad('1234', 4, '0') AND e.type = 'heartbeat'
      AND e.ts > device_now.t - interval '300 s' AND e.ts <= device_now.t) AS heartbeats;
\echo === occupancy ===
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF)
(SELECT ts, in_room FROM events
  WHERE room_id = 'room_' || lpad('617', 3, '0') AND type = 'presence'
    AND ts < now() - interval '1 hour'
  ORDER BY ts DESC, id DESC LIMIT 1)
UNION ALL
(SELECT ts, in_room FROM events
  WHERE room_id = 'room_' || lpad('617', 3, '0') AND type = 'presence'
    AND ts >= now() - interval '1 hour'
  ORDER BY ts, id);
