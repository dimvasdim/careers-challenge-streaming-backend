-- Room occupancy inputs for the 1h window (worst case): the carry-in point
-- before the window plus every presence point inside it (pgbench script;
-- :room = room number; a query parameter in the service).
\set room random(0, 2499)
(SELECT ts, in_room FROM events
  WHERE room_id = 'room_' || lpad(:room::text, 3, '0') AND type = 'presence'
    AND ts < now() - interval '1 hour'
  ORDER BY ts DESC, id DESC LIMIT 1)
UNION ALL
(SELECT ts, in_room FROM events
  WHERE room_id = 'room_' || lpad(:room::text, 3, '0') AND type = 'presence'
    AND ts >= now() - interval '1 hour'
  ORDER BY ts, id);
