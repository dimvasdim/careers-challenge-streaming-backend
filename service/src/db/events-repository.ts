import type pg from 'pg';
import type { InsertedRow } from '../core/batch-writer.js';
import type { IngestEvent } from '../core/event.js';

// One statement per batch: column arrays are unnested into rows.
// ON CONFLICT DO NOTHING skips duplicates (also identical rows within the
// batch); RETURNING lists only the rows actually inserted.
const INSERT_EVENTS = `
  INSERT INTO events (device_id, room_id, type, ts, seq, in_room, magnitude, state, confidence, rssi,
                      received_at)
  SELECT * FROM unnest(
    $1::text[], $2::text[], $3::text[], $4::timestamptz[], $5::bigint[],
    $6::boolean[], $7::float8[], $8::text[], $9::float8[], $10::integer[], $11::timestamptz[]
  )
  ON CONFLICT (device_id, seq, ts) DO NOTHING
  RETURNING id, device_id, seq, ts`;

export async function insertEvents(
  db: pg.Pool | pg.PoolClient,
  events: IngestEvent[],
): Promise<InsertedRow[]> {
  if (events.length === 0) return [];

  const deviceIds: string[] = [];
  const roomIds: string[] = [];
  const types: string[] = [];
  const timestamps: string[] = [];
  const seqs: number[] = [];
  const inRoom: (boolean | null)[] = [];
  const magnitude: (number | null)[] = [];
  const state: (string | null)[] = [];
  const confidence: (number | null)[] = [];
  const rssi: (number | null)[] = [];
  const receivedAt: string[] = [];

  for (const e of events) {
    deviceIds.push(e.deviceId);
    roomIds.push(e.roomId);
    types.push(e.type);
    timestamps.push(new Date(e.tsMs).toISOString());
    seqs.push(e.seq);
    inRoom.push(e.type === 'presence' ? e.inRoom : null);
    magnitude.push(e.type === 'motion' ? e.magnitude : null);
    state.push(e.type === 'sleep_state' ? e.state : null);
    confidence.push(e.type === 'fall_warn' ? e.confidence : null);
    rssi.push(e.type === 'net_status' ? e.rssi : null);
    receivedAt.push(new Date(e.receivedAtMs).toISOString());
  }

  const result = await db.query<{ id: string; device_id: string; seq: string; ts: Date }>(
    INSERT_EVENTS,
    [deviceIds, roomIds, types, timestamps, seqs, inRoom, magnitude, state, confidence, rssi, receivedAt],
  );

  // pg returns BIGINT as string; ids and seqs stay well below 2^53.
  return result.rows.map((r) => ({
    id: Number(r.id),
    deviceId: r.device_id,
    seq: Number(r.seq),
    tsMs: r.ts.getTime(),
  }));
}
