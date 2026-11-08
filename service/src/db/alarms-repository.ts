import type pg from 'pg';
import type { Alarm } from '../core/fall-incidents.js';

// One statement per batch. A new incident is inserted; an existing one keeps
// the higher confidence. RETURNING gives every row's id (also on conflict),
// which keeps retries idempotent.
const UPSERT_ALARMS = `
  INSERT INTO alarms (event_id, device_id, room_id, ts, confidence, received_at)
  SELECT * FROM unnest(
    $1::text[], $2::text[], $3::text[], $4::timestamptz[], $5::float8[], $6::timestamptz[]
  )
  ON CONFLICT (event_id) DO UPDATE SET confidence = GREATEST(alarms.confidence, EXCLUDED.confidence)
  RETURNING id, event_id`;

const COLUMNS = 'id, event_id, device_id, room_id, ts, confidence, received_at';

interface AlarmRow {
  id: string;
  event_id: string;
  device_id: string;
  room_id: string;
  ts: Date;
  confidence: number;
  received_at: Date;
}

function toAlarm(r: AlarmRow): Alarm {
  return {
    id: Number(r.id),
    eventId: r.event_id,
    deviceId: r.device_id,
    roomId: r.room_id,
    tsMs: r.ts.getTime(),
    confidence: r.confidence,
    receivedAtMs: r.received_at.getTime(),
  };
}

export async function upsertAlarms(
  db: pg.Pool | pg.PoolClient,
  alarms: Alarm[],
): Promise<Array<{ id: number; eventId: string }>> {
  if (alarms.length === 0) return [];
  const result = await db.query<{ id: string; event_id: string }>(
    UPSERT_ALARMS,
    [
      alarms.map((a) => a.eventId),
      alarms.map((a) => a.deviceId),
      alarms.map((a) => a.roomId),
      alarms.map((a) => new Date(a.tsMs).toISOString()),
      alarms.map((a) => a.confidence),
      alarms.map((a) => new Date(a.receivedAtMs).toISOString()),
    ],
  );
  return result.rows.map((r) => ({ id: Number(r.id), eventId: r.event_id }));
}

/**
 * Startup: alarms with ts >= fromMs (id order), and the highest id of the
 * older ones, which memory will not hold.
 */
export async function loadRecentAlarms(
  db: pg.Pool,
  fromMs: number,
): Promise<{ alarms: Alarm[]; maxOlderId: number }> {
  const from = new Date(fromMs).toISOString();
  const recent = await db.query<AlarmRow>(
    `SELECT ${COLUMNS} FROM alarms WHERE ts >= $1 ORDER BY id`,
    [from],
  );
  const older = await db.query<{ max: string | null }>(
    'SELECT max(id) AS max FROM alarms WHERE ts < $1',
    [from],
  );
  return {
    alarms: recent.rows.map(toAlarm),
    maxOlderId: Number(older.rows[0].max ?? 0),
  };
}

/** SSE catch-up beyond memory: alarms with id > afterId, in id order. */
export async function loadAlarmsAfter(
  db: pg.Pool,
  afterId: number,
): Promise<Alarm[]> {
  const result = await db.query<AlarmRow>(
    `SELECT ${COLUMNS} FROM alarms WHERE id > $1 ORDER BY id`,
    [afterId],
  );
  return result.rows.map(toAlarm);
}
