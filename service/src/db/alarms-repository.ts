import type pg from 'pg';
import { type Alarm, FALL_DEDUP_WINDOW_MS } from '../core/fall-incidents.js';

type Db = pg.Pool | pg.PoolClient;

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

/**
 * Per-device advisory locks, held until the transaction ends. Two transactions
 * handling falls of the same device then run "look for a nearby alarm, then
 * insert" one after the other, so two copies of one fall can't both create an
 * alarm. Different devices never wait for each other. Locks are taken in key
 * order, so two transactions locking several devices can't deadlock.
 * (A hash collision only makes two devices wait for each other occasionally.)
 */
export async function lockDevices(
  db: pg.PoolClient,
  deviceIds: string[],
): Promise<void> {
  await db.query(
    `SELECT pg_advisory_xact_lock(key)
     FROM (SELECT DISTINCT hashtext(device_id) AS key
           FROM unnest($1::text[]) AS device_id
           ORDER BY key OFFSET 0) AS keys`,
    [deviceIds],
  );
}

/** Alarms of these devices with ts within the dedup window of [fromMs, toMs]. */
export async function loadNearbyAlarms(
  db: Db,
  deviceIds: string[],
  fromMs: number,
  toMs: number,
): Promise<Alarm[]> {
  const { rows } = await db.query<AlarmRow>(
    `SELECT ${COLUMNS} FROM alarms
     WHERE device_id = ANY($1) AND ts BETWEEN $2 AND $3`,
    [
      deviceIds,
      new Date(fromMs - FALL_DEDUP_WINDOW_MS).toISOString(),
      new Date(toMs + FALL_DEDUP_WINDOW_MS).toISOString(),
    ],
  );
  return rows.map(toAlarm);
}

// New incidents are inserted; existing ones keep the higher confidence.
// RETURNING gives each row's id (also on conflict).
const UPSERT_ALARMS = `
  INSERT INTO alarms (event_id, device_id, room_id, ts, confidence, received_at)
  SELECT * FROM unnest(
    $1::text[], $2::text[], $3::text[], $4::timestamptz[], $5::float8[], $6::timestamptz[]
  )
  ON CONFLICT (event_id) DO UPDATE SET confidence = GREATEST(alarms.confidence, EXCLUDED.confidence)
  RETURNING id, event_id`;

/** Upserts the alarms (one statement) and sets each one's `id`. */
export async function upsertAlarms(db: Db, alarms: Alarm[]): Promise<void> {
  if (alarms.length === 0) return;
  const { rows } = await db.query<{ id: string; event_id: string }>(
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
  const idByEventId = new Map(rows.map((r) => [r.event_id, Number(r.id)]));
  for (const a of alarms) a.id = idByEventId.get(a.eventId) ?? a.id;
}

/** GET /alarms: alarms with ts >= sinceMs, sorted by ts. */
export async function listAlarmsSince(
  db: Db,
  sinceMs: number,
): Promise<Alarm[]> {
  const { rows } = await db.query<AlarmRow>(
    `SELECT ${COLUMNS} FROM alarms WHERE ts >= $1 ORDER BY ts, id`,
    [new Date(sinceMs).toISOString()],
  );
  return rows.map(toAlarm);
}

/** SSE catch-up: alarms with id > afterId, in id order. */
export async function loadAlarmsAfter(
  db: Db,
  afterId: number,
): Promise<Alarm[]> {
  const { rows } = await db.query<AlarmRow>(
    `SELECT ${COLUMNS} FROM alarms WHERE id > $1 ORDER BY id`,
    [afterId],
  );
  return rows.map(toAlarm);
}
