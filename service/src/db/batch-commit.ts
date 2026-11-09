import type pg from 'pg';
import { type BatchResult, matchInserted } from '../core/batch-writer.js';
import type { IngestEvent } from '../core/event.js';
import { type FallEvent, planAlarms } from '../core/fall-incidents.js';
import {
  loadNearbyAlarms,
  lockAlarms,
  notifyAlarms,
  upsertAlarms,
} from './alarms-repository.js';
import { insertEvents } from './events-repository.js';

/**
 * Writes one batch in a single transaction: the events, then the alarms their
 * new fall_warns create or update. Events and alarms commit together, so a
 * crash can never leave a fall persisted without its alarm. Only newly
 * inserted events count: a replayed duplicate never reaches the dedup.
 *
 * Several of these can run at once (one per worker process): see lockAlarms
 * for alarms, and the sort below for duplicate events.
 */
export async function commitBatch(
  pool: pg.Pool,
  batch: IngestEvent[],
): Promise<BatchResult> {
  // Rows are inserted in array order, each taking a lock on its unique key.
  // In one fixed order, two batches sharing duplicates (a replay arriving at
  // two workers) wait for each other instead of deadlocking.
  const events = [...batch].sort(byIdentityKey);

  const client = await pool.connect();
  let broken: Error | undefined;
  try {
    await client.query('BEGIN');
    const inserted = await insertEvents(client, events);

    const ids = matchInserted(events, inserted);
    const falls = events.filter(
      (e, i): e is FallEvent => e.type === 'fall_warn' && ids[i] !== undefined,
    );

    let alarms: BatchResult['alarms'] = [];
    if (falls.length > 0) {
      await lockAlarms(client);
      const deviceIds = [...new Set(falls.map((f) => f.deviceId))];
      const ts = falls.map((f) => f.tsMs);
      const existing = await loadNearbyAlarms(
        client,
        deviceIds,
        Math.min(...ts),
        Math.max(...ts),
      );
      const plan = planAlarms(existing, falls);
      // New alarms in ts order, so ids within a batch follow ts.
      plan.upserts.sort((a, b) => a.tsMs - b.tsMs);
      await upsertAlarms(client, plan.upserts);
      alarms = plan.created.sort((a, b) => a.id - b.id);
      if (alarms.length > 0) await notifyAlarms(client);
    }

    await client.query('COMMIT');
    return { inserted, alarms };
  } catch (err) {
    // The BatchWriter retries the whole batch; nothing of it was committed.
    // (If only the COMMIT reply was lost, the retry finds every event already
    // inserted; the alarms were committed and notified, so they still go out.)
    await client.query('ROLLBACK').catch((rollbackErr: Error) => {
      broken = rollbackErr; // connection is gone: don't return it to the pool
    });
    throw err;
  } finally {
    client.release(broken);
  }
}

function byIdentityKey(a: IngestEvent, b: IngestEvent): number {
  if (a.deviceId !== b.deviceId) return a.deviceId < b.deviceId ? -1 : 1;
  return a.tsMs - b.tsMs || a.seq - b.seq;
}
