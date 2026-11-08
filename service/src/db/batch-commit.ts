import type pg from 'pg';
import { type BatchResult, matchInserted } from '../core/batch-writer.js';
import type { IngestEvent } from '../core/event.js';
import { type FallEvent, planAlarms } from '../core/fall-incidents.js';
import {
  loadNearbyAlarms,
  lockDevices,
  upsertAlarms,
} from './alarms-repository.js';
import { insertEvents } from './events-repository.js';

/**
 * Writes one batch in a single transaction: the events, then the alarms their
 * new fall_warns create or update. Events and alarms commit together, so a
 * crash can never leave a fall persisted without its alarm. Only newly
 * inserted events count: a replayed duplicate never reaches the dedup.
 */
export async function commitBatch(
  pool: pg.Pool,
  events: IngestEvent[],
): Promise<BatchResult> {
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
      const deviceIds = [...new Set(falls.map((f) => f.deviceId))];
      await lockDevices(client, deviceIds);
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
    }

    await client.query('COMMIT');
    return { inserted, alarms };
  } catch (err) {
    // The BatchWriter retries the whole batch; nothing of it was committed.
    // (If only the COMMIT reply was lost, the retry finds every event already
    // inserted: the alarms exist but are not pushed live; SSE catch-up has them.)
    await client.query('ROLLBACK').catch((rollbackErr: Error) => {
      broken = rollbackErr; // connection is gone: don't return it to the pool
    });
    throw err;
  } finally {
    client.release(broken);
  }
}
