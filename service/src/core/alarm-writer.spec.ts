import { AlarmWriter } from './alarm-writer.js';
import type { Alarm, FallChange } from './fall-incidents.js';

const T0 = Date.parse('2026-05-23T18:00:00.000Z');

const alarm = (device: string, tsMs = T0, confidence = 0.8): Alarm => ({
  id: 0,
  eventId: `${device}:${tsMs}`,
  deviceId: device,
  roomId: 'room_000',
  tsMs,
  confidence,
  receivedAtMs: tsMs,
});
const created = (a: Alarm): FallChange => ({ kind: 'new', alarm: a });
const raised = (a: Alarm): FallChange => ({ kind: 'raised', alarm: a });

/** In-memory alarms table: unique on eventId, sequential ids, max confidence. */
function fakeTable() {
  const rows = new Map<string, { id: number; confidence: number }>();
  let nextId = 1;
  let failNext = 0;
  const calls: string[][] = [];
  const upsertAlarms = async (alarms: Alarm[]) => {
    await Promise.resolve();
    calls.push(alarms.map((a) => a.eventId));
    if (failNext > 0) {
      failNext--;
      throw new Error('connection lost');
    }
    return alarms.map((a) => {
      const row = rows.get(a.eventId) ?? { id: nextId++, confidence: 0 };
      row.confidence = Math.max(row.confidence, a.confidence);
      rows.set(a.eventId, row);
      return { id: row.id, eventId: a.eventId };
    });
  };
  return {
    rows,
    calls,
    upsertAlarms,
    failTimes: (n: number) => (failNext = n),
  };
}

function setup() {
  const table = fakeTable();
  const published: Alarm[][] = [];
  const writer = new AlarmWriter({
    upsertAlarms: table.upsertAlarms,
    onPersisted: (alarms) => published.push([...alarms]),
    retryBaseMs: 1,
    retryMaxMs: 2,
  });
  return { table, published, writer };
}

describe('AlarmWriter', () => {
  it('publishes new alarms only after they are written, with ids in ts order', async () => {
    const { table, published, writer } = setup();
    const late = alarm('dev_b', T0 - 1000);
    const live = alarm('dev_a', T0);
    writer.enqueue([created(live), created(late)]);
    expect(published).toEqual([]); // nothing before the commit
    await writer.drain();
    expect(published).toHaveLength(1);
    expect(published[0].map((a) => a.deviceId)).toEqual(['dev_b', 'dev_a']);
    expect(published[0].map((a) => a.id)).toEqual([1, 2]);
    expect(table.rows.size).toBe(2);
  });

  it('writes a confidence raise without publishing again', async () => {
    const { table, published, writer } = setup();
    const a = alarm('dev_a');
    writer.enqueue([created(a)]);
    await writer.drain();
    a.confidence = 0.95; // FallIncidents mutates the shared object
    writer.enqueue([raised(a)]);
    await writer.drain();
    expect(published).toHaveLength(1);
    expect(table.rows.get(a.eventId)?.confidence).toBe(0.95);
  });

  it('folds a raise into a still-queued insert', async () => {
    const { table, published, writer } = setup();
    const a = alarm('dev_a');
    writer.enqueue([created(a)]);
    a.confidence = 0.95;
    writer.enqueue([raised(a)]);
    await writer.drain();
    expect(table.calls).toEqual([[a.eventId]]);
    expect(published.flat().map((x) => x.eventId)).toEqual([a.eventId]);
    expect(table.rows.get(a.eventId)?.confidence).toBe(0.95);
  });

  it('batches alarms that arrive while a write is in flight', async () => {
    const { table, writer } = setup();
    writer.enqueue([created(alarm('dev_a'))]);
    await new Promise((resolve) => setImmediate(resolve)); // first write starts
    writer.enqueue([created(alarm('dev_b'))]);
    writer.enqueue([created(alarm('dev_c'))]);
    await writer.drain();
    expect(table.calls.map((c) => c.length)).toEqual([1, 2]);
  });

  it('retries a failed write and publishes each alarm exactly once', async () => {
    const { table, published, writer } = setup();
    table.failTimes(2);
    writer.enqueue([created(alarm('dev_a'))]);
    await writer.drain();
    expect(table.calls).toHaveLength(3);
    expect(published.flat()).toHaveLength(1);
    expect(writer.depth()).toBe(0);
  });
});
