import { ALARM_HISTORY_MS, type Alarm } from './fall-incidents.js';

/** Response shape of an alarm, for GET /alarms and the SSE feed. */
export interface AlarmJson {
  event_id: string;
  room_id: string;
  ts: string;
  confidence: number;
}

export function toAlarmJson(a: Alarm): AlarmJson {
  return {
    event_id: a.eventId,
    room_id: a.roomId,
    ts: new Date(a.tsMs).toISOString(),
    confidence: a.confidence,
  };
}

/**
 * `since` for GET /alarms: epoch seconds (`since=0`, `since=1779562429.5`) or an
 * ISO 8601 date. Missing means "everything in memory". Returns ms, or undefined
 * if the value is invalid.
 */
export function parseSince(raw: unknown): number | undefined {
  if (raw === undefined || raw === '') return 0;
  if (typeof raw !== 'string') return undefined;
  const asNumber = Number(raw);
  if (Number.isFinite(asNumber)) return asNumber * 1000;
  const asDate = Date.parse(raw);
  return Number.isNaN(asDate) ? undefined : asDate;
}

export type AlarmListener = (alarms: Alarm[]) => void;

/**
 * Persisted alarms of the last hour, plus live fan-out to subscribers.
 * Only alarms already committed to Postgres are published here.
 */
export class AlarmFeed {
  /** Persisted alarms sorted by ts, pruned after 1h. */
  private readonly alarms: Alarm[] = [];
  /** Highest id ever pruned: memory is complete only for ids above it. */
  private maxPrunedId = 0;
  private maxId = 0;
  private readonly listeners = new Set<AlarmListener>();

  /** Startup: fills memory from the alarms table without pushing to anyone. */
  load(alarms: Alarm[], maxPrunedId: number): void {
    for (const a of alarms) this.insert(a);
    this.maxPrunedId = Math.max(this.maxPrunedId, maxPrunedId);
  }

  /** Adds newly persisted alarms (sorted by id) and pushes them to subscribers. */
  publish(alarms: Alarm[], nowMs: number): void {
    if (alarms.length === 0) return;
    for (const a of alarms) this.insert(a);
    this.prune(nowMs - ALARM_HISTORY_MS);
    for (const listener of this.listeners) listener(alarms);
  }

  /** Alarms with ts >= sinceMs, sorted by ts. */
  since(sinceMs: number): Alarm[] {
    let lo = 0;
    let hi = this.alarms.length;
    while (lo < hi) {
      const mid = (lo + hi) >>> 1;
      if (this.alarms[mid].tsMs < sinceMs) lo = mid + 1;
      else hi = mid;
    }
    return this.alarms.slice(lo);
  }

  /**
   * Alarms with id > afterId in id order, for SSE catch-up, or undefined if
   * some of them were already pruned (then the caller asks Postgres).
   */
  after(afterId: number): Alarm[] | undefined {
    if (afterId < this.maxPrunedId) return undefined;
    return this.alarms
      .filter((a) => a.id > afterId)
      .sort((x, y) => x.id - y.id);
  }

  /** Id of the newest persisted alarm (0 if none). */
  lastId(): number {
    return this.maxId;
  }

  subscribe(listener: AlarmListener): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  subscriberCount(): number {
    return this.listeners.size;
  }

  private insert(alarm: Alarm): void {
    let i = this.alarms.length;
    while (i > 0 && this.alarms[i - 1].tsMs > alarm.tsMs) i--; // late ones are rare
    this.alarms.splice(i, 0, alarm);
    if (alarm.id > this.maxId) this.maxId = alarm.id;
  }

  private prune(cutoffMs: number): void {
    let drop = 0;
    while (drop < this.alarms.length && this.alarms[drop].tsMs < cutoffMs) {
      if (this.alarms[drop].id > this.maxPrunedId)
        this.maxPrunedId = this.alarms[drop].id;
      drop++;
    }
    if (drop > 0) this.alarms.splice(0, drop);
  }
}
