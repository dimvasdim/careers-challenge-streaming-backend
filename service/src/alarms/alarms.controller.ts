import {
  BadRequestException,
  Controller,
  Get,
  Headers,
  Logger,
  type MessageEvent,
  Query,
  Sse,
} from '@nestjs/common';
import { Observable } from 'rxjs';
import { loadConfig } from '../config.js';
import { type AlarmJson, parseSince, toAlarmJson } from '../core/alarm-feed.js';
import { openSubscription } from '../core/alarm-subscription.js';
import { ReadModels } from '../core/read-models.js';
import { AlarmsService } from './alarms.service.js';

@Controller('alarms')
export class AlarmsController {
  private readonly logger = new Logger('Alarms');
  private readonly keepaliveMs = loadConfig().sseKeepaliveMs;

  constructor(
    private readonly readModels: ReadModels,
    private readonly alarms: AlarmsService,
  ) {}

  /** Deduplicated falls with ts >= since (last hour, from memory), sorted by ts. */
  @Get()
  list(@Query('since') since: unknown): { alarms: AlarmJson[] } {
    const sinceMs = parseSince(since);
    if (sinceMs === undefined) {
      throw new BadRequestException(
        'since must be epoch seconds or an ISO 8601 date',
      );
    }
    return {
      alarms: this.readModels.alarmFeed.since(sinceMs).map(toAlarmJson),
    };
  }

  /**
   * Live alarms over SSE. Each message id is the alarm's id; a reconnecting
   * client sends it back as Last-Event-ID (or ?last_event_id=) and first gets
   * every alarm it missed.
   */
  @Sse('stream')
  stream(
    @Headers('last-event-id') header: string | undefined,
    @Query('last_event_id') query: string | undefined,
  ): Observable<MessageEvent> {
    const lastEventId = parseLastEventId(header ?? query);
    return new Observable<MessageEvent>((subscriber) => {
      const close = openSubscription({
        feed: this.readModels.alarmFeed,
        lastEventId,
        loadAfter: (afterId) => this.alarms.loadAfter(afterId),
        send: (alarm) =>
          subscriber.next({ id: String(alarm.id), data: toAlarmJson(alarm) }),
        onError: (err) => {
          this.logger.warn(
            `SSE catch-up failed: ${err instanceof Error ? err.message : String(err)}`,
          );
          subscriber.error(err);
        },
      });
      // A comment line (": keepalive"), not an event: Nest gives every event without
      // an id a generated one, which a client would send back as Last-Event-ID.
      const keepalive = setInterval(
        () => subscriber.next({ comment: 'keepalive' }),
        this.keepaliveMs,
      );
      return () => {
        clearInterval(keepalive);
        close();
      };
    });
  }
}

function parseLastEventId(raw: string | undefined): number | undefined {
  if (raw === undefined || raw === '') return undefined;
  const id = Number(raw);
  return Number.isSafeInteger(id) && id >= 0 ? id : undefined;
}
