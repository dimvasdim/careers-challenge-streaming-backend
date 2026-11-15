import {
  BadRequestException,
  Controller,
  Get,
  Inject,
  Param,
  Query,
} from '@nestjs/common';
import type pg from 'pg';
import {
  computeOccupancy,
  isOccupancyWindow,
  OCCUPANCY_WINDOWS,
  type RoomOccupancy,
} from '../core/room-occupancy.js';
import { PG_READ_POOL } from '../db/database.module.js';
import { loadPresencePoints } from '../db/read-queries.js';
import { metrics } from '../metrics/metrics.js';

@Controller('rooms')
export class RoomOccupancyController {
  constructor(@Inject(PG_READ_POOL) private readonly pool: pg.Pool) {}

  /** A room without presence events (or unknown) is absent: { false, 0 }. */
  @Get(':roomId/occupancy')
  async occupancy(
    @Param('roomId') roomId: string,
    @Query('window') window: unknown,
  ): Promise<RoomOccupancy> {
    if (!isOccupancyWindow(window)) {
      throw new BadRequestException('window must be one of 1m, 5m, 1h');
    }
    const windowMs = OCCUPANCY_WINDOWS[window];
    const now = Date.now();
    const points = await metrics.timeRead('occupancy', () =>
      loadPresencePoints(this.pool, roomId, now - windowMs),
    );
    return computeOccupancy(points, windowMs, now);
  }
}
