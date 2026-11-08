import {
  Controller,
  Get,
  Inject,
  NotFoundException,
  Param,
} from '@nestjs/common';
import type pg from 'pg';
import { computeHealth, type DeviceHealth } from '../core/device-health.js';
import { PG_POOL } from '../db/database.module.js';
import { loadHealthInput } from '../db/read-queries.js';

@Controller('devices')
export class DeviceHealthController {
  constructor(@Inject(PG_POOL) private readonly pool: pg.Pool) {}

  @Get(':deviceId/health')
  async health(@Param('deviceId') deviceId: string): Promise<DeviceHealth> {
    const now = Date.now();
    const input = await loadHealthInput(this.pool, deviceId, now);
    if (!input) throw new NotFoundException(`unknown device ${deviceId}`);
    return computeHealth(input, now);
  }
}
