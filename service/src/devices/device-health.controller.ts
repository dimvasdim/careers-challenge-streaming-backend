import { Controller, Get, NotFoundException, Param } from '@nestjs/common';
import type { DeviceHealth } from '../core/device-health.js';
import { ReadModels } from '../core/read-models.js';

@Controller('devices')
export class DeviceHealthController {
  constructor(private readonly readModels: ReadModels) {}

  @Get(':deviceId/health')
  health(@Param('deviceId') deviceId: string): DeviceHealth {
    const health = this.readModels.deviceHealth.getHealth(deviceId, Date.now());
    if (!health) throw new NotFoundException(`unknown device ${deviceId}`);
    return health;
  }
}
