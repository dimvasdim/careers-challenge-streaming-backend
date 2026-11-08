import { Module } from '@nestjs/common';
import { DeviceHealthController } from './device-health.controller.js';

@Module({
  controllers: [DeviceHealthController],
})
export class DevicesModule {}
