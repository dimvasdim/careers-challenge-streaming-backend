import { Module } from '@nestjs/common';
import { AlarmsController } from './alarms.controller.js';
import { AlarmsService } from './alarms.service.js';

@Module({
  controllers: [AlarmsController],
  providers: [AlarmsService],
  exports: [AlarmsService],
})
export class AlarmsModule {}
