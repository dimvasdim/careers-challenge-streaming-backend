import { Module } from '@nestjs/common';
import { AlarmsModule } from './alarms/alarms.module.js';
import { DatabaseModule } from './db/database.module.js';
import { DevicesModule } from './devices/devices.module.js';
import { IngestModule } from './ingest/ingest.module.js';
import { RoomsModule } from './rooms/rooms.module.js';

@Module({
  imports: [
    DatabaseModule,
    IngestModule,
    DevicesModule,
    RoomsModule,
    AlarmsModule,
  ],
})
export class AppModule {}
