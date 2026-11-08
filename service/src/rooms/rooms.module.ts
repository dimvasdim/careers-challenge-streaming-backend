import { Module } from '@nestjs/common';
import { RoomOccupancyController } from './room-occupancy.controller.js';

@Module({
  controllers: [RoomOccupancyController],
})
export class RoomsModule {}
