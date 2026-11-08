import {
  BadRequestException,
  Controller,
  Get,
  NotFoundException,
  Param,
  Query,
} from '@nestjs/common';
import { ReadModels } from '../core/read-models.js';
import {
  OCCUPANCY_WINDOWS,
  type RoomOccupancy,
  isOccupancyWindow,
} from '../core/room-occupancy.js';

@Controller('rooms')
export class RoomOccupancyController {
  constructor(private readonly readModels: ReadModels) {}

  @Get(':roomId/occupancy')
  occupancy(
    @Param('roomId') roomId: string,
    @Query('window') window: unknown,
  ): RoomOccupancy {
    if (!isOccupancyWindow(window)) {
      throw new BadRequestException('window must be one of 1m, 5m, 1h');
    }
    const occupancy = this.readModels.roomOccupancy.getOccupancy(
      roomId,
      OCCUPANCY_WINDOWS[window],
      Date.now(),
    );
    if (!occupancy) throw new NotFoundException(`unknown room ${roomId}`);
    return occupancy;
  }
}
