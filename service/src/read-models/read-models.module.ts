import { Global, Module } from '@nestjs/common';
import { ReadModels } from '../core/read-models.js';

/** One shared instance of the in-memory read models for the whole process. */
@Global()
@Module({
  providers: [{ provide: ReadModels, useValue: new ReadModels() }],
  exports: [ReadModels],
})
export class ReadModelsModule {}
