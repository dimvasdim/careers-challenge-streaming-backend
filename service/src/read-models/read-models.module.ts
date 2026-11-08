import { Global, Module } from '@nestjs/common';
import { ReadModels } from '../core/read-models.js';

/** One shared instance of the in-memory read models for the whole process. */
@Global()
@Module({
  // A factory, not useValue: each app instance (e.g. a restart in tests) starts empty.
  providers: [{ provide: ReadModels, useFactory: () => new ReadModels() }],
  exports: [ReadModels],
})
export class ReadModelsModule {}
