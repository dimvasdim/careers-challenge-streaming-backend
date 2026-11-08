import { Injectable, type OnModuleInit } from '@nestjs/common';
import { HttpAdapterHost } from '@nestjs/core';
import type { FastifyError, FastifyInstance, FastifyReply } from 'fastify';
import type { EventIngest } from './event-ingest.js';
import { IngestService } from './ingest.service.js';

const JSON_TYPE = 'application/json; charset=utf-8';
const OK = '{"status":"ok"}';
const DUPLICATE = '{"status":"duplicate"}';

/**
 * POST /events, registered directly on Fastify rather than as a Nest
 * controller, so the per-event path skips Nest's guard/pipe/interceptor chain
 * (including any global enhancers added later).
 */
@Injectable()
export class HttpIngestTransport implements OnModuleInit {
  private readonly ingest: EventIngest;

  constructor(
    private readonly adapterHost: HttpAdapterHost,
    ingestService: IngestService,
  ) {
    this.ingest = ingestService;
  }

  onModuleInit(): void {
    const fastify = this.adapterHost.httpAdapter.getInstance<FastifyInstance>();

    fastify.route({
      method: 'POST',
      url: '/events',
      bodyLimit: 16 * 1024,
      handler: async (request, reply) => {
        const result = await this.ingest.ingest(request.body);
        if (result.status === 'rejected') {
          return sendJson(reply, 400, `{"error":"${result.reason}"}`);
        }
        return sendJson(reply, 200, result.status === 'inserted' ? OK : DUPLICATE);
      },
      // Body parsing errors (bad JSON, empty body, wrong content type, too large).
      errorHandler: (error: FastifyError, _request, reply) => {
        const status = error.statusCode ?? 500;
        const reason =
          status === 400 && error.code?.startsWith('FST_ERR_CTP') ? 'invalid_json' : error.code;
        return sendJson(reply, status, JSON.stringify({ error: reason ?? 'internal_error' }));
      },
    });
  }
}

function sendJson(reply: FastifyReply, status: number, body: string): FastifyReply {
  return reply.code(status).header('content-type', JSON_TYPE).send(body);
}
