/**
 * Ingest Lambda. EventBridge fires it every minute; it polls NWS several times
 * (about every 12 s) before returning. buildHandler takes the engine pieces
 * (normalize, parseFeed, apply, cellsFor) so the orchestrator can wire them in.
 */
import { S3Client } from '@aws-sdk/client-s3';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { DynamoSeenStore } from '../adapters/aws/dynamo-seen-store.js';
import { S3RawArchive } from '../adapters/aws/s3-raw-archive.js';
import { createNwsClient } from './nws-client.js';
import { pollLoop, type PollCounts, type PollDeps } from './poll.js';

/** The parts of PollDeps that the caller supplies. */
export type EngineDeps = Pick<
  PollDeps,
  'normalize' | 'parseFeed' | 'apply' | 'cellsFor' | 'onApplied'
>;

export interface HandlerEnv {
  RAW_BUCKET?: string;
  STATE_TABLE?: string;
  NWS_USER_AGENT?: string;
  POLL_INTERVAL_SECONDS?: string;
  POLLS_PER_INVOCATION?: string;
}

export interface LambdaContext {
  getRemainingTimeInMillis(): number;
}

export interface HandlerOverrides {
  env?: HandlerEnv;
  /** Replace the real NWS client, archive and seen store (tests). */
  base?: Pick<PollDeps, 'client' | 'archive' | 'seen'>;
  sleep?(ms: number): Promise<void>;
}

export function buildHandler(engine: EngineDeps, overrides: HandlerOverrides = {}) {
  return async (_event: unknown, context: LambdaContext): Promise<{ polls: PollCounts[] }> => {
    const env: HandlerEnv = overrides.env ?? process.env;
    const intervalMs = Number(env.POLL_INTERVAL_SECONDS ?? '12') * 1000;
    const polls = Number(env.POLLS_PER_INVOCATION ?? '5');

    let base = overrides.base;
    if (!base) {
      const { RAW_BUCKET, STATE_TABLE, NWS_USER_AGENT } = env;
      if (!RAW_BUCKET || !STATE_TABLE || !NWS_USER_AGENT) {
        throw new Error('RAW_BUCKET, STATE_TABLE and NWS_USER_AGENT must be set');
      }
      base = {
        // The client lives for this invocation, so If-None-Match carries across its polls.
        client: createNwsClient({ fetch, userAgent: NWS_USER_AGENT }),
        archive: new S3RawArchive(new S3Client({}), RAW_BUCKET),
        seen: new DynamoSeenStore(DynamoDBDocumentClient.from(new DynamoDBClient({})), STATE_TABLE),
      };
    }

    const results = await pollLoop(
      { ...base, ...engine },
      {
        polls,
        intervalMs,
        sleep: overrides.sleep,
        remainingMs: () => context.getRemainingTimeInMillis(),
      },
    );
    console.log(JSON.stringify({ event: 'invocation.done', polls: results }));
    return { polls: results };
  };
}

// Not wired yet: the orchestrator replaces this with buildHandler({ normalize, parseFeed, apply, cellsFor }).
// It exists so the CDK app can still bundle this file.
export const handler = async (_event: unknown, _context: LambdaContext): Promise<never> => {
  throw new Error('ingest handler is not wired yet: call buildHandler with the engine deps');
};
