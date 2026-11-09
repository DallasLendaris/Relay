/**
 * Delivery worker Lambda, triggered by the SQS delivery queue. Sends Expo pushes and
 * returns batchItemFailures so SQS retries only the jobs that failed.
 * Dependencies are built on the first invocation and reused while warm.
 */
import { RDSDataClient } from '@aws-sdk/client-rds-data';
import type { SQSBatchResponse, SQSEvent } from 'aws-lambda';
import { ExpoPushSender } from '../adapters/expo/expo-push-sender.js';
import { PostgresAlertStore } from '../adapters/postgres/alert-store.js';
import { loadSecretString } from '../config/secrets.js';
import { DataApiExecutor } from '../db/sql.js';
import type { Logger } from './types.js';
import { deliveryWorker } from './worker.js';

const logger: Logger = {
  info: (msg, f) => console.log(JSON.stringify({ level: 'info', msg, ...f })),
  warn: (msg, f) => console.warn(JSON.stringify({ level: 'warn', msg, ...f })),
  error: (msg, f) => console.error(JSON.stringify({ level: 'error', msg, ...f })),
};

type Deps = Parameters<typeof deliveryWorker>[1];
let deps: Promise<Deps> | undefined;

async function buildDeps(env: Record<string, string | undefined>): Promise<Deps> {
  const { DB_CLUSTER_ARN, DB_SECRET_ARN, DB_NAME, EXPO_ACCESS_TOKEN_SECRET_ARN } = env;
  if (!DB_CLUSTER_ARN || !DB_SECRET_ARN || !DB_NAME) {
    throw new Error('DB_CLUSTER_ARN, DB_SECRET_ARN and DB_NAME must be set');
  }
  // The Expo access token is optional. When set, it goes out as a bearer token.
  const token = EXPO_ACCESS_TOKEN_SECRET_ARN
    ? (await loadSecretString(EXPO_ACCESS_TOKEN_SECRET_ARN)).trim()
    : null;
  const expoFetch: typeof fetch = token
    ? (input, init) => {
        const headers = new Headers(init?.headers);
        headers.set('Authorization', `Bearer ${token}`);
        return fetch(input, { ...init, headers });
      }
    : fetch;
  const db = new DataApiExecutor(new RDSDataClient({}), DB_CLUSTER_ARN, DB_SECRET_ARN, DB_NAME);
  return {
    sender: new ExpoPushSender({ fetch: expoFetch }),
    alerts: new PostgresAlertStore(db),
    logger,
  };
}

export const handler = async (event: SQSEvent): Promise<SQSBatchResponse> => {
  // Clear the cached promise on failure so the next invocation retries the build.
  deps ??= buildDeps(process.env).catch((err: unknown) => {
    deps = undefined;
    throw err;
  });
  return deliveryWorker(event, await deps);
};
