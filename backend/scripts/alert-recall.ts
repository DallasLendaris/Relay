/**
 * Operator recall: end one alert by hand (a wrong, mistaken or withdrawn alert).
 * Sets the stored alert to state "cancelled", re-signs it, and republishes the public JSON so the
 * CDN copy and the shard index drop it. It does NOT send a "cancelled" push: see
 * docs/runbooks/alert-recall.md.
 *
 *   DB_CLUSTER_ARN=... DB_SECRET_ARN=... DB_NAME=... \
 *   ALERT_SIGNING_KMS_KEY=alias/relay/<stage>/alert-signing-v<n> ALERT_SIGNING_KID=<stage>-alert-v<n> \
 *   PUBLIC_ALERTS_BUCKET=... PUBLIC_ALERTS_REGION=... \
 *     npx tsx backend/scripts/alert-recall.ts <alert-id> --reason "why" [--dry-run]
 *
 * Without PUBLIC_ALERTS_BUCKET the stored alert is still cancelled but nothing is republished
 * (the script says so). The reason is written to the structured log line `alert.recalled`.
 *
 * IAM the operator needs (an alert writer, like the ingest Lambda): kms:Sign on the current alert
 * signing key (ConfigStack output AlertSigningKeyArn), rds-data:ExecuteStatement and
 * secretsmanager:GetSecretValue on the cluster and a role secret that can write alerts
 * (relay/<stage>/db/relay_ingest), and s3:PutObject on the public alerts bucket when republishing.
 * Grant kms:Sign to the operator role for the recall only; nothing else holds it.
 */
import { pathToFileURL } from 'node:url';
import { RDSDataClient } from '@aws-sdk/client-rds-data';
import { S3Client } from '@aws-sdk/client-s3';
import { SigningAlertStore } from '../src/adapters/signing-alert-store.js';
import { PostgresAlertStore } from '../src/adapters/postgres/alert-store.js';
import { DataApiExecutor } from '../src/db/sql.js';
import type { Logger } from '../src/delivery/types.js';
import { H3_RESOLUTION } from '../src/geo/index.js';
import type { AlertStore } from '../src/ports.js';
import { AlertPublisher } from '../src/scale/publish/alert-publisher.js';
import { S3PublicObjectWriter } from '../src/scale/publish/s3-object-writer.js';
import { alertSignerFromEnv } from '../src/signing/kms.js';

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

export type RecallResult =
  | { ok: true; alertId: string; previousState: string; republished: boolean; dryRun: boolean }
  | { ok: false; error: string };

export interface RecallDeps {
  /** A store that signs on save (SigningAlertStore) wrapped around the real one. */
  store: AlertStore;
  /** Republishes the public alert JSON and shard indexes. Omit when no bucket is configured. */
  publisher?: Pick<AlertPublisher, 'publish'>;
  logger: Logger;
  now?: () => Date;
}

/**
 * Cancel one stored alert and republish it. Idempotent: recalling an already cancelled alert
 * re-saves and republishes it (a safe way to repair a failed publish).
 */
export async function recallAlert(
  deps: RecallDeps,
  alertId: string,
  reason: string,
  opts: { dryRun?: boolean } = {},
): Promise<RecallResult> {
  const why = reason.trim();
  if (!UUID.test(alertId)) return { ok: false, error: 'alert id must be a Relay alert UUID' };
  if (why.length < 5) return { ok: false, error: 'a reason of at least 5 characters is required' };
  const alert = await deps.store.get(alertId);
  if (!alert) return { ok: false, error: `no alert with id ${alertId}` };

  const base = {
    alertId: alert.id,
    sourceId: alert.sourceId,
    event: alert.event,
    previousState: alert.state,
    reason: why,
  };
  if (opts.dryRun) {
    deps.logger.info('alert.recall_dry_run', base);
    return {
      ok: true,
      alertId: alert.id,
      previousState: alert.state,
      republished: false,
      dryRun: true,
    };
  }

  // The signing store drops the summaries of a cancelled alert and signs the result.
  await deps.store.save({ ...alert, state: 'cancelled' });
  const saved = await deps.store.get(alert.id);
  if (!saved || saved.state !== 'cancelled') {
    return { ok: false, error: 'alert was not stored as cancelled; check the database' };
  }
  let republished = false;
  if (deps.publisher) {
    await deps.publisher.publish(saved);
    republished = true;
  }
  deps.logger.warn('alert.recalled', {
    ...base,
    republished,
    at: (deps.now?.() ?? new Date()).toISOString(),
  });
  return { ok: true, alertId: alert.id, previousState: alert.state, republished, dryRun: false };
}

export function parseArgs(argv: string[]): { alertId: string; reason: string; dryRun: boolean } {
  let alertId = '';
  let reason = '';
  let dryRun = false;
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i] ?? '';
    if (arg === '--dry-run') dryRun = true;
    else if (arg === '--reason') reason = argv[++i] ?? '';
    else if (arg.startsWith('--')) throw new Error(`Unknown option "${arg}"`);
    else if (alertId === '') alertId = arg;
    else throw new Error('Only one alert id may be given');
  }
  if (alertId === '' || reason.trim() === '') {
    throw new Error('Usage: alert-recall.ts <alert-id> --reason "why" [--dry-run]');
  }
  return { alertId, reason, dryRun };
}

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing environment variable ${name}`);
  return value;
}

const logger: Logger = {
  info: (msg, f) => console.log(JSON.stringify({ level: 'info', msg, ...f })),
  warn: (msg, f) => console.warn(JSON.stringify({ level: 'warn', msg, ...f })),
  error: (msg, f) => console.error(JSON.stringify({ level: 'error', msg, ...f })),
};

async function main(): Promise<void> {
  const { alertId, reason, dryRun } = parseArgs(process.argv.slice(2));
  const db = new DataApiExecutor(
    new RDSDataClient({}),
    requireEnv('DB_CLUSTER_ARN'),
    requireEnv('DB_SECRET_ARN'),
    requireEnv('DB_NAME'),
  );
  const inner = new PostgresAlertStore(db);
  const store = new SigningAlertStore(inner, alertSignerFromEnv(process.env));
  const bucket = process.env['PUBLIC_ALERTS_BUCKET'];
  const region = process.env['PUBLIC_ALERTS_REGION'];
  const publisher = bucket
    ? new AlertPublisher({
        writer: new S3PublicObjectWriter(new S3Client(region ? { region } : {}), bucket),
        alerts: inner,
        cellResolution: H3_RESOLUTION,
        logger,
      })
    : undefined;
  if (!publisher) logger.warn('alert.recall_no_publisher', { note: 'PUBLIC_ALERTS_BUCKET unset' });
  const result = await recallAlert({ store, publisher, logger }, alertId, reason, { dryRun });
  console.log(JSON.stringify(result));
  if (!result.ok) process.exit(1);
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch((err: unknown) => {
    console.error(
      JSON.stringify({
        level: 'error',
        msg: 'alert.recall_failed',
        error: err instanceof Error ? err.message : 'unknown',
      }),
    );
    process.exit(1);
  });
}
