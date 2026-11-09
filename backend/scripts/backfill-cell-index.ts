/**
 * Copies every saved place from Aurora into the DynamoDB cell index. Safe to re-run.
 *
 *   DB_CLUSTER_ARN=... DB_SECRET_ARN=... DB_NAME=... CELL_INDEX_TABLE=... \
 *     npx tsx backend/scripts/backfill-cell-index.ts [--dry-run] [--page-size 1000]
 */
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { RDSDataClient } from '@aws-sdk/client-rds-data';
import { DataApiExecutor } from '../src/db/sql.js';
import { DynamoCellIndex, backfillCellIndex } from '../src/scale/cell-index/index.js';

function parseArgs(argv: string[]): { dryRun: boolean; pageSize: number } {
  let dryRun = false;
  let pageSize = 1000;
  for (let i = 0; i < argv.length; i++) {
    const flag = argv[i];
    if (flag === '--dry-run') dryRun = true;
    else if (flag === '--page-size') {
      pageSize = Number(argv[++i]);
      if (!Number.isInteger(pageSize) || pageSize < 1) throw new Error('--page-size must be >= 1');
    } else throw new Error(`Unknown option "${flag}". Use --dry-run or --page-size <n>`);
  }
  return { dryRun, pageSize };
}

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing environment variable ${name}`);
  return value;
}

async function main(): Promise<void> {
  const { dryRun, pageSize } = parseArgs(process.argv.slice(2));
  const db = new DataApiExecutor(
    new RDSDataClient({}),
    requireEnv('DB_CLUSTER_ARN'),
    requireEnv('DB_SECRET_ARN'),
    requireEnv('DB_NAME'),
  );
  const index = new DynamoCellIndex(
    DynamoDBDocumentClient.from(new DynamoDBClient({})),
    requireEnv('CELL_INDEX_TABLE'),
  );
  const counts = await backfillCellIndex({
    db,
    index,
    pageSize,
    dryRun,
    onPage: (c) => console.log(JSON.stringify({ msg: 'cellindex.backfill_progress', ...c })),
  });
  console.log(JSON.stringify({ msg: 'cellindex.backfill_done', ...counts }));
}

main().catch((err: unknown) => {
  console.error(
    JSON.stringify({
      level: 'error',
      msg: 'cellindex.backfill_failed',
      error: err instanceof Error ? err.message : 'unknown',
    }),
  );
  process.exit(1);
});
