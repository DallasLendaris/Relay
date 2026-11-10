/**
 * Child process of t4: runs one kernel against DynamoDB Local and Postgres and sends the result to
 * the parent (see ../../lib/child.ts). Args: --task NAME --params FILE.json --scratch DIR
 * Env: RELAY_T4_DYNAMO, RELAY_T4_PG, RELAY_T4_NAMES (JSON).
 */
import { readFileSync } from 'node:fs';
import { parseArgs } from '../../../mock-common.js';
import { runWorkerMain } from '../../lib/child.js';
import { makeRealBackend } from './backend.js';
import { KERNELS, type TaskName } from './kernels.js';
import type { TableNames } from './tables.js';

runWorkerMain(async () => {
  const args = parseArgs(process.argv.slice(2));
  const task = args['task'] as TaskName | undefined;
  const paramsFile = args['params'];
  const scratch = args['scratch'];
  const kernel = task ? KERNELS[task] : undefined;
  if (!kernel || !paramsFile || !scratch)
    throw new Error(`bad t4 worker args: ${process.argv.slice(2).join(' ')}`);
  const env = process.env;
  const dynamoEndpoint = env['RELAY_T4_DYNAMO'];
  const postgresUrl = env['RELAY_T4_PG'];
  const namesJson = env['RELAY_T4_NAMES'];
  if (!dynamoEndpoint || !postgresUrl || !namesJson)
    throw new Error('missing RELAY_T4_* environment');
  const backend = makeRealBackend({
    dynamoEndpoint,
    postgresUrl,
    names: JSON.parse(namesJson) as TableNames,
    scratchDir: scratch,
  });
  try {
    const params = JSON.parse(readFileSync(paramsFile, 'utf8')) as never;
    return await kernel(backend, params);
  } finally {
    await backend.close();
  }
});
