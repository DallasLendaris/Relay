import { randomUUID } from 'node:crypto';
import { createApi, type ApiDeps, type ApiEvent, type ApiResult } from './index.js';

/** The stores the API needs that come from AWS (Aurora). Wired by the orchestrator. */
export type StoreDeps = Pick<ApiDeps, 'devices' | 'places' | 'alerts' | 'geocoder'>;
export type StoreFactory = () => StoreDeps | Promise<StoreDeps>;

/**
 * Builds a Lambda handler. The factory runs once, on the first request, so cold
 * starts that never receive traffic do not open database connections.
 */
export function createHandler(buildStores: StoreFactory): (event: ApiEvent) => Promise<ApiResult> {
  let route: ((event: ApiEvent) => Promise<ApiResult>) | undefined;
  return async (event) => {
    route ??= createApi({
      ...(await buildStores()),
      clock: { now: () => new Date() },
      newId: randomUUID,
    });
    return route(event);
  };
}

/**
 * TODO: replace with the real factory that builds the Postgres (Aurora) stores and
 * the Amazon Location geocoder from environment variables (DB secret ARN, etc.).
 */
const awsStores: StoreFactory = () => {
  throw new Error('AWS adapters are not wired yet (see TODO in backend/src/api/handler.ts)');
};

export const handler = createHandler(awsStores);
