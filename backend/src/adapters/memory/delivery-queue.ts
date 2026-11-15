import type { DeliveryJob, DeliveryQueue } from '../../ports.js';

/** In-memory DeliveryQueue. `jobs` is readable so tests can inspect what was queued. */
export class MemoryDeliveryQueue implements DeliveryQueue {
  readonly jobs: DeliveryJob[] = [];
  /** Size of each enqueue call, to check batching. */
  readonly batchSizes: number[] = [];

  async enqueue(jobs: DeliveryJob[]): Promise<void> {
    this.batchSizes.push(jobs.length);
    this.jobs.push(...jobs);
  }
}
