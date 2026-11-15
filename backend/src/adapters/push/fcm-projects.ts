import { createHash } from 'node:crypto';
import type {
  NativePushMessage,
  NativePushResult,
  NativePushSender,
} from '../../scale/contracts.js';
import { noopLogger, type Logger } from '../../delivery/types.js';
import { TokenBucket } from './token-bucket.js';

/**
 * FCM project sharding (audit PERF-1). One Firebase project caps FCM HTTP v1 sends at about
 * 600,000 per minute, so 10M statewide pushes take 7 to 8 minutes through one. Android devices
 * are spread over N projects instead. An FCM registration token only works with the project (the
 * sender) the app registered with, so:
 *   - the server assigns each device a project (assignFcmProject) and tells the app,
 *   - the app registers its FCM token with that project and reports it with the token,
 *   - the device store and cell index keep it in PushTarget.project,
 *   - FcmProjectSender sends each message through its project's FcmSender and rate-limits every
 *     project on its own bucket.
 * A target without a project uses the first configured project, so existing devices keep working.
 */

/** Default per-project send rate: 9,000/s = 540,000/min, under the 600,000/min default quota. */
export const DEFAULT_FCM_PROJECT_RATE = 9_000;

/**
 * Default per-project budget for the whole sender fleet: each task gets this divided by the number
 * of running sender tasks, so the fleet as a whole stays under the project quota at any size.
 */
export const DEFAULT_FCM_PROJECT_FLEET_RATE = DEFAULT_FCM_PROJECT_RATE;

/** Parses FCM_PROJECT_IDS (comma-separated). Empty or unset: no projects configured. */
export function parseFcmProjectIds(raw: string | undefined): string[] {
  const ids = (raw ?? '')
    .split(',')
    .map((s) => s.trim())
    .filter((s) => s !== '');
  if (new Set(ids).size !== ids.length) throw new Error('FCM_PROJECT_IDS has a duplicate id');
  return ids;
}

/**
 * The project for a device: a stable hash of the device id, modulo the project count. The same
 * device always gets the same project while the list is unchanged, so only append to the list
 * (reordering or removing an id re-assigns devices that already registered a token). Undefined
 * when no projects are configured.
 */
export function assignFcmProject(
  deviceId: string,
  projectIds: readonly string[],
): string | undefined {
  if (projectIds.length === 0) return undefined;
  const digest = createHash('sha256').update(deviceId).digest();
  return projectIds[digest.readUInt32BE(0) % projectIds.length];
}

export interface FcmProjectSenderOptions {
  /** One sender per Firebase project id, in configuration order; the first is the default. */
  projects: ReadonlyArray<{ projectId: string; sender: NativePushSender }>;
  /** Sends per second per project. Default 9,000. */
  ratePerSecond?: number;
  /**
   * Returns this task's per-project rate; when given it replaces `ratePerSecond`. Called at
   * construction and by refreshRate(), so a fleet-wide budget can follow the fleet size.
   */
  rateProvider?: () => number;
  logger?: Logger;
  /** Injectable for tests. */
  now?: () => number;
  sleep?: (ms: number) => Promise<void>;
}

/**
 * Routes Android messages to the sender of `target.project` (default: the first project) and
 * rate-limits each project with its own token bucket. A project over its rate makes only its
 * own messages wait; the other projects are not held up.
 */
export class FcmProjectSender implements NativePushSender {
  private readonly byProject = new Map<string, { sender: NativePushSender; bucket: TokenBucket }>();
  private readonly defaultProject: string;

  constructor(private readonly opts: FcmProjectSenderOptions) {
    const first = opts.projects[0];
    if (!first) throw new Error('FcmProjectSender needs at least one project');
    this.defaultProject = first.projectId;
    const rate = this.currentRate();
    for (const p of opts.projects) {
      if (this.byProject.has(p.projectId)) throw new Error('duplicate FCM project id');
      this.byProject.set(p.projectId, {
        sender: p.sender,
        bucket: new TokenBucket(rate, rate, opts.now, opts.sleep),
      });
    }
  }

  private currentRate(): number {
    const rate = this.opts.rateProvider?.() ?? this.opts.ratePerSecond ?? DEFAULT_FCM_PROJECT_RATE;
    return Number.isFinite(rate) && rate > 0 ? rate : DEFAULT_FCM_PROJECT_RATE;
  }

  /** Re-reads the rate provider and applies it to every project bucket. Returns the new rate. */
  refreshRate(): number {
    const rate = this.currentRate();
    for (const e of this.byProject.values()) e.bucket.setRate(rate, Math.max(1, rate));
    return rate;
  }

  /** The current per-project rate of this task, in sends per second. */
  get ratePerSecond(): number {
    return this.byProject.values().next().value?.bucket.ratePerSecond ?? this.currentRate();
  }

  /** The project ids this sender serves, in configuration order. */
  get projectIds(): string[] {
    return [...this.byProject.keys()];
  }

  async send(messages: NativePushMessage[]): Promise<NativePushResult[]> {
    const results = new Array<NativePushResult>(messages.length);
    const groups = new Map<string, { index: number[]; msgs: NativePushMessage[] }>();
    messages.forEach((m, i) => {
      const project = m.target.project ?? this.defaultProject;
      if (!this.byProject.has(project)) {
        // Not a project this sender knows (the list shrank, or a bad value got stored). The token
        // is not known to be dead, so this is a permanent failure of the message only.
        results[i] = { ok: false, kind: 'permanent', scope: 'message', error: 'UnknownFcmProject' };
        return;
      }
      let g = groups.get(project);
      if (!g) groups.set(project, (g = { index: [], msgs: [] }));
      g.index.push(i);
      g.msgs.push(m);
    });
    await Promise.all(
      [...groups].map(async ([project, g]) => {
        const entry = this.byProject.get(project);
        if (!entry) return;
        let out: NativePushResult[];
        try {
          await entry.bucket.take(g.msgs.length);
          out = await entry.sender.send(g.msgs);
        } catch (err) {
          const error = `FcmProjectError: ${err instanceof Error ? err.name : 'Error'}`;
          (this.opts.logger ?? noopLogger).error('fcm.project_send_failed', { error });
          out = g.msgs.map((): NativePushResult => ({ ok: false, kind: 'retryable', error }));
        }
        g.index.forEach((orig, j) => {
          results[orig] = out[j] ?? { ok: false, kind: 'retryable', error: 'MissingResult' };
        });
      }),
    );
    return results;
  }
}
