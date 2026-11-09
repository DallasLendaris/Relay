/**
 * PLACEHOLDER. The real APNs / FCM adapters are built by another agent and replace this file
 * when branches merge. It exists only so the sender service compiles in this worktree.
 * Constructor option shapes here are the sender's assumption; adjust main.ts to the real ones.
 */
import type {
  NativePushMessage,
  NativePushResult,
  NativePushSender,
} from '../../scale/contracts.js';

export interface ApnsOptions {
  keyId: string;
  teamId: string;
  bundleId: string;
  /** Contents of the .p8 key. */
  p8: string;
  sandbox: boolean;
}

export class ApnsSender implements NativePushSender {
  constructor(_options: ApnsOptions) {}
  async send(_messages: NativePushMessage[]): Promise<NativePushResult[]> {
    throw new Error('not implemented');
  }
}

export interface FcmOptions {
  /** Parsed service account JSON. */
  serviceAccount: Record<string, unknown>;
}

export class FcmSender implements NativePushSender {
  constructor(_options: FcmOptions) {}
  async send(_messages: NativePushMessage[]): Promise<NativePushResult[]> {
    throw new Error('not implemented');
  }
}

/** Sends each message through the adapter for its target platform. */
export class RoutingPushSender implements NativePushSender {
  constructor(_senders: { ios: NativePushSender; android: NativePushSender }) {}
  async send(_messages: NativePushMessage[]): Promise<NativePushResult[]> {
    throw new Error('not implemented');
  }
}
