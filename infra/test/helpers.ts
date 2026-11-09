import { App } from 'aws-cdk-lib';
import { Template } from 'aws-cdk-lib/assertions';
import { buildRelayApp, type RelayAppOptions } from '../lib/relay-app.js';
import { SKIP_IMAGE_CONTEXT } from '../lib/sender-stack.js';

type Stacks = ReturnType<typeof buildRelayApp>;
export type Templates = { [K in keyof Stacks]: Template };

/**
 * Builds the whole app through the same function bin/relay.ts uses and returns each stack's
 * template (plus the stack objects). The context flag keeps the Docker image asset out of tests.
 * Results are cached per option set: a full synth is slow.
 */
const cache = new Map<string, { stacks: Stacks; templates: Templates }>();

type Options = Partial<Pick<RelayAppOptions, 'throttle' | 'fanoutMode' | 'capacity'>>;

export function synthApp(stage: 'dev' | 'prod' = 'dev', options: Options = {}) {
  const key = JSON.stringify([stage, options]);
  const hit = cache.get(key);
  if (hit) return hit;
  const app = new App({ context: { [SKIP_IMAGE_CONTEXT]: 'true' } });
  const stacks = buildRelayApp(app, {
    stage,
    env: { account: '111111111111', region: 'us-west-2' },
    edgeEnv: { account: '111111111111', region: 'us-east-1' },
    prefix: stage,
    ...options,
  });
  const templates = Object.fromEntries(
    Object.entries(stacks).map(([name, stack]) => [name, Template.fromStack(stack)]),
  ) as Templates;
  const built = { stacks, templates };
  cache.set(key, built);
  return built;
}

export function synthAll(stage: 'dev' | 'prod' = 'dev', options: Options = {}): Templates {
  return synthApp(stage, options).templates;
}
