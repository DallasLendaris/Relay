import { GetParameterCommand, SSMClient } from '@aws-sdk/client-ssm';
import { mockClient } from 'aws-sdk-client-mock';
import { beforeEach, describe, expect, it } from 'vitest';
import { createKillSwitch } from '../../src/config/kill-switch.js';

const ssm = mockClient(SSMClient);
const value = (v: string) => ({ Parameter: { Value: v } });

beforeEach(() => ssm.reset());

describe('kill switch', () => {
  it('is paused only when the value is "paused"', async () => {
    const client = new SSMClient({});
    ssm.on(GetParameterCommand).resolves(value('paused'));
    expect(await createKillSwitch('/relay/kill', client)()).toBe(true);
    ssm.on(GetParameterCommand).resolves(value('running'));
    expect(await createKillSwitch('/relay/kill', client)()).toBe(false);
    ssm.on(GetParameterCommand).resolves(value('anything else'));
    expect(await createKillSwitch('/relay/kill', client)()).toBe(false);
  });

  it('caches for 30 seconds', async () => {
    ssm.on(GetParameterCommand).resolves(value('running'));
    let t = 0;
    const isPaused = createKillSwitch('/relay/kill', new SSMClient({}), () => t);
    await isPaused();
    t = 29_000;
    await isPaused();
    expect(ssm.commandCalls(GetParameterCommand)).toHaveLength(1);

    ssm.on(GetParameterCommand).resolves(value('paused'));
    t = 31_000;
    expect(await isPaused()).toBe(true);
    expect(ssm.commandCalls(GetParameterCommand)).toHaveLength(2);
  });

  it('treats a missing parameter as running and rethrows other errors', async () => {
    const client = new SSMClient({});
    ssm
      .on(GetParameterCommand)
      .rejects(Object.assign(new Error('x'), { name: 'ParameterNotFound' }));
    expect(await createKillSwitch('/relay/kill', client)()).toBe(false);
    ssm
      .on(GetParameterCommand)
      .rejects(Object.assign(new Error('denied'), { name: 'AccessDenied' }));
    await expect(createKillSwitch('/relay/kill', client)()).rejects.toThrow('denied');
  });
});
