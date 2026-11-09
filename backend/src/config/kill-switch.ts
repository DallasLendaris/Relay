import { GetParameterCommand, SSMClient } from '@aws-sdk/client-ssm';

const TTL_MS = 30 * 1000;

/**
 * Kill switch backed by an SSM String parameter. The value "paused" stops all
 * delivery; anything else means running. Returns () => Promise<boolean> (true = paused),
 * cached for 30 seconds. A missing parameter means running; any other read error throws
 * (planDelivery's caller logs it and sends nothing, so a failed read never sends).
 */
export function createKillSwitch(
  paramName: string,
  client: Pick<SSMClient, 'send'> = new SSMClient({}),
  now: () => number = Date.now,
): () => Promise<boolean> {
  let cached: { at: number; paused: boolean } | undefined;
  return async () => {
    if (cached && now() - cached.at < TTL_MS) return cached.paused;
    let paused: boolean;
    try {
      const out = await client.send(new GetParameterCommand({ Name: paramName }));
      paused = out.Parameter?.Value?.trim().toLowerCase() === 'paused';
    } catch (err) {
      if ((err as { name?: string }).name !== 'ParameterNotFound') throw err;
      paused = false;
    }
    cached = { at: now(), paused };
    return paused;
  };
}
