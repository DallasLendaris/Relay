import http from 'node:http';
import http2 from 'node:http2';

const agent = new http.Agent({ keepAlive: true, maxSockets: 64 });

export interface FastResponse {
  status: number;
  retryAfter: string | undefined;
  body: string;
}

/** Keep-alive HTTP/1.1 POST; much faster than fetch for tens of thousands of requests. */
export function postJson(url: string, body: string, accessToken?: string): Promise<FastResponse> {
  return new Promise((resolve, reject) => {
    const req = http.request(
      url,
      {
        method: 'POST',
        agent,
        headers: {
          'content-type': 'application/json',
          ...(accessToken ? { authorization: `Bearer ${accessToken}` } : {}),
        },
      },
      (res) => {
        const chunks: Buffer[] = [];
        res.on('data', (c: Buffer) => chunks.push(c));
        res.on('end', () =>
          resolve({
            status: res.statusCode ?? 0,
            retryAfter: res.headers['retry-after'] as string | undefined,
            body: Buffer.concat(chunks).toString('utf8'),
          }),
        );
      },
    );
    req.on('error', reject);
    req.end(body);
  });
}

export interface H2Response {
  status: number;
  headers: http2.IncomingHttpHeaders;
  body: string;
}

export function connectH2(url: string, ca?: string): http2.ClientHttp2Session {
  const session = http2.connect(url, ca ? { ca } : {});
  session.on('error', () => {});
  return session;
}

export const GOOD_HEADERS = {
  authorization: 'bearer test-jwt',
  'apns-topic': 'org.relay.app',
  'apns-push-type': 'alert',
};

export function apnsPost(
  session: http2.ClientHttp2Session,
  token: string,
  headers: Record<string, string> = GOOD_HEADERS,
  body: string = JSON.stringify({ aps: { alert: 'hi' } }),
): Promise<H2Response> {
  return new Promise((resolve, reject) => {
    const req = session.request({ ':method': 'POST', ':path': `/3/device/${token}`, ...headers });
    let status = 0;
    let responseHeaders: http2.IncomingHttpHeaders = {};
    const chunks: Buffer[] = [];
    req.on('response', (h) => {
      status = Number(h[':status']);
      responseHeaders = h;
    });
    req.on('data', (c: Buffer) => chunks.push(c));
    req.on('end', () => {
      resolve({ status, headers: responseHeaders, body: Buffer.concat(chunks).toString('utf8') });
    });
    req.on('error', reject);
    req.end(body);
  });
}

/** Runs `total` jobs with at most `concurrency` in flight. */
export async function pool(
  total: number,
  concurrency: number,
  job: (i: number) => Promise<void>,
): Promise<void> {
  let next = 0;
  await Promise.all(
    Array.from({ length: concurrency }, async () => {
      while (next < total) {
        const i = next++;
        await job(i);
      }
    }),
  );
}

export function hexToken(i: number): string {
  return i.toString(16).padStart(64, '0');
}

export const NO_LATENCY = { p50Ms: 0, p99Ms: 0 };
export const NO_ERRORS = { unregistered: 0, tooManyRequests: 0, serverError: 0 };
