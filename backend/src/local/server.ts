/**
 * HTTP server for local mode. Serves the real Relay API (the same createApi the Lambda uses),
 * plus a few local-only routes:
 *
 *   GET  /health        liveness, for smoke checks
 *   GET  /local/status  JSON: feed mode, clock, last poll, alerts, pushes, check-in totals
 *   POST /local/poll    poll the feed and deliver now, instead of waiting for the timer
 *   GET  /              a small status page that refreshes itself (used in the demo video)
 */
import { createServer, type IncomingMessage, type Server, type ServerResponse } from 'node:http';
import type { ApiEvent } from '../api/index.js';
import type { LocalRelay } from './relay.js';

const MAX_BODY_BYTES = 64 * 1024;

function readBody(req: IncomingMessage): Promise<string> {
  return new Promise((resolve, reject) => {
    let size = 0;
    const chunks: Buffer[] = [];
    req.on('data', (c: Buffer) => {
      size += c.length;
      if (size > MAX_BODY_BYTES) {
        // Stop keeping it, but let the rest drain so the 413 reaches the client.
        chunks.length = 0;
        reject(new Error('body too large'));
        return;
      }
      chunks.push(c);
    });
    req.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
    req.on('error', reject);
  });
}

function send(res: ServerResponse, status: number, body: string, type = 'application/json') {
  if (res.headersSent) return;
  res.writeHead(status, {
    'content-type': type,
    'cache-control': 'no-store',
    ...(status === 413 ? { connection: 'close' } : {}),
  });
  res.end(body);
}

export interface ServerInfo {
  feed: string;
  push: string;
  replayFile?: string;
}

export async function status(relay: LocalRelay, info: ServerInfo) {
  const alerts = await relay.stores.alerts.listActiveBySource('nws', relay.clock.now());
  return {
    feed: info.feed,
    push: info.push,
    ...(info.replayFile ? { replayFile: info.replayFile } : {}),
    clock: relay.clock.now().toISOString(),
    lastPoll: relay.lastPoll() ?? null,
    alerts: alerts.map((a) => ({
      id: a.id,
      event: a.event,
      relaySeverity: a.relaySeverity,
      state: a.state,
      expires: a.expires,
      cells: a.area.h3Cells.length,
    })),
    // The token itself is never shown, only that a push went to some device.
    pushes: relay.sentLog.map(({ to: _to, ...p }) => p),
    checkInTotals: relay.checkInTotals(),
  };
}

const PAGE = `<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>Relay local mode</title>
<style>body{font:15px/1.4 system-ui,sans-serif;margin:24px;max-width:960px;color:#14213d}
h1{font-size:20px}h2{font-size:16px;margin-top:24px}table{border-collapse:collapse;width:100%}
td,th{border-bottom:1px solid #ddd;padding:6px 8px;text-align:left}code{font-size:13px}
.muted{color:#666}</style></head><body>
<h1>Relay local mode</h1><p class="muted" id="meta">Loading…</p>
<h2>Alerts stored</h2><table id="alerts"></table>
<h2>Pushes sent</h2><table id="pushes"></table>
<h2>"I'm okay" check-ins per H3 cell</h2><table id="cells"></table>
<script>
const esc=s=>String(s??'').replace(/[&<>"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
const rows=(el,head,data)=>{document.getElementById(el).innerHTML='<tr>'+head.map(h=>'<th>'+h+'</th>').join('')+'</tr>'+
(data.length?data.map(r=>'<tr>'+r.map(c=>'<td>'+esc(c)+'</td>').join('')+'</tr>').join(''):'<tr><td class="muted">None yet</td></tr>')};
async function tick(){try{const s=await (await fetch('/local/status')).json();
document.getElementById('meta').textContent='Feed: '+s.feed+' · Push: '+s.push+' · Clock: '+s.clock+(s.lastPoll?' · Last poll: '+s.lastPoll.at:'');
rows('alerts',['Event','Severity','State','Expires','H3 cells'],s.alerts.map(a=>[a.event,a.relaySeverity,a.state,a.expires,a.cells]));
rows('pushes',['Time','Title','Result'],s.pushes.map(p=>[p.at,p.title,p.ok?'accepted':p.error]));
rows('cells',['Alert','H3 cell','Okay'],s.checkInTotals.flatMap(t=>t.cells.map(c=>[t.alertId.slice(0,8),c.h3Cell,c.okCount])));
}catch(e){document.getElementById('meta').textContent='Server not reachable'}}
tick();setInterval(tick,2000);
</script></body></html>`;

export function createLocalServer(relay: LocalRelay, info: ServerInfo): Server {
  return createServer((req, res) => {
    void (async () => {
      const url = new URL(req.url ?? '/', 'http://local');
      const method = (req.method ?? 'GET').toUpperCase();
      try {
        if (method === 'GET' && url.pathname === '/health') {
          return send(res, 200, JSON.stringify({ ok: true }));
        }
        if (method === 'GET' && url.pathname === '/') return send(res, 200, PAGE, 'text/html');
        if (method === 'GET' && url.pathname === '/local/status') {
          return send(res, 200, JSON.stringify(await status(relay, info)));
        }
        if (method === 'POST' && url.pathname === '/local/poll') {
          return send(res, 200, JSON.stringify(await relay.pollAndDeliver()));
        }
        const body = method === 'GET' || method === 'HEAD' ? '' : await readBody(req);
        const headers: Record<string, string | undefined> = {};
        for (const [k, v] of Object.entries(req.headers)) {
          headers[k] = Array.isArray(v) ? v.join(',') : v;
        }
        const event: ApiEvent = {
          rawPath: url.pathname,
          headers,
          body: body || null,
          requestContext: { http: { method } },
        };
        const result = await relay.handle(event);
        res.writeHead(result.statusCode, result.headers);
        res.end(result.body);
      } catch (err) {
        const tooLarge = err instanceof Error && err.message === 'body too large';
        send(
          res,
          tooLarge ? 413 : 500,
          JSON.stringify(
            tooLarge
              ? { code: 'payload_too_large', message: 'Body too large' }
              : { code: 'internal_error', message: 'Internal error' },
          ),
        );
        if (!tooLarge)
          console.error(
            JSON.stringify({
              level: 'error',
              msg: 'local server error',
              error: (err as Error).name,
            }),
          );
      }
    })();
  });
}
