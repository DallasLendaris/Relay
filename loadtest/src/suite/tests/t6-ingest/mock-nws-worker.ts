/**
 * t6 child process: a mock api.weather.gov. Serves `GET /alerts/active?area=CA` (the feed the test
 * sets, with ETag / If-None-Match like the real one) and `GET /zones/{type}/{ugc}` (square zone
 * polygons for CAZ001..CAZ080, 404 for anything else), and records every request.
 *
 * Control (same port): POST /control/feed (body = the feed JSON text), GET /control/log,
 * POST /control/reset. Sends { port } to the parent when listening.
 */
import { createHash } from 'node:crypto';
import http from 'node:http';
import type { AddressInfo } from 'node:net';
import { runWorkerMain, sendMessage } from '../../lib/child.js';
import { KNOWN_ZONES, square, zoneCenter } from './feed.js';

runWorkerMain(async () => {
  let body = JSON.stringify({ type: 'FeatureCollection', features: [] });
  let etag = '"0"';
  const feedRequests: Array<{ at: number; conditional: boolean; status: number }> = [];
  let zoneRequests = 0;

  const server = http.createServer((req, res) => {
    const chunks: Buffer[] = [];
    req.on('data', (c: Buffer) => chunks.push(c));
    req.on('end', () => {
      const url = new URL(req.url ?? '/', 'http://127.0.0.1');
      const json = (status: number, payload: unknown): void => {
        res.writeHead(status, { 'content-type': 'application/json' });
        res.end(JSON.stringify(payload));
      };
      if (req.method === 'POST' && url.pathname === '/control/feed') {
        body = Buffer.concat(chunks).toString('utf8');
        etag = `"${createHash('sha1').update(body).digest('hex').slice(0, 16)}"`;
        json(200, { ok: true, bytes: body.length });
        return;
      }
      if (req.method === 'POST' && url.pathname === '/control/reset') {
        feedRequests.length = 0;
        zoneRequests = 0;
        json(200, { ok: true });
        return;
      }
      if (req.method === 'GET' && url.pathname === '/control/log') {
        json(200, { feedRequests, zoneRequests });
        return;
      }
      if (req.method === 'GET' && url.pathname === '/alerts/active') {
        const conditional = req.headers['if-none-match'] !== undefined;
        if (req.headers['if-none-match'] === etag) {
          feedRequests.push({ at: Date.now(), conditional, status: 304 });
          res.writeHead(304);
          res.end();
          return;
        }
        feedRequests.push({ at: Date.now(), conditional, status: 200 });
        res.writeHead(200, {
          'content-type': 'application/geo+json',
          etag,
          'last-modified': new Date(0).toUTCString(),
        });
        res.end(body);
        return;
      }
      const zone = /^\/zones\/(forecast|fire|county)\/CAZ(\d{3})$/.exec(url.pathname);
      if (req.method === 'GET' && zone) {
        zoneRequests++;
        const n = Number(zone[2]);
        if (zone[1] !== 'forecast' || n < 1 || n > KNOWN_ZONES) {
          json(404, { title: 'Not Found' });
          return;
        }
        const c = zoneCenter(n);
        json(200, {
          type: 'Feature',
          geometry: { type: 'Polygon', coordinates: square(c.lng, c.lat, 0.04) },
        });
        return;
      }
      json(404, { title: 'Not Found' });
    });
  });
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  await sendMessage({ port: (server.address() as AddressInfo).port });
  await new Promise<void>(() => undefined);
  return {};
});
