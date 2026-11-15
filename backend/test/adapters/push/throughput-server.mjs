// Minimal mock APNs (h2c) or FCM server, run in its own process by throughput.test.ts so the
// sender under test and the mock do not compete for one event loop. It only counts requests
// and peak concurrency; payload and mapping behaviour is covered by the other test files.
import { createServer as createHttp } from 'node:http';
import { createServer as createH2c } from 'node:http2';

const mode = process.argv[2];
let total = 0;
let inFlight = 0;
let maxInFlight = 0;
let tokenRequests = 0;
const open = () => {
  total++;
  inFlight++;
  if (inFlight > maxInFlight) maxInFlight = inFlight;
};

let server;
if (mode === 'apns') {
  server = createH2c();
  server.on('stream', (stream) => {
    open();
    stream.on('error', () => {});
    stream.on('close', () => inFlight--);
    stream.resume();
    stream.on('end', () => {
      stream.respond({ ':status': 200 });
      stream.end();
    });
  });
} else {
  server = createHttp((req, res) => {
    if (req.url === '/token') {
      tokenRequests++;
      req.resume();
      req.on('end', () =>
        res
          .writeHead(200, { 'content-type': 'application/json' })
          .end('{"access_token":"t","expires_in":3600}'),
      );
      return;
    }
    open();
    res.on('close', () => inFlight--);
    req.resume();
    req.on('end', () => res.writeHead(200, { 'content-type': 'application/json' }).end('{}'));
  });
}

server.listen(0, '127.0.0.1', () => {
  process.send({ ready: server.address().port });
});
process.on('message', (msg) => {
  if (msg === 'stats') process.send({ stats: { total, maxInFlight, tokenRequests } });
  if (msg === 'stop') process.exit(0);
});
