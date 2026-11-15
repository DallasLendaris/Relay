/** A tiny router for API Gateway HTTP API (payload v2) events. No framework. */
import { LIMITS } from '@relay/shared';

export interface ApiEvent {
  rawPath: string;
  headers?: Record<string, string | undefined>;
  body?: string | null;
  isBase64Encoded?: boolean;
  requestContext: { http: { method: string } };
}

export interface ApiResult {
  statusCode: number;
  headers: Record<string, string>;
  body: string;
}

export interface Request {
  params: Record<string, string>;
  /** Header names are lower-cased. */
  headers: Record<string, string | undefined>;
  /** Parsed JSON body; undefined when the body is empty. */
  body: unknown;
}

export type Handler = (req: Request) => Promise<ApiResult>;

/** Thrown by handlers to produce an error response in the spec's {code, message} shape. */
export class HttpError extends Error {
  constructor(
    readonly status: number,
    readonly code: string,
    message: string,
  ) {
    super(message);
  }
}

/** Replaces UUID path segments so logs group by route and carry no identifiers. */
export const routeOf = (path: string): string =>
  path.replace(/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/gi, '{id}');

export function json(statusCode: number, body: unknown): ApiResult {
  return {
    statusCode,
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  };
}

export function noContent(): ApiResult {
  return { statusCode: 204, headers: {}, body: '' };
}

export function errorResult(status: number, code: string, message: string): ApiResult {
  return json(status, { code, message });
}

interface RouteDef {
  method: string;
  segments: string[];
  handler: Handler;
}

/** Collects routes like `GET /places/{placeId}` and dispatches events to them. */
export class Router {
  private readonly routes: RouteDef[] = [];

  add(method: string, pattern: string, handler: Handler): void {
    this.routes.push({ method, segments: split(pattern), handler });
  }

  async route(event: ApiEvent): Promise<ApiResult> {
    try {
      const method = event.requestContext.http.method.toUpperCase();
      const path = split(event.rawPath);
      let pathMatched = false;
      for (const r of this.routes) {
        const params = match(r.segments, path);
        if (!params) continue;
        pathMatched = true;
        if (r.method !== method) continue;
        const headers: Record<string, string | undefined> = {};
        for (const [k, v] of Object.entries(event.headers ?? {})) headers[k.toLowerCase()] = v;
        return await r.handler({ params, headers, body: parseBody(event) });
      }
      return pathMatched
        ? errorResult(405, 'method_not_allowed', 'Method not allowed')
        : errorResult(404, 'not_found', 'Route not found');
    } catch (err) {
      if (err instanceof HttpError) return errorResult(err.status, err.code, err.message);
      // Never log err.message or the error object: database errors can quote row contents,
      // including coordinates. Name, a stable code and the route are enough to find the bug.
      console.error(
        JSON.stringify({
          level: 'error',
          msg: 'request failed',
          error: err instanceof Error ? err.name : 'UnknownError',
          code: 'internal_error',
          route: routeOf(event.rawPath),
        }),
      );
      return errorResult(500, 'internal_error', 'Internal error');
    }
  }
}

function split(path: string): string[] {
  return path.split('/').filter(Boolean);
}

function match(pattern: string[], path: string[]): Record<string, string> | null {
  if (pattern.length !== path.length) return null;
  const params: Record<string, string> = {};
  for (let i = 0; i < pattern.length; i++) {
    const p = pattern[i];
    const v = path[i];
    if (p === undefined || v === undefined) return null;
    if (p.startsWith('{') && p.endsWith('}')) {
      try {
        params[p.slice(1, -1)] = decodeURIComponent(v);
      } catch {
        // e.g. "%E0%A4%A": a client error, not a server one.
        throw new HttpError(400, 'invalid_request', 'Path contains invalid percent-encoding');
      }
    } else if (p !== v) return null;
  }
  return params;
}

function parseBody(event: ApiEvent): unknown {
  if (!event.body) return undefined;
  // Check the size before decoding or parsing anything. Base64 is 4 chars per 3 bytes; a string's
  // UTF-8 size is at least its length, so the length check is a cheap lower bound for text.
  const approxBytes = event.isBase64Encoded
    ? Math.floor((event.body.length * 3) / 4)
    : Buffer.byteLength(event.body, 'utf8');
  if (approxBytes > LIMITS.maxRequestBodyBytes) {
    throw new HttpError(413, 'payload_too_large', 'Request body is too large');
  }
  const text = event.isBase64Encoded
    ? Buffer.from(event.body, 'base64').toString('utf8')
    : event.body;
  if (text.trim() === '') return undefined;
  try {
    return JSON.parse(text);
  } catch {
    throw new HttpError(400, 'invalid_json', 'Request body is not valid JSON');
  }
}
