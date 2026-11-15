import { PutObjectCommand, type S3Client } from '@aws-sdk/client-s3';

/**
 * Agency audit trail (NIST AU-2, AU-3, AU-12). One record per agency API call, allowed or denied,
 * written BEFORE the response leaves the function. If the write fails the request is refused with
 * 503 (fail closed): no agency data is ever served without its audit record.
 *
 * The record says who did what to which resource, when, from where and with what outcome. It
 * never holds check-in counts, cells, tokens or any data about the public.
 */
export const AUDIT_SCHEMA = 'relay.agency-audit/1';

export type AuditOutcome = 'allow' | 'deny' | 'error';

export interface AuditRecord {
  schema: typeof AUDIT_SCHEMA;
  /** ISO 8601, UTC, when the decision was made. */
  time: string;
  /** API Gateway request id (also in the access log, so the two can be joined). */
  requestId: string;
  /** e.g. agency.alert.summary.read; agency.unknown for unmatched routes. */
  action: string;
  outcome: AuditOutcome;
  /** Stable code for a deny or error (e.g. area_outside_jurisdiction); null when allowed. */
  reason: string | null;
  /** HTTP status returned to the caller. */
  status: number;
  actor: {
    sub: string | null;
    agencyId: string | null;
    /** The caller's groups as the token states them (agency roles and anything else). */
    roles: string[];
  };
  resource: {
    /** API Gateway route key, e.g. "GET /agency/alerts/{alertId}/summary". */
    route: string;
    alertId: string | null;
    /** Counties the request asked for (or was given by default). */
    areas: string[];
  };
  source: {
    /** The peer API Gateway saw (a CloudFront edge address when the request came through it). */
    ip: string | null;
    /**
     * X-Forwarded-For as received. CloudFront appends the viewer address as the LAST entry;
     * entries to its left were sent by the client and are not trustworthy.
     */
    forwardedFor: string | null;
    userAgent: string | null;
  };
}

export interface AuditSink {
  /** Resolves only once the record is durably stored; rejects otherwise. */
  write(record: AuditRecord): Promise<void>;
}

/** Longest free-text field kept (user agents can be long; keep records small and bounded). */
export const MAX_AUDIT_TEXT = 512;
export const clip = (v: string | undefined | null): string | null =>
  v === undefined || v === null || v === '' ? null : v.slice(0, MAX_AUDIT_TEXT);

export const DEFAULT_AUDIT_PREFIX = 'agency-audit/v1/';

/**
 * Object key: <prefix>YYYY/MM/DD/HH/<time>-<requestId>.json. Time-partitioned so Athena or a
 * plain prefix listing can read one day, and unique per request so records never overwrite.
 */
export function auditKey(record: AuditRecord, prefix = DEFAULT_AUDIT_PREFIX): string {
  const t = new Date(record.time);
  if (Number.isNaN(t.getTime())) throw new Error('audit record has an invalid time');
  const p = (n: number) => String(n).padStart(2, '0');
  const day = `${t.getUTCFullYear()}/${p(t.getUTCMonth() + 1)}/${p(t.getUTCDate())}/${p(t.getUTCHours())}`;
  const id = record.requestId.replace(/[^A-Za-z0-9=_-]/g, '_').slice(0, 128) || 'no-request-id';
  return `${prefix}${day}/${record.time.replace(/[:.]/g, '')}-${id}.json`;
}

/**
 * Writes each record as its own object with PutObject (see docs/agency-auth.md for why not
 * Firehose). The bucket has Object Lock (governance), versioning and SSE-KMS with the logs key,
 * and its policy lets only the agency function write and nobody delete, so a stored record cannot
 * be changed or removed inside the retention window without the break-glass role.
 */
export class S3AuditSink implements AuditSink {
  constructor(
    private readonly s3: Pick<S3Client, 'send'>,
    private readonly bucket: string,
    private readonly prefix = DEFAULT_AUDIT_PREFIX,
  ) {}

  async write(record: AuditRecord): Promise<void> {
    await this.s3.send(
      new PutObjectCommand({
        Bucket: this.bucket,
        Key: auditKey(record, this.prefix),
        Body: `${JSON.stringify(record)}\n`,
        ContentType: 'application/json',
        // Object Lock buckets require an integrity checksum on every upload.
        ChecksumAlgorithm: 'SHA256',
      }),
    );
  }
}

/** In-memory sink for tests and local runs. `fail` makes every write reject. */
export class MemoryAuditSink implements AuditSink {
  readonly records: AuditRecord[] = [];
  fail = false;

  write(record: AuditRecord): Promise<void> {
    if (this.fail) return Promise.reject(new Error('audit sink unavailable'));
    this.records.push(structuredClone(record));
    return Promise.resolve();
  }
}
