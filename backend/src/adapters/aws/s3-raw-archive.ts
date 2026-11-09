import { randomUUID } from 'node:crypto';
import { PutObjectCommand, type S3Client } from '@aws-sdk/client-s3';
import type { RawArchive } from '../../ports.js';

const pad = (n: number): string => String(n).padStart(2, '0');

/** Stores raw NWS responses at nws/YYYY/MM/DD/HHmmss-<random>.json (UTC). */
export class S3RawArchive implements RawArchive {
  constructor(
    private readonly s3: S3Client,
    private readonly bucket: string,
  ) {}

  async put(body: string, fetchedAt: Date): Promise<string> {
    const d = fetchedAt;
    const date = `${d.getUTCFullYear()}/${pad(d.getUTCMonth() + 1)}/${pad(d.getUTCDate())}`;
    const time = `${pad(d.getUTCHours())}${pad(d.getUTCMinutes())}${pad(d.getUTCSeconds())}`;
    // The random suffix keeps two polls in the same second from overwriting each other.
    const key = `nws/${date}/${time}-${randomUUID().slice(0, 8)}.json`;
    await this.s3.send(
      new PutObjectCommand({
        Bucket: this.bucket,
        Key: key,
        Body: body,
        ContentType: 'application/geo+json',
      }),
    );
    return key;
  }
}
