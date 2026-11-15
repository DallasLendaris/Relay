import { PutObjectCommand, type S3Client } from '@aws-sdk/client-s3';
import type { PublicObjectWriter } from './alert-publisher.js';

/** Writes public JSON objects to S3 with PutObject. */
export class S3PublicObjectWriter implements PublicObjectWriter {
  constructor(
    private readonly s3: S3Client,
    private readonly bucket: string,
  ) {}

  async putJson(key: string, body: string, cacheControl: string): Promise<void> {
    await this.s3.send(
      new PutObjectCommand({
        Bucket: this.bucket,
        Key: key,
        Body: body,
        ContentType: 'application/json',
        CacheControl: cacheControl,
      }),
    );
  }
}
