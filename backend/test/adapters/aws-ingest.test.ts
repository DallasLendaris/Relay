import { ConditionalCheckFailedException, DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { PutObjectCommand, S3Client } from '@aws-sdk/client-s3';
import { DynamoDBDocumentClient, GetCommand, PutCommand } from '@aws-sdk/lib-dynamodb';
import { mockClient } from 'aws-sdk-client-mock';
import { beforeEach, describe, expect, it } from 'vitest';
import { DynamoSeenStore } from '../../src/adapters/aws/dynamo-seen-store.js';
import { S3RawArchive } from '../../src/adapters/aws/s3-raw-archive.js';

describe('S3RawArchive', () => {
  const s3Mock = mockClient(S3Client);
  beforeEach(() => s3Mock.reset());

  it('writes to nws/YYYY/MM/DD/HHmmss-<random>.json in UTC with geo+json content type', async () => {
    s3Mock.on(PutObjectCommand).resolves({});
    const archive = new S3RawArchive(new S3Client({ region: 'us-west-2' }), 'raw-bucket');
    // 23:05:09 UTC on Dec 31; local time zones must not shift the date.
    const key = await archive.put('{"features":[]}', new Date('2026-12-31T23:05:09Z'));

    expect(key).toMatch(/^nws\/2026\/12\/31\/230509-[0-9a-f]{8}\.json$/);
    const input = s3Mock.commandCalls(PutObjectCommand)[0]?.args[0].input;
    expect(input).toMatchObject({
      Bucket: 'raw-bucket',
      Key: key,
      Body: '{"features":[]}',
      ContentType: 'application/geo+json',
    });
  });

  it('uses different keys for two puts in the same second', async () => {
    s3Mock.on(PutObjectCommand).resolves({});
    const archive = new S3RawArchive(new S3Client({ region: 'us-west-2' }), 'b');
    const at = new Date('2026-11-09T10:00:00Z');
    expect(await archive.put('a', at)).not.toBe(await archive.put('b', at));
  });

  it('propagates S3 errors so the caller can log them', async () => {
    s3Mock.on(PutObjectCommand).rejects(new Error('denied'));
    const archive = new S3RawArchive(new S3Client({ region: 'us-west-2' }), 'b');
    await expect(archive.put('x', new Date())).rejects.toThrow('denied');
  });
});

describe('DynamoSeenStore', () => {
  const ddbMock = mockClient(DynamoDBDocumentClient);
  beforeEach(() => ddbMock.reset());
  const db = () => DynamoDBDocumentClient.from(new DynamoDBClient({ region: 'us-west-2' }));

  it('mark does a conditional put with a 30-day expiresAt', async () => {
    ddbMock.on(PutCommand).resolves({});
    const nowMs = Date.parse('2026-11-09T00:00:00Z');
    const store = new DynamoSeenStore(db(), 'state', () => nowMs);
    await store.mark('urn:oid:1', '2026-11-08T10:00:00Z');

    const input = ddbMock.commandCalls(PutCommand)[0]?.args[0].input;
    expect(input?.TableName).toBe('state');
    expect(input?.ConditionExpression).toBe('attribute_not_exists(sourceId)');
    expect(input?.Item).toMatchObject({
      sourceId: 'urn:oid:1',
      sent: '2026-11-08T10:00:00Z',
      expiresAt: nowMs / 1000 + 30 * 24 * 60 * 60,
    });
  });

  it('mark is a no-op when the item already exists', async () => {
    ddbMock
      .on(PutCommand)
      .rejects(new ConditionalCheckFailedException({ message: 'exists', $metadata: {} }));
    const store = new DynamoSeenStore(db(), 'state');
    await expect(store.mark('urn:oid:1', 'x')).resolves.toBeUndefined();
  });

  it('mark rethrows other errors', async () => {
    ddbMock.on(PutCommand).rejects(new Error('throttled'));
    const store = new DynamoSeenStore(db(), 'state');
    await expect(store.mark('urn:oid:1', 'x')).rejects.toThrow('throttled');
  });

  it('has reflects whether the item exists', async () => {
    ddbMock.on(GetCommand, { Key: { sourceId: 'yes' } }).resolves({ Item: { sourceId: 'yes' } });
    ddbMock.on(GetCommand, { Key: { sourceId: 'no' } }).resolves({});
    const store = new DynamoSeenStore(db(), 'state');
    expect(await store.has('yes')).toBe(true);
    expect(await store.has('no')).toBe(false);
  });

  it('keeps the feed-absence record per alert id, unconditionally, and clears it by overwrite', async () => {
    ddbMock.on(PutCommand).resolves({});
    const nowMs = Date.parse('2026-11-09T00:00:00Z');
    const store = new DynamoSeenStore(db(), 'state', () => nowMs);
    const marker = {
      endedBy: 'feed_absence',
      priorState: 'updated',
      endedAt: '2026-11-09T00:00:00Z',
    } as const;
    await store.markEnded('alert-1', marker);
    const put = ddbMock.commandCalls(PutCommand)[0]?.args[0].input;
    expect(put?.ConditionExpression).toBeUndefined(); // a second ending overwrites
    expect(put?.Item).toMatchObject({ sourceId: 'ended#alert-1', ...marker, restored: false });

    ddbMock
      .on(GetCommand, { Key: { sourceId: 'ended#alert-1' } })
      .resolves({ Item: { sourceId: 'ended#alert-1', ...marker, restored: false } });
    expect(await store.getEnded('alert-1')).toEqual(marker);

    await store.clearEnded('alert-1');
    const cleared = ddbMock.commandCalls(PutCommand)[1]?.args[0].input;
    expect(cleared?.Item).toMatchObject({ sourceId: 'ended#alert-1', restored: true });
    ddbMock
      .on(GetCommand, { Key: { sourceId: 'ended#alert-1' } })
      .resolves({ Item: { sourceId: 'ended#alert-1', restored: true } });
    expect(await store.getEnded('alert-1')).toBeNull();
    ddbMock.on(GetCommand, { Key: { sourceId: 'ended#none' } }).resolves({});
    expect(await store.getEnded('none')).toBeNull();
  });
});
