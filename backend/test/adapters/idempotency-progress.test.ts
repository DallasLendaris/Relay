import { ConditionalCheckFailedException, DynamoDBClient } from '@aws-sdk/client-dynamodb';
import {
  DeleteCommand,
  DynamoDBDocumentClient,
  PutCommand,
  UpdateCommand,
} from '@aws-sdk/lib-dynamodb';
import { mockClient } from 'aws-sdk-client-mock';
import { describe, expect, it } from 'vitest';
import {
  DynamoIdempotencyStore,
  decodeChunkProgress,
  encodeChunkProgress,
} from '../../src/adapters/aws/dynamo-idempotency-store.js';
import { MemoryIdempotencyStore } from '../../src/adapters/memory/idempotency-store.js';

const NOW = new Date('2026-11-10T10:00:00Z');
const later = (ms: number) => new Date(NOW.getTime() + ms);
const lost = () => new ConditionalCheckFailedException({ message: 'x', $metadata: {} });

describe('MemoryIdempotencyStore: sub-chunk progress', () => {
  const setup = () => {
    const state = { now: NOW };
    return { state, store: new MemoryIdempotencyStore({ now: () => state.now }) };
  };

  it('a claim that takes over an expired lease inherits the progress recorded under it', async () => {
    const { state, store } = setup();
    expect(await store.claim('k', later(45_000))).toBe('claimed');
    expect(await store.claimedProgress('k')).toEqual([]);
    expect(await store.recordChunk('k', { set: 'ab', chunk: 0, redo: [] })).toBe(true);
    expect(await store.recordChunk('k', { set: 'ab', chunk: 2, redo: [5, 9] })).toBe(true);
    // The sender dies; the lease runs out; a redelivery claims again (another process).
    state.now = later(46_000);
    expect(await store.claim('k', later(91_000))).toBe('claimed');
    expect(await store.claimedProgress('k')).toEqual([
      { set: 'ab', chunk: 0, redo: [] },
      { set: 'ab', chunk: 2, redo: [5, 9] },
    ]);
  });

  it('progress is only recorded under our own live claim', async () => {
    const { state, store } = setup();
    expect(await store.recordChunk('k', { set: 'ab', chunk: 0, redo: [] })).toBe(false); // never claimed
    await store.claim('k', later(45_000));
    state.now = later(46_000);
    await store.claim('k', later(91_000)); // re-claim after expiry: a new attempt
    expect(await store.recordChunk('k', { set: 'ab', chunk: 1, redo: [] })).toBe(true);
    await store.markSent('k', later(3_600_000));
    expect(await store.recordChunk('k', { set: 'ab', chunk: 2, redo: [] })).toBe(false); // sent: not a lease
  });

  it('release keeps the progress for the next claim; without progress it deletes as before', async () => {
    const { store } = setup();
    await store.claim('k', later(45_000));
    await store.recordChunk('k', { set: 'ab', chunk: 1, redo: [3] });
    await store.release('k');
    expect(await store.peek('k')).toBe('absent');
    expect(await store.claim('k', later(45_000))).toBe('claimed'); // at once, no lease wait
    expect(await store.claimedProgress('k')).toEqual([{ set: 'ab', chunk: 1, redo: [3] }]);

    await store.claim('j', later(45_000));
    await store.release('j');
    expect(await store.claim('j', later(45_000))).toBe('claimed');
    expect(await store.claimedProgress('j')).toEqual([]);
  });

  it('markSent ends the progress; a sent key is never claimed, and retry keys are separate', async () => {
    const { state, store } = setup();
    await store.claim('k', later(45_000));
    await store.recordChunk('k', { set: 'ab', chunk: 0, redo: [] });
    await store.markSent('k', later(60_000));
    expect(await store.claim('k', later(45_000))).toBe('sent');
    expect(await store.claim('k:r1', later(45_000))).toBe('claimed');
    expect(await store.claimedProgress('k:r1')).toEqual([]);
    // Even after the sent key expires, a new claim starts clean.
    state.now = later(61_000);
    expect(await store.claim('k', later(106_000))).toBe('claimed');
    expect(await store.claimedProgress('k')).toEqual([]);
  });
});

describe('DynamoIdempotencyStore: sub-chunk progress', () => {
  const setup = () => {
    const d = DynamoDBDocumentClient.from(new DynamoDBClient({ region: 'us-west-2' }));
    const mock = mockClient(d);
    const store = new DynamoIdempotencyStore(d, 'tbl', { now: () => NOW });
    return { mock, store };
  };

  it('encodes one set element per chunk and decodes it back', () => {
    expect(encodeChunkProgress({ set: 'ab', chunk: 3, redo: [] })).toBe('ab@3:');
    expect(encodeChunkProgress({ set: 'ab', chunk: 12, redo: [0, 7, 99] })).toBe('ab@12:0,7,99');
    expect(decodeChunkProgress('ab@12:0,7,99')).toEqual({ set: 'ab', chunk: 12, redo: [0, 7, 99] });
    expect(decodeChunkProgress('ab@3:')).toEqual({ set: 'ab', chunk: 3, redo: [] });
    expect(decodeChunkProgress('junk')).toBeUndefined();
    // Progress without a recipient fingerprint is never trusted.
    expect(decodeChunkProgress('3:')).toBeUndefined();
  });

  it('recordChunk ADDs to the sentChunks set, guarded by our own lease', async () => {
    const { mock, store } = setup();
    mock.on(PutCommand).resolves({});
    expect(await store.recordChunk('k', { set: 'ab', chunk: 0, redo: [] })).toBe(false); // not ours: no call
    expect(mock.commandCalls(UpdateCommand)).toHaveLength(0);
    await store.claim('k', later(45_000));
    const put = mock.commandCalls(PutCommand)[0]?.args[0].input;
    expect(put).toMatchObject({ ReturnValues: 'ALL_OLD' });
    const attemptId = put?.Item?.['attemptId'];
    mock.on(UpdateCommand).resolves({});
    expect(await store.recordChunk('k', { set: 'ab', chunk: 2, redo: [4, 8] })).toBe(true);
    expect(mock.commandCalls(UpdateCommand)[0]?.args[0].input).toMatchObject({
      TableName: 'tbl',
      Key: { pk: 'k' },
      UpdateExpression: 'ADD sentChunks :p',
      ConditionExpression: '#state = :leased AND attemptId = :attempt',
      ExpressionAttributeValues: {
        ':leased': 'leased',
        ':attempt': attemptId,
        ':p': new Set(['ab@2:4,8']),
      },
    });
    mock.on(UpdateCommand).rejects(lost()); // a newer attempt owns the key
    expect(await store.recordChunk('k', { set: 'ab', chunk: 3, redo: [] })).toBe(false);
  });

  it('a claim over an expired lease inherits its progress and writes it back onto the new lease', async () => {
    const { mock, store } = setup();
    mock.on(PutCommand).resolves({
      Attributes: {
        pk: 'k',
        state: 'leased',
        attemptId: 'old',
        leaseUntil: NOW.getTime() - 1,
        sentChunks: new Set(['ab@0:', 'ab@1:17', 'ab@1:', 'bad', '2:']),
      },
    });
    mock.on(UpdateCommand).resolves({});
    expect(await store.claim('k', later(45_000))).toBe('claimed');
    expect(await store.claimedProgress('k')).toEqual([
      { set: 'ab', chunk: 0, redo: [] },
      { set: 'ab', chunk: 1, redo: [17] },
      { set: 'ab', chunk: 1, redo: [] },
    ]);
    const attemptId = mock.commandCalls(PutCommand)[0]?.args[0].input.Item?.['attemptId'];
    const restore = mock.commandCalls(UpdateCommand)[0]?.args[0].input;
    expect(restore).toMatchObject({
      UpdateExpression: 'ADD sentChunks :p',
      ExpressionAttributeValues: { ':attempt': attemptId },
    });
    expect([...(restore?.ExpressionAttributeValues?.[':p'] as Set<string>)].sort()).toEqual(
      ['ab@0:', 'ab@1:', 'ab@1:17', 'bad', '2:'].sort(),
    );
    // A fresh key: no progress, no extra write.
    const fresh = setup();
    fresh.mock.on(PutCommand).resolves({});
    await fresh.store.claim('n', later(45_000));
    expect(await fresh.store.claimedProgress('n')).toEqual([]);
    expect(fresh.mock.commandCalls(UpdateCommand)).toHaveLength(0);
  });

  it('release with progress ends the lease instead of deleting; without progress it deletes', async () => {
    const { mock, store } = setup();
    mock.on(PutCommand).resolves({});
    mock.on(UpdateCommand).resolves({});
    mock.on(DeleteCommand).resolves({});
    await store.claim('k', later(45_000));
    const attemptId = mock.commandCalls(PutCommand)[0]?.args[0].input.Item?.['attemptId'];
    await store.recordChunk('k', { set: 'ab', chunk: 0, redo: [] });
    await store.release('k');
    expect(mock.commandCalls(DeleteCommand)).toHaveLength(0);
    expect(mock.commandCalls(UpdateCommand)[1]?.args[0].input).toMatchObject({
      Key: { pk: 'k' },
      UpdateExpression: 'SET leaseUntil = :zero',
      ConditionExpression: '#state = :leased AND attemptId = :attempt',
      ExpressionAttributeValues: { ':leased': 'leased', ':attempt': attemptId, ':zero': 0 },
    });

    await store.claim('j', later(45_000));
    await store.release('j');
    expect(mock.commandCalls(DeleteCommand)).toHaveLength(1);
    expect(mock.commandCalls(DeleteCommand)[0]?.args[0].input).toMatchObject({ Key: { pk: 'j' } });
  });
});
