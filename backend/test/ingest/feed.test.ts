import { readFileSync } from 'node:fs';
import { describe, expect, it } from 'vitest';
import { parseFeed } from '../../src/ingest/feed.js';

const fixture = (name: string) =>
  readFileSync(new URL(`../fixtures/nws/${name}`, import.meta.url), 'utf8');

describe('parseFeed', () => {
  it('returns the features of a FeatureCollection', () => {
    const feed = parseFeed(fixture('active-ca-mixed.json'));
    expect('features' in feed && feed.features.length).toBe(4);
  });

  it('gives zero features for an empty collection', () => {
    expect(parseFeed(fixture('empty.json'))).toEqual({ features: [] });
  });

  it('returns an error, not a throw, for non-JSON', () => {
    expect(parseFeed('<html>502 Bad Gateway</html>')).toHaveProperty('error');
    expect(parseFeed('')).toHaveProperty('error');
  });

  it('returns an error for JSON that is not a FeatureCollection', () => {
    for (const body of ['null', '42', '[]', '{"type":"Feature"}', '{"type":"FeatureCollection"}']) {
      expect(parseFeed(body)).toHaveProperty('error');
    }
  });
});
