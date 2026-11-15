import { describe, expect, it } from 'vitest';
import { ping } from './index.js';

describe('backend', () => {
  it('responds to ping', () => {
    expect(ping()).toBe('relay-backend');
  });
});
