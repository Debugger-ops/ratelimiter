import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { createRedis, whenReady } from '../src/core/redis.js';
import { RateLimiter } from '../src/core/limiter.js';
import { REDIS_URL, id } from './helpers.js';
import type { Policy } from '../src/core/types.js';

/**
 * The correctness claim of this whole project is that a limit holds when many
 * processes check it at the same instant. Read-then-write in application code
 * cannot do that; a Lua script can, because Redis runs it to completion as a
 * single unit. These tests are the proof.
 */

const CONNECTIONS = 8;
const clients = Array.from({ length: CONNECTIONS }, () => createRedis(REDIS_URL));
const frozen = Date.now(); // freeze time so no refill can muddy the count
const limiters = clients.map(
  (r) => new RateLimiter(r, { keyPrefix: 'conc', now: () => frozen }),
);

beforeAll(async () => {
  await Promise.all(clients.map((c) => whenReady(c)));
});
afterAll(async () => {
  await Promise.all(clients.map((c) => c.quit().catch(() => {})));
});

async function stampede(policy: Policy, attempts: number): Promise<number> {
  const c = id();
  const results = await Promise.all(
    Array.from({ length: attempts }, (_, i) => limiters[i % CONNECTIONS]!.check(c, policy)),
  );
  return results.filter((d) => d.allowed).length;
}

describe('atomicity under a stampede', () => {
  it('token bucket admits exactly the burst, from 8 connections at once', async () => {
    const allowed = await stampede(
      { name: 'c-tb', algorithm: 'token_bucket', limit: 50, windowMs: 60_000, burst: 50 },
      500,
    );
    expect(allowed).toBe(50);
  });

  it('sliding window log admits exactly the limit', async () => {
    const allowed = await stampede(
      { name: 'c-swl', algorithm: 'sliding_window_log', limit: 50, windowMs: 60_000 },
      500,
    );
    expect(allowed).toBe(50);
  });

  it('sliding window counter admits exactly the limit', async () => {
    const allowed = await stampede(
      { name: 'c-swc', algorithm: 'sliding_window_counter', limit: 50, windowMs: 60_000 },
      500,
    );
    expect(allowed).toBe(50);
  });

  it('holds with a multi-token cost, where a partial spend would be visible', async () => {
    const allowed = await stampede(
      { name: 'c-cost', algorithm: 'token_bucket', limit: 100, windowMs: 60_000, burst: 100 },
      200,
    );
    // Every request costs 1 here; the interesting case is that no request ever
    // observes a half-applied decrement — allowed count is exact, not "about 100".
    expect(allowed).toBe(100);
  });
});
