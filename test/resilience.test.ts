import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import Redis from 'ioredis';
import { createRedis, whenReady } from '../src/core/redis.js';
import { RateLimiter } from '../src/core/limiter.js';
import { PolicyStore, DEFAULT_POLICIES } from '../src/core/policies.js';
import { REDIS_URL, id } from './helpers.js';
import type { Policy } from '../src/core/types.js';

const policy: Policy = { name: 'res', algorithm: 'token_bucket', limit: 10, windowMs: 60_000, burst: 10 };

// Nothing is listening here. Every command rejects.
const DEAD_URL = 'redis://127.0.0.1:6399';
const dead = createRedis(DEAD_URL, { retryStrategy: () => null, maxRetriesPerRequest: 0 });
dead.on('error', () => {}); // expected; keep the run quiet

const live = createRedis(REDIS_URL);

beforeAll(async () => whenReady(live));
afterAll(async () => {
  await Promise.all([dead.quit().catch(() => {}), live.quit().catch(() => {})]);
});

describe('Redis failure modes', () => {
  it('fails open: traffic flows and the decision is flagged degraded', async () => {
    const limiter = new RateLimiter(dead, { failureMode: 'open' });
    const d = await limiter.check(id(), policy);
    expect(d.allowed).toBe(true);
    expect(d.degraded).toBe(true);
    expect(limiter.redisErrors).toBeGreaterThan(0);
  });

  it('fails closed: traffic is refused, for endpoints where the limit IS the control', async () => {
    const limiter = new RateLimiter(dead, { failureMode: 'closed' });
    const d = await limiter.check(id(), policy);
    expect(d.allowed).toBe(false);
    expect(d.degraded).toBe(true);
    expect(d.retryAfterMs).toBeGreaterThan(0);
  });

  it('does not hang when Redis is unreachable', async () => {
    const limiter = new RateLimiter(dead, { failureMode: 'open' });
    const started = Date.now();
    await limiter.check(id(), policy);
    expect(Date.now() - started).toBeLessThan(3000);
  });
});

describe('policy store', () => {
  it('seeds defaults, resolves tiers, and honours assignment', async () => {
    const store = new PolicyStore(live);
    await store.init();
    expect(store.list().length).toBeGreaterThanOrEqual(DEFAULT_POLICIES.length);

    const c = id();
    expect(store.resolve(c).name).toBe('free'); // default tier
    await store.assign(c, 'pro');
    expect(store.resolve(c).name).toBe('pro');
    expect(store.resolve(c, 'login').name).toBe('login'); // per-route override wins
    await store.unassign(c);
    expect(store.resolve(c).name).toBe('free');
  });

  it('propagates a policy change to another instance via pub/sub', async () => {
    const subscriber = new Redis(REDIS_URL);
    const a = new PolicyStore(live, { ttlMs: 60_000 });
    const b = new PolicyStore(live, { ttlMs: 60_000 });
    await a.init();
    await b.init(subscriber);

    const name = `tmp-${Date.now()}`;
    await a.upsert({ name, algorithm: 'token_bucket', limit: 42, windowMs: 1000 });

    // b never polled; it learned from the invalidation message.
    await new Promise((r) => setTimeout(r, 250));
    expect(b.get(name)?.limit).toBe(42);

    await a.remove(name);
    await b.close();
    await subscriber.quit();
  });

  it('rejects nonsense before it can reach the hot path', async () => {
    const store = new PolicyStore(live);
    await store.init();
    await expect(store.upsert({ name: 'bad', algorithm: 'magic' as never, limit: 1, windowMs: 1 })).rejects.toThrow();
    await expect(store.upsert({ name: 'bad', algorithm: 'token_bucket', limit: -5, windowMs: 1000 })).rejects.toThrow();
    await expect(store.upsert({ name: 'bad name!', algorithm: 'token_bucket', limit: 5, windowMs: 1000 })).rejects.toThrow();
    // A sorted set with a million members per client is a memory incident.
    await expect(
      store.upsert({ name: 'huge', algorithm: 'sliding_window_log', limit: 1_000_000, windowMs: 60_000 }),
    ).rejects.toThrow(/memory/);
    await expect(store.remove('free')).rejects.toThrow(/default/);
  });
});
