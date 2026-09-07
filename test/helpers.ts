import { randomUUID } from 'node:crypto';
import { createRedis, whenReady, type LimiterRedis } from '../src/core/redis.js';
import { RateLimiter } from '../src/core/limiter.js';
import type { LimiterOptions } from '../src/core/types.js';

export const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';

export function client(): LimiterRedis {
  return createRedis(REDIS_URL);
}

/** A limiter with an isolated key prefix and a clock the test controls. */
export function fixture(opts: Partial<LimiterOptions> = {}) {
  const redis = client();
  const clock = { now: Date.now() };
  const limiter = new RateLimiter(redis, {
    keyPrefix: `t${randomUUID().slice(0, 8)}`,
    now: () => clock.now,
    ...opts,
  });
  return {
    redis,
    limiter,
    clock,
    ready: () => whenReady(redis),
    advance: (ms: number) => {
      clock.now += ms;
    },
    close: async () => {
      await redis.quit().catch(() => {});
    },
  };
}

export const id = () => `c-${randomUUID().slice(0, 8)}`;
