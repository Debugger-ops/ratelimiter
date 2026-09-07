import { type LimiterRedis } from '../src/core/redis.js';
import { RateLimiter } from '../src/core/limiter.js';
import type { LimiterOptions } from '../src/core/types.js';
export declare const REDIS_URL: string;
export declare function client(): LimiterRedis;
/** A limiter with an isolated key prefix and a clock the test controls. */
export declare function fixture(opts?: Partial<LimiterOptions>): {
    redis: LimiterRedis;
    limiter: RateLimiter;
    clock: {
        now: number;
    };
    ready: () => Promise<void>;
    advance: (ms: number) => void;
    close: () => Promise<void>;
};
export declare const id: () => string;
