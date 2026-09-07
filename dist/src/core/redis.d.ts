import Redis, { type RedisOptions } from 'ioredis';
/**
 * ioredis' defineCommand registers the script once and calls it with EVALSHA,
 * transparently re-loading on NOSCRIPT (which happens after a Redis restart or
 * SCRIPT FLUSH). That keeps the hot path to a single 40-byte hash on the wire
 * instead of shipping the whole script per request.
 */
export interface LimiterRedis extends Redis {
    tokenBucket(key: string, capacity: string, rate: string, now: string, cost: string): Promise<[number, number, number, number, number]>;
    slidingWindowLog(key: string, limit: string, windowMs: string, now: string, cost: string, reqId: string): Promise<[number, number, number, number, number]>;
    slidingWindowCounter(key: string, limit: string, windowMs: string, now: string, cost: string): Promise<[number, number, number, number, number]>;
    /** Withdraw a block of tokens for a local lease. See core/lease.ts. */
    leaseAcquire(key: string, capacity: string, rate: string, now: string, need: string, want: string): Promise<[number, number, number, number, number]>;
    /** Return unspent leased tokens to the shared bucket. */
    leaseRelease(key: string, capacity: string, rate: string, now: string, amount: string): Promise<[number, number]>;
}
export declare function createRedis(url?: string, opts?: RedisOptions): LimiterRedis;
/**
 * Resolve once the connection can accept commands. Because the offline queue
 * is disabled, a command issued in the same tick as the constructor would be
 * rejected outright — boot has to wait for this, the request path never does.
 */
export declare function whenReady(client: Redis, timeoutMs?: number): Promise<void>;
