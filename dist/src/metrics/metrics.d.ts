import type Redis from 'ioredis';
import type { Decision } from '../core/types.js';
import { Histogram } from './histogram.js';
export type Verdict = 'allowed' | 'throttled';
export interface SecondBucket {
    t: number;
    allowed: number;
    throttled: number;
    byClient: Record<string, {
        allowed: number;
        throttled: number;
    }>;
}
/**
 * Metrics are aggregated in process and flushed on an interval, not written
 * per request.
 *
 * Writing one HINCRBY per request would double the Redis round trips the
 * limiter costs — the counters would be more expensive than the decision they
 * describe. Buffering trades a bounded window of loss on a hard crash (at most
 * `flushMs` of counts) for halving the load on the shared dependency, which is
 * the right trade for observability data.
 */
export declare class MetricsRecorder {
    private readonly redis;
    private readonly flushMs;
    private buffer;
    private timer;
    readonly latency: Histogram;
    readonly redisLatency: Histogram;
    flushes: number;
    flushErrors: number;
    constructor(redis: Redis, flushMs?: number);
    start(): void;
    stop(): Promise<void>;
    record(decision: Decision, totalLatencyMs?: number): void;
    flush(): Promise<void>;
}
export declare class MetricsReader {
    private readonly redis;
    constructor(redis: Redis);
    /** Per-second series for the last `seconds` seconds, oldest first. */
    series(seconds?: number): Promise<SecondBucket[]>;
    /** All-time totals per client. */
    totals(): Promise<{
        allowed: number;
        throttled: number;
        byClient: Record<string, {
            allowed: number;
            throttled: number;
        }>;
    }>;
    resetAll(): Promise<void>;
}
