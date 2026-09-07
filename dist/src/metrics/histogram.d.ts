/**
 * Log-bucketed latency histogram.
 *
 * Storing every sample to compute p99 is O(requests); sorting on read is worse.
 * This keeps 3 significant-ish digits over ~5 orders of magnitude in a fixed
 * array of counters, so recording is O(1) with no allocation and the whole
 * thing is a few kB regardless of traffic. Same idea as HdrHistogram, minus
 * the parts a demo does not need.
 */
export declare class Histogram {
    private readonly bucketCount;
    private readonly buckets;
    private readonly base;
    count: number;
    sum: number;
    max: number;
    min: number;
    constructor(bucketCount?: number, 
    /** Ratio between adjacent bucket bounds. 1.06 => ~6% worst-case error. */
    growth?: number);
    record(valueMs: number): void;
    private indexFor;
    private valueAt;
    percentile(p: number): number;
    snapshot(): {
        count: number;
        meanMs: number;
        minMs: number;
        p50Ms: number;
        p95Ms: number;
        p99Ms: number;
        maxMs: number;
    };
    reset(): void;
}
