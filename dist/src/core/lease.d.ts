import type { LimiterRedis } from './redis.js';
/**
 * Local token leases.
 *
 * The unavoidable cost of a Redis-backed limiter is one network round trip per
 * request. At 0.13 ms that is affordable; at 50k req/s across a fleet it is
 * 50k round trips per second landing on a single shared node, and Redis
 * becomes the capacity ceiling for the whole service.
 *
 * A lease moves the decision off the network. An instance withdraws a block of
 * tokens from the client's shared bucket in one round trip and then serves
 * requests out of that block from process memory — a map lookup and a
 * subtraction, no I/O — until the block runs out or gets old.
 *
 * The safety argument is short, which is the point:
 *
 *   an instance can only admit what it has already paid Redis for,
 *   so total admitted <= total withdrawn <= what the bucket allowed.
 *
 * Leasing therefore cannot over-admit. It is not an approximation of the limit
 * the way sliding_window_counter is an approximation; the ceiling is exactly
 * as hard as before. What it gives up is *promptness*: tokens sitting unspent
 * on an idle instance are unavailable to the others until they are returned,
 * so a client can briefly see less than their full quota. That is the whole
 * trade, and the two mechanisms below bound it:
 *
 *   - every lease has a TTL, and a reaper returns unspent tokens off the
 *     request path (lease_release.lua) instead of letting them sit;
 *   - a single lease is capped at a fraction of the bucket, so no one instance
 *     can corner a client's burst.
 *
 * Only token_bucket can be leased. A bucket is a credit pool, and withdrawing
 * credit from a pool is a well-defined operation. A sliding window is a
 * statement about the arrival times of individual requests, and there is no
 * coherent way to withdraw part of one — which conveniently means `login`, the
 * policy where the limit *is* the security control, is excluded by
 * construction rather than by remembering to exclude it.
 */
export interface LeaseOptions {
    enabled?: boolean;
    /**
     * How long an instance may hold unspent tokens. This is the knob that sets
     * the worst-case staleness of a client's quota, so it is deliberately short:
     * a client can be under-served by at most the tokens outstanding, for at
     * most this long.
     */
    ttlMs?: number;
    /**
     * Ceiling on one lease as a fraction of the bucket. Stops a single instance
     * from cornering a client's entire burst allowance.
     */
    maxShare?: number;
    /** Absolute ceiling on one lease, whatever the bucket size. */
    maxTokens?: number;
    /** How often the reaper returns expired leases. */
    sweepMs?: number;
    /** Bound on tracked clients per instance — leases are memory. */
    maxEntries?: number;
}
export interface LeaseParams {
    capacity: number;
    ratePerSec: number;
    /** ms cost of the request that triggered the acquire. */
    need: number;
    /** Passed straight to Lua: -1 means "use the Redis clock". */
    redisNow: string;
}
/** What the fast path was able to decide without talking to Redis. */
export type Take = 'hit' | 'denied' | 'miss';
export interface AcquireResult {
    granted: number;
    retryAfterMs: number;
    resetAfterMs: number;
    bucketRemaining: number;
    limit: number;
}
export interface LeaseStats {
    enabled: boolean;
    entries: number;
    /** Decisions that went down the leased path at all. */
    decisions: number;
    /** Decisions satisfied out of tokens already held. */
    hits: number;
    /** Round trips actually made to refill a lease. */
    acquires: number;
    /**
     * Fraction of leased decisions that cost no Redis command. Measured against
     * acquires rather than hits, because a decision that had to refill first is
     * a hit *and* a round trip — counting it as a hit alone would flatter the
     * number, and this figure is quoted in the README.
     */
    hitRate: number;
    /** Round trips the lease layer removed — the headline number. */
    redisOpsSaved: number;
    /** Refusals answered from the local memo instead of a round trip. */
    denyHits: number;
    tokensAcquired: number;
    tokensReturned: number;
    tokensDropped: number;
    avgLeaseSize: number;
    /** Misses that piggybacked on an acquire already in flight for that key. */
    coalesced: number;
    /**
     * Coalesced callers that still found the lease empty and fell back to one
     * direct Redis check. The number to watch: persistently non-zero means the
     * lease size is too small for the concurrency.
     */
    races: number;
    /** Entries evicted because the instance was tracking too many clients. */
    evictions: number;
    releaseErrors: number;
    degradedHits: number;
}
export declare class LeaseCache {
    private readonly redis;
    private readonly opts;
    private entries;
    /**
     * One acquire in flight per key. Without this, fifty concurrent requests
     * that all miss the lease would fire fifty acquires — a thundering herd on
     * the exact dependency the lease exists to protect, and fifty separate
     * withdrawals of the client's quota.
     */
    private inflight;
    private timer;
    decisions: number;
    hits: number;
    denyHits: number;
    acquires: number;
    races: number;
    coalesced: number;
    tokensAcquired: number;
    tokensReturned: number;
    tokensDropped: number;
    evictions: number;
    releaseErrors: number;
    degradedHits: number;
    constructor(redis: LimiterRedis, opts?: LeaseOptions);
    get enabled(): boolean;
    get ttlMs(): number;
    /**
     * Largest lease this policy permits. Returned so the limiter can decline to
     * lease at all when the answer is too small to be worth the bookkeeping — a
     * two-token ceiling saves at most one round trip in two, and costs a map
     * lookup on every request to do it.
     */
    ceilingFor(capacity: number): number;
    /**
     * The fast path: spend from a live lease. Synchronous by design — this is
     * the entire point of the layer, so it must not touch a promise, a socket,
     * or a timer.
     */
    take(key: string, cost: number, nowMs: number): Take;
    /** ms until a locally memoised refusal lapses. */
    deniedFor(key: string, nowMs: number): number;
    /**
     * Record a refusal Redis just handed down, so the next few rejected requests
     * from this client cost nothing. Capped at the lease TTL: a memo is a cache
     * of a fact about a shared bucket, and no such cache should outlive the
     * window in which the fact can change.
     */
    noteDenied(key: string, params: LeaseParams, retryAfterMs: number, nowMs: number): void;
    /**
     * Degraded spend: Redis is unreachable, but this instance is still holding
     * tokens it has already paid for. Serving them is strictly more correct than
     * failing open, because they were withdrawn from a real bucket — the client
     * gets a bounded allowance instead of an unlimited one. Expiry is ignored
     * here for the same reason: the reaper cannot return these tokens anyway
     * while Redis is down, so dropping them buys nothing.
     */
    spendDegraded(key: string, cost: number): boolean;
    /**
     * Drop every lease belonging to a client, returning what is unspent.
     *
     * Support tooling that resets a client's limiter state has to reach the
     * leases too, otherwise the reset clears Redis while every instance quietly
     * keeps serving from tokens it withdrew beforehand — the client stays
     * throttled after being told they were not.
     */
    forget(keyPrefix: string): Promise<number>;
    /** Tokens currently held for a key, for reporting a lower-bound `remaining`. */
    held(key: string): number;
    /** Reset horizon reported at acquire time, decayed to now. */
    resetAfterMs(key: string, nowMs: number, fallback: number): number;
    /**
     * Refill a lease. Concurrent callers for the same key share one round trip
     * and one withdrawal; they each try `spend` again afterwards.
     */
    acquire(key: string, params: LeaseParams, nowMs: number): Promise<AcquireResult>;
    private runAcquire;
    /**
     * First guess at a lease size: roughly one TTL of the policy's sustained
     * rate. It only has to be in the right order of magnitude — the adaptive
     * loop converges within a couple of leases either way.
     */
    private initialSize;
    /**
     * The lease size is a control loop, not a constant, because the right size
     * depends on a client's actual request rate and nothing knows that in
     * advance. A lease drained before its TTL doubles the next one; a lease that
     * expires with tokens left shrinks toward what was actually spent, with a
     * little headroom. Both directions converge on "about one acquire per TTL",
     * which is the behaviour that keeps the hit rate high without stranding
     * quota on quiet clients.
     */
    private adaptUp;
    private adaptDown;
    private put;
    /**
     * Map iteration is insertion order, and every lease has the same TTL, so the
     * first entry is the oldest acquire — an adequate victim without paying for
     * LRU bookkeeping on the hot path. Its tokens go back to Redis rather than
     * being dropped.
     */
    private evictOldest;
    private release;
    /**
     * Reaper. Returns unspent tokens from expired leases, off the request path
     * and pipelined, in the same spirit as the buffered metrics writer: the
     * bookkeeping a limiter does about itself must not cost more round trips
     * than the limiting.
     */
    sweep(nowMs?: number): Promise<number>;
    start(): void;
    /**
     * Return everything before the process exits. A rolling deploy without this
     * would strand a lease per client per instance on every restart — invisible
     * in a test, extremely visible as a quota dip during a deploy.
     */
    stop(): Promise<void>;
    /** Called once per decision that takes the leased path. */
    countDecision(): void;
    stats(): LeaseStats;
}
