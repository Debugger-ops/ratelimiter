import type { LimiterRedis } from './redis.js';
import { LeaseCache } from './lease.js';
import { type Decision, type LimiterOptions, type Policy } from './types.js';
export declare class RateLimiter {
    private readonly redis;
    private readonly prefix;
    private readonly failureMode;
    private readonly now;
    /** Local token leases. Disabled unless asked for; see core/lease.ts. */
    readonly leases: LeaseCache;
    /** Rolling count of Redis failures, surfaced on /admin/stats. */
    redisErrors: number;
    lastRedisError: string | null;
    constructor(redis: LimiterRedis, opts?: LimiterOptions);
    /**
     * A wall clock for lease bookkeeping.
     *
     * Lease expiry is a purely local concern — how long *this* process may hold
     * tokens — so unlike the bucket arithmetic it does not need the Redis
     * timeline, and asking Redis for the time would defeat the entire purpose of
     * not talking to Redis. Tests inject a clock here so lease expiry can be
     * driven deterministically rather than with sleeps.
     */
    private wall;
    /** Release every outstanding lease and stop the reaper. */
    close(): Promise<void>;
    /**
     * Key layout: rl:{client}:algo:policy
     *
     * The braces are a Redis Cluster hash tag. Everything for one client hashes
     * to one slot, which is required for sliding_window_counter (it touches two
     * derived keys in one script) and keeps a client's traffic on one node.
     *
     * The policy name is part of the key on purpose: moving a client from `free`
     * to `pro` gives them a fresh bucket rather than carrying debt across tiers.
     */
    private keyFor;
    check(clientId: string, policy: Policy, cost?: number): Promise<Decision>;
    /**
     * Three conditions, each of them a real constraint rather than a
     * configuration preference:
     *
     *   - Only a token bucket can be leased. Withdrawing credit from a pool is
     *     well defined; withdrawing part of a statement about arrival times is
     *     not. This is also what keeps `login` off the lease path.
     *   - cost 0 is a peek, and a peek must report the shared bucket, not this
     *     instance's slice of it.
     *   - A ceiling below two tokens means a lease would save at most one round
     *     trip in two while costing a map lookup on every request. Not worth it;
     *     small buckets go straight to Redis.
     */
    private shouldLease;
    /**
     * The leased path. In the common case this is a map lookup and a
     * subtraction: no promise resolution against a socket, no round trip, no
     * chance of a Redis blip turning into a request failure.
     */
    private checkLeased;
    private leaseDecision;
    private checkDirect;
    private noteRedisError;
    /**
     * Fail open by default: a limiter outage should degrade protection, not
     * availability. Endpoints where the limit IS the security control (login,
     * password reset, signup) should be mounted with failureMode 'closed'
     * instead — losing Redis there means losing brute-force protection, which is
     * worse than serving 503s.
     */
    private failureDecision;
    /** Read current state without consuming a token — for a "check my quota" endpoint. */
    peek(clientId: string, policy: Policy): Promise<Decision>;
    /** Drop all limiter state for a client. Useful in tests and for support tooling. */
    reset(clientId: string): Promise<number>;
}
