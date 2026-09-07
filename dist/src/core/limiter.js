import { randomUUID } from 'node:crypto';
import { LeaseCache } from './lease.js';
import { normalizePolicy } from './types.js';
export class RateLimiter {
    redis;
    prefix;
    failureMode;
    now;
    /** Local token leases. Disabled unless asked for; see core/lease.ts. */
    leases;
    /** Rolling count of Redis failures, surfaced on /admin/stats. */
    redisErrors = 0;
    lastRedisError = null;
    constructor(redis, opts = {}) {
        this.redis = redis;
        this.prefix = opts.keyPrefix ?? 'rl';
        this.failureMode = opts.failureMode ?? 'open';
        // -1 tells the Lua scripts to read the Redis server clock, so every app
        // instance shares one timeline. Tests override this with a fake clock.
        this.now = opts.now ?? (() => -1);
        this.leases = new LeaseCache(redis, opts.lease);
        this.leases.start();
    }
    /**
     * A wall clock for lease bookkeeping.
     *
     * Lease expiry is a purely local concern — how long *this* process may hold
     * tokens — so unlike the bucket arithmetic it does not need the Redis
     * timeline, and asking Redis for the time would defeat the entire purpose of
     * not talking to Redis. Tests inject a clock here so lease expiry can be
     * driven deterministically rather than with sleeps.
     */
    wall() {
        const t = this.now();
        return t < 0 ? Date.now() : t;
    }
    /** Release every outstanding lease and stop the reaper. */
    async close() {
        await this.leases.stop();
    }
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
    keyFor(clientId, policyName, algo) {
        return `${this.prefix}:{${clientId}}:${algo}:${policyName}`;
    }
    async check(clientId, policy, cost = 1) {
        const p = normalizePolicy(policy);
        const key = this.keyFor(clientId, p.name, shortAlgo(p.algorithm));
        if (this.shouldLease(p.algorithm, p.burst, cost)) {
            return this.checkLeased(key, clientId, p, cost);
        }
        return this.checkDirect(key, clientId, p, cost);
    }
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
    shouldLease(algorithm, burst, cost) {
        if (!this.leases.enabled || algorithm !== 'token_bucket' || cost < 1)
            return false;
        const ceiling = this.leases.ceilingFor(burst);
        return ceiling >= 2 && ceiling >= cost;
    }
    /**
     * The leased path. In the common case this is a map lookup and a
     * subtraction: no promise resolution against a socket, no round trip, no
     * chance of a Redis blip turning into a request failure.
     */
    async checkLeased(key, clientId, p, cost) {
        const started = performance.now();
        const nowMs = this.wall();
        const ratePerSec = (p.limit * 1000) / p.windowMs;
        this.leases.countDecision();
        const params = { capacity: p.burst, ratePerSec, need: cost, redisNow: String(this.now()) };
        const fast = this.leases.take(key, cost, nowMs);
        if (fast === 'hit')
            return this.leaseDecision(key, clientId, p, nowMs, started, true, 0);
        if (fast === 'denied') {
            // Refused without asking. Under a flood this is the common case, which
            // is the point: exceeding a limit must not be a way to make the limiter
            // generate load on the store it is protecting.
            return this.leaseDecision(key, clientId, p, nowMs, started, false, this.leases.deniedFor(key, nowMs));
        }
        try {
            const r = await this.leases.acquire(key, params, nowMs);
            if (this.leases.take(key, cost, nowMs) === 'hit') {
                return this.leaseDecision(key, clientId, p, nowMs, started, true, 0);
            }
            if (r.granted === 0) {
                // A real throttle: the shared bucket could not cover this request.
                this.leases.noteDenied(key, params, r.retryAfterMs, nowMs);
                const d = this.leaseDecision(key, clientId, p, nowMs, started, false, r.retryAfterMs);
                // This one number is straight from Redis, so report it rather than the
                // decayed local estimate.
                d.resetAfterMs = r.resetAfterMs;
                return d;
            }
            // Rare: coalesced with another caller's acquire and their requests drank
            // the lease first. One direct check settles it — no retry loop, because
            // a loop here is a retry storm aimed at the thing we are protecting.
            this.leases.races++;
            return this.checkDirect(key, clientId, p, cost);
        }
        catch (err) {
            this.noteRedisError(err);
            // Redis is gone, but tokens already withdrawn are still ours to spend.
            // This is the quietly valuable property of the lease layer: fail-open
            // stops meaning "unlimited" and starts meaning "bounded by whatever this
            // instance had already paid for".
            if (this.leases.spendDegraded(key, cost)) {
                const d = this.leaseDecision(key, clientId, p, nowMs, started, true, 0);
                d.degraded = true;
                return d;
            }
            return this.failureDecision(clientId, p, started);
        }
    }
    leaseDecision(key, clientId, p, nowMs, started, allowed, retryAfterMs) {
        return {
            allowed,
            // Tokens this instance can still serve without asking Redis. It is a
            // lower bound on what the client actually has left, not an estimate of
            // it — which is the direction a client-facing header should err in, since
            // it makes callers back off early rather than late.
            remaining: this.leases.held(key),
            limit: p.burst,
            quota: p.limit,
            windowMs: p.windowMs,
            retryAfterMs: allowed ? 0 : retryAfterMs,
            resetAfterMs: this.leases.resetAfterMs(key, nowMs, p.windowMs),
            policy: p.name,
            algorithm: p.algorithm,
            clientId,
            degraded: false,
            source: 'lease',
            latencyMs: performance.now() - started,
        };
    }
    async checkDirect(key, clientId, p, cost) {
        const started = performance.now();
        const now = String(this.now());
        try {
            let res;
            switch (p.algorithm) {
                case 'token_bucket': {
                    // limit/windowMs is the sustained rate; burst is the bucket size.
                    const ratePerSec = (p.limit * 1000) / p.windowMs;
                    res = await this.redis.tokenBucket(key, String(p.burst), String(ratePerSec), now, String(cost));
                    break;
                }
                case 'sliding_window_log':
                    res = await this.redis.slidingWindowLog(key, String(p.limit), String(p.windowMs), now, String(cost), randomUUID());
                    break;
                case 'sliding_window_counter':
                    res = await this.redis.slidingWindowCounter(key, String(p.limit), String(p.windowMs), now, String(cost));
                    break;
            }
            const [allowed, remaining, retryAfterMs, resetAfterMs, limit] = res;
            return {
                allowed: allowed === 1,
                remaining,
                limit,
                quota: p.limit,
                windowMs: p.windowMs,
                retryAfterMs,
                resetAfterMs,
                policy: p.name,
                algorithm: p.algorithm,
                clientId,
                degraded: false,
                source: 'redis',
                latencyMs: performance.now() - started,
            };
        }
        catch (err) {
            this.noteRedisError(err);
            return this.failureDecision(clientId, p, started);
        }
    }
    noteRedisError(err) {
        this.redisErrors++;
        this.lastRedisError = err instanceof Error ? err.message : String(err);
    }
    /**
     * Fail open by default: a limiter outage should degrade protection, not
     * availability. Endpoints where the limit IS the security control (login,
     * password reset, signup) should be mounted with failureMode 'closed'
     * instead — losing Redis there means losing brute-force protection, which is
     * worse than serving 503s.
     */
    failureDecision(clientId, p, started) {
        const open = this.failureMode === 'open';
        return {
            allowed: open,
            remaining: open ? p.limit : 0,
            limit: p.limit,
            quota: p.limit,
            windowMs: p.windowMs,
            retryAfterMs: open ? 0 : 1000,
            resetAfterMs: p.windowMs,
            policy: p.name,
            algorithm: p.algorithm,
            clientId,
            degraded: true,
            source: 'redis',
            latencyMs: performance.now() - started,
        };
    }
    /** Read current state without consuming a token — for a "check my quota" endpoint. */
    async peek(clientId, policy) {
        return this.check(clientId, policy, 0);
    }
    /** Drop all limiter state for a client. Useful in tests and for support tooling. */
    async reset(clientId) {
        // Leases first. Clearing Redis while this instance still holds withdrawn
        // tokens would leave the client throttled by a lease that no longer
        // corresponds to anything in the shared bucket.
        await this.leases.forget(`${this.prefix}:{${clientId}}:`);
        const pattern = `${this.prefix}:{${clientId}}:*`;
        let cursor = '0';
        let deleted = 0;
        do {
            const [next, keys] = await this.redis.scan(cursor, 'MATCH', pattern, 'COUNT', 200);
            cursor = next;
            if (keys.length)
                deleted += await this.redis.del(...keys);
        } while (cursor !== '0');
        return deleted;
    }
}
function shortAlgo(a) {
    if (a === 'token_bucket')
        return 'tb';
    if (a === 'sliding_window_log')
        return 'swl';
    return 'swc';
}
//# sourceMappingURL=limiter.js.map