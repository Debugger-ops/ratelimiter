import { validatePolicy } from './types.js';
const POLICY_HASH = 'rl:policies';
const ASSIGN_HASH = 'rl:assignments';
const CHANNEL = 'rl:policies:changed';
export const DEFAULT_POLICIES = [
    {
        name: 'free',
        algorithm: 'token_bucket',
        limit: 60,
        windowMs: 60_000,
        burst: 10,
        note: '60 req/min sustained, 10 in a burst. Small bucket keeps free traffic smooth.',
    },
    {
        name: 'pro',
        algorithm: 'token_bucket',
        limit: 600,
        windowMs: 60_000,
        burst: 120,
        note: '600 req/min with a 120-request burst allowance for batch jobs.',
    },
    {
        name: 'enterprise',
        algorithm: 'sliding_window_counter',
        limit: 6000,
        windowMs: 60_000,
        note: 'High cardinality, high volume — O(1) memory matters more than exactness here.',
    },
    {
        name: 'login',
        algorithm: 'sliding_window_log',
        limit: 5,
        windowMs: 300_000,
        note: '5 attempts / 5 min, exact. Brute-force protection cannot tolerate boundary bursts.',
    },
];
export const DEFAULT_POLICY_NAME = 'free';
/**
 * Policies live in Redis so every app instance agrees, and are cached in
 * process so the hot path never pays a round trip to read one. A pub/sub
 * message invalidates the cache on write; the TTL sweep is the backstop for a
 * dropped message (pub/sub is at-most-once).
 */
export class PolicyStore {
    redis;
    cache = new Map();
    assignments = new Map();
    loadedAt = 0;
    ttlMs;
    subscriber = null;
    constructor(redis, opts = {}) {
        this.redis = redis;
        this.ttlMs = opts.ttlMs ?? 30_000;
    }
    async init(subscriber) {
        const existing = await this.redis.hlen(POLICY_HASH);
        if (existing === 0) {
            const seed = {};
            for (const p of DEFAULT_POLICIES)
                seed[p.name] = JSON.stringify(p);
            await this.redis.hset(POLICY_HASH, seed);
        }
        await this.refresh();
        if (subscriber) {
            this.subscriber = subscriber;
            await subscriber.subscribe(CHANNEL);
            subscriber.on('message', (channel) => {
                if (channel === CHANNEL)
                    void this.refresh();
            });
        }
    }
    async refresh() {
        const [raw, assigned] = await Promise.all([
            this.redis.hgetall(POLICY_HASH),
            this.redis.hgetall(ASSIGN_HASH),
        ]);
        const next = new Map();
        for (const [name, json] of Object.entries(raw)) {
            try {
                next.set(name, JSON.parse(json));
            }
            catch {
                // A single corrupt record must not take the whole store down.
            }
        }
        this.cache = next;
        this.assignments = new Map(Object.entries(assigned));
        this.loadedAt = Date.now();
    }
    async maybeRefresh() {
        if (Date.now() - this.loadedAt > this.ttlMs)
            await this.refresh().catch(() => { });
    }
    list() {
        return [...this.cache.values()].sort((a, b) => a.name.localeCompare(b.name));
    }
    listAssignments() {
        return Object.fromEntries(this.assignments);
    }
    get(name) {
        return this.cache.get(name);
    }
    /** Resolve which policy governs a client, falling back to the default tier. */
    resolve(clientId, override) {
        void this.maybeRefresh();
        const name = override ?? this.assignments.get(clientId) ?? DEFAULT_POLICY_NAME;
        return (this.cache.get(name) ??
            this.cache.get(DEFAULT_POLICY_NAME) ??
            DEFAULT_POLICIES[0]);
    }
    async upsert(policy) {
        validatePolicy(policy);
        await this.redis.hset(POLICY_HASH, policy.name, JSON.stringify(policy));
        await this.redis.publish(CHANNEL, policy.name);
        await this.refresh();
        return policy;
    }
    async remove(name) {
        if (name === DEFAULT_POLICY_NAME)
            throw new Error(`cannot delete the default policy "${name}"`);
        const n = await this.redis.hdel(POLICY_HASH, name);
        await this.redis.publish(CHANNEL, name);
        await this.refresh();
        return n > 0;
    }
    async assign(clientId, policyName) {
        if (!this.cache.has(policyName))
            throw new Error(`unknown policy "${policyName}"`);
        await this.redis.hset(ASSIGN_HASH, clientId, policyName);
        await this.redis.publish(CHANNEL, `assign:${clientId}`);
        await this.refresh();
    }
    async unassign(clientId) {
        await this.redis.hdel(ASSIGN_HASH, clientId);
        await this.redis.publish(CHANNEL, `assign:${clientId}`);
        await this.refresh();
    }
    async close() {
        if (this.subscriber)
            await this.subscriber.unsubscribe(CHANNEL).catch(() => { });
    }
}
//# sourceMappingURL=policies.js.map