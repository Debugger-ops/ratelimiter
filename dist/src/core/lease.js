const DEFAULTS = {
    enabled: false,
    ttlMs: 1_000,
    maxShare: 0.25,
    maxTokens: 500,
    sweepMs: 250,
    maxEntries: 10_000,
};
export class LeaseCache {
    redis;
    opts;
    entries = new Map();
    /**
     * One acquire in flight per key. Without this, fifty concurrent requests
     * that all miss the lease would fire fifty acquires — a thundering herd on
     * the exact dependency the lease exists to protect, and fifty separate
     * withdrawals of the client's quota.
     */
    inflight = new Map();
    timer = null;
    decisions = 0;
    hits = 0;
    denyHits = 0;
    acquires = 0;
    races = 0;
    coalesced = 0;
    tokensAcquired = 0;
    tokensReturned = 0;
    tokensDropped = 0;
    evictions = 0;
    releaseErrors = 0;
    degradedHits = 0;
    constructor(redis, opts = {}) {
        this.redis = redis;
        this.opts = { ...DEFAULTS, ...opts };
    }
    get enabled() {
        return this.opts.enabled;
    }
    get ttlMs() {
        return this.opts.ttlMs;
    }
    /**
     * Largest lease this policy permits. Returned so the limiter can decline to
     * lease at all when the answer is too small to be worth the bookkeeping — a
     * two-token ceiling saves at most one round trip in two, and costs a map
     * lookup on every request to do it.
     */
    ceilingFor(capacity) {
        return Math.max(1, Math.min(Math.floor(capacity * this.opts.maxShare), this.opts.maxTokens));
    }
    /**
     * The fast path: spend from a live lease. Synchronous by design — this is
     * the entire point of the layer, so it must not touch a promise, a socket,
     * or a timer.
     */
    take(key, cost, nowMs) {
        const e = this.entries.get(key);
        if (e === undefined)
            return 'miss';
        if (e.tokens >= cost && e.expiresAt > nowMs) {
            e.tokens -= cost;
            e.spent += cost;
            this.hits++;
            if (e.tokens === 0) {
                // Drained before the TTL ran out: demand is higher than the last guess.
                this.adaptUp(e);
                e.idleSince = nowMs;
            }
            return 'hit';
        }
        // A refusal recorded for a cost this cheap or cheaper still stands: if the
        // bucket could not cover 1 token a moment ago it cannot cover 5 now. The
        // cost is checked because the reverse is not true — being unable to afford
        // 5 says nothing about affording 1.
        if (e.deniedUntil > nowMs && cost >= e.deniedCost) {
            this.denyHits++;
            return 'denied';
        }
        return 'miss';
    }
    /** ms until a locally memoised refusal lapses. */
    deniedFor(key, nowMs) {
        const e = this.entries.get(key);
        return e ? Math.max(0, e.deniedUntil - nowMs) : 0;
    }
    /**
     * Record a refusal Redis just handed down, so the next few rejected requests
     * from this client cost nothing. Capped at the lease TTL: a memo is a cache
     * of a fact about a shared bucket, and no such cache should outlive the
     * window in which the fact can change.
     */
    noteDenied(key, params, retryAfterMs, nowMs) {
        const until = nowMs + Math.max(1, Math.min(retryAfterMs, this.opts.ttlMs));
        const e = this.entries.get(key);
        if (e) {
            e.deniedUntil = until;
            e.deniedCost = params.need;
            return;
        }
        this.put(key, {
            key,
            capacity: params.capacity,
            ratePerSec: params.ratePerSec,
            tokens: 0,
            size: 0,
            spent: 0,
            nextSize: 1,
            expiresAt: 0,
            acquiredAt: nowMs,
            resetAfterMsAtAcquire: 0,
            idleSince: nowMs,
            deniedUntil: until,
            deniedCost: params.need,
        });
    }
    /**
     * Degraded spend: Redis is unreachable, but this instance is still holding
     * tokens it has already paid for. Serving them is strictly more correct than
     * failing open, because they were withdrawn from a real bucket — the client
     * gets a bounded allowance instead of an unlimited one. Expiry is ignored
     * here for the same reason: the reaper cannot return these tokens anyway
     * while Redis is down, so dropping them buys nothing.
     */
    spendDegraded(key, cost) {
        const e = this.entries.get(key);
        if (e === undefined || e.tokens < cost)
            return false;
        e.tokens -= cost;
        e.spent += cost;
        this.degradedHits++;
        return true;
    }
    /**
     * Drop every lease belonging to a client, returning what is unspent.
     *
     * Support tooling that resets a client's limiter state has to reach the
     * leases too, otherwise the reset clears Redis while every instance quietly
     * keeps serving from tokens it withdrew beforehand — the client stays
     * throttled after being told they were not.
     */
    async forget(keyPrefix) {
        const doomed = [];
        for (const [key, e] of this.entries) {
            if (key.startsWith(keyPrefix)) {
                doomed.push(e);
                this.entries.delete(key);
            }
        }
        await Promise.all(doomed.map((e) => this.release(e).catch(() => { })));
        return doomed.length;
    }
    /** Tokens currently held for a key, for reporting a lower-bound `remaining`. */
    held(key) {
        return this.entries.get(key)?.tokens ?? 0;
    }
    /** Reset horizon reported at acquire time, decayed to now. */
    resetAfterMs(key, nowMs, fallback) {
        const e = this.entries.get(key);
        if (e === undefined)
            return fallback;
        return Math.max(0, e.resetAfterMsAtAcquire - (nowMs - e.acquiredAt));
    }
    /**
     * Refill a lease. Concurrent callers for the same key share one round trip
     * and one withdrawal; they each try `spend` again afterwards.
     */
    async acquire(key, params, nowMs) {
        const pending = this.inflight.get(key);
        if (pending) {
            this.coalesced++;
            return pending;
        }
        const p = this.runAcquire(key, params, nowMs).finally(() => this.inflight.delete(key));
        this.inflight.set(key, p);
        return p;
    }
    async runAcquire(key, params, nowMs) {
        const ceiling = this.ceilingFor(params.capacity);
        const existing = this.entries.get(key);
        const want = Math.max(params.need, Math.min(ceiling, existing?.nextSize ?? this.initialSize(params.ratePerSec, ceiling)));
        this.acquires++;
        const [granted, retryAfterMs, resetAfterMs, bucketRemaining, limit] = await this.redis.leaseAcquire(key, String(params.capacity), String(params.ratePerSec), params.redisNow, String(params.need), String(want));
        if (granted > 0) {
            this.tokensAcquired += granted;
            const prev = this.entries.get(key);
            // Fold any straggler tokens from the outgoing lease into the new one
            // rather than releasing them separately — they are already withdrawn.
            const carried = prev?.tokens ?? 0;
            this.entries.delete(key);
            this.put(key, {
                key,
                capacity: params.capacity,
                ratePerSec: params.ratePerSec,
                tokens: granted + carried,
                size: want,
                spent: 0,
                nextSize: prev?.nextSize ?? want,
                expiresAt: nowMs + this.opts.ttlMs,
                acquiredAt: nowMs,
                resetAfterMsAtAcquire: resetAfterMs,
                idleSince: 0,
                deniedUntil: 0,
                deniedCost: 0,
            });
        }
        return { granted, retryAfterMs, resetAfterMs, bucketRemaining, limit };
    }
    /**
     * First guess at a lease size: roughly one TTL of the policy's sustained
     * rate. It only has to be in the right order of magnitude — the adaptive
     * loop converges within a couple of leases either way.
     */
    initialSize(ratePerSec, ceiling) {
        return Math.max(1, Math.min(ceiling, Math.ceil((ratePerSec * this.opts.ttlMs) / 1000)));
    }
    /**
     * The lease size is a control loop, not a constant, because the right size
     * depends on a client's actual request rate and nothing knows that in
     * advance. A lease drained before its TTL doubles the next one; a lease that
     * expires with tokens left shrinks toward what was actually spent, with a
     * little headroom. Both directions converge on "about one acquire per TTL",
     * which is the behaviour that keeps the hit rate high without stranding
     * quota on quiet clients.
     */
    adaptUp(e) {
        e.nextSize = Math.max(1, e.size * 2);
    }
    adaptDown(e) {
        e.nextSize = Math.max(1, Math.ceil(e.spent * 1.25));
    }
    put(key, entry) {
        if (this.entries.size >= this.opts.maxEntries)
            this.evictOldest();
        this.entries.set(key, entry);
    }
    /**
     * Map iteration is insertion order, and every lease has the same TTL, so the
     * first entry is the oldest acquire — an adequate victim without paying for
     * LRU bookkeeping on the hot path. Its tokens go back to Redis rather than
     * being dropped.
     */
    evictOldest() {
        const victim = this.entries.entries().next();
        if (victim.done)
            return;
        const [key, e] = victim.value;
        this.entries.delete(key);
        this.evictions++;
        if (e.tokens > 0)
            void this.release(e).catch(() => { });
    }
    async release(e) {
        const amount = e.tokens;
        if (amount <= 0)
            return;
        e.tokens = 0;
        try {
            await this.redis.leaseRelease(e.key, String(e.capacity), String(e.ratePerSec), '-1', String(amount));
            this.tokensReturned += amount;
        }
        catch {
            // A failed return is not a correctness problem — the tokens were already
            // debited, and the bucket refills on its own. Count it and move on.
            this.releaseErrors++;
            this.tokensDropped += amount;
        }
    }
    /**
     * Reaper. Returns unspent tokens from expired leases, off the request path
     * and pipelined, in the same spirit as the buffered metrics writer: the
     * bookkeeping a limiter does about itself must not cost more round trips
     * than the limiting.
     */
    async sweep(nowMs = Date.now()) {
        const expiring = [];
        const dropping = [];
        for (const [key, e] of this.entries) {
            if (e.expiresAt <= nowMs && e.tokens > 0) {
                this.adaptDown(e);
                e.idleSince = nowMs;
                expiring.push(e);
            }
            else if (e.idleSince > 0 && nowMs - e.idleSince > this.opts.ttlMs * 2) {
                // Keep a drained entry around for a couple of TTLs so its adapted size
                // survives a short gap in traffic, then let it go.
                dropping.push(key);
            }
        }
        for (const key of dropping)
            this.entries.delete(key);
        if (expiring.length === 0)
            return 0;
        const pipe = this.redis.pipeline();
        let returned = 0;
        for (const e of expiring) {
            const amount = e.tokens;
            e.tokens = 0;
            returned += amount;
            pipe.leaseRelease(e.key, String(e.capacity), String(e.ratePerSec), '-1', String(amount));
        }
        try {
            const results = (await pipe.exec()) ?? [];
            let failed = 0;
            for (const r of results)
                if (r?.[0])
                    failed++;
            this.tokensReturned += returned;
            if (failed)
                this.releaseErrors += failed;
        }
        catch {
            this.releaseErrors++;
            this.tokensDropped += returned;
        }
        return returned;
    }
    start() {
        if (this.timer || !this.opts.enabled)
            return;
        this.timer = setInterval(() => void this.sweep().catch(() => { }), this.opts.sweepMs);
        this.timer.unref?.();
    }
    /**
     * Return everything before the process exits. A rolling deploy without this
     * would strand a lease per client per instance on every restart — invisible
     * in a test, extremely visible as a quota dip during a deploy.
     */
    async stop() {
        if (this.timer)
            clearInterval(this.timer);
        this.timer = null;
        await this.sweep(Number.MAX_SAFE_INTEGER).catch(() => { });
        this.entries.clear();
    }
    /** Called once per decision that takes the leased path. */
    countDecision() {
        this.decisions++;
    }
    stats() {
        const d = this.decisions;
        return {
            enabled: this.opts.enabled,
            entries: this.entries.size,
            decisions: d,
            hits: this.hits,
            acquires: this.acquires,
            hitRate: d ? Math.max(0, d - this.acquires) / d : 0,
            redisOpsSaved: Math.max(0, d - this.acquires),
            denyHits: this.denyHits,
            tokensAcquired: this.tokensAcquired,
            tokensReturned: this.tokensReturned,
            tokensDropped: this.tokensDropped,
            avgLeaseSize: this.acquires ? this.tokensAcquired / this.acquires : 0,
            coalesced: this.coalesced,
            races: this.races,
            evictions: this.evictions,
            releaseErrors: this.releaseErrors,
            degradedHits: this.degradedHits,
        };
    }
}
//# sourceMappingURL=lease.js.map