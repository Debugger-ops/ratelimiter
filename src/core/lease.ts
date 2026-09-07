import type { ChainableCommander } from 'ioredis';
import type { LimiterRedis } from './redis.js';

/**
 * ioredis registers custom commands on the pipeline as well as the client, but
 * its published types cannot know about a script defined at runtime. This is
 * the narrow shape the reaper needs — one cast at one call site, rather than an
 * `any` spreading through the sweep.
 */
type LeasePipeline = ChainableCommander & {
  leaseRelease(
    key: string,
    capacity: string,
    rate: string,
    now: string,
    amount: string,
  ): LeasePipeline;
};

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

const DEFAULTS: Required<LeaseOptions> = {
  enabled: false,
  ttlMs: 1_000,
  maxShare: 0.25,
  maxTokens: 500,
  sweepMs: 250,
  maxEntries: 10_000,
};

export interface LeaseParams {
  capacity: number;
  ratePerSec: number;
  /** ms cost of the request that triggered the acquire. */
  need: number;
  /** Passed straight to Lua: -1 means "use the Redis clock". */
  redisNow: string;
}

interface Entry {
  key: string;
  /**
   * The bucket parameters this lease was taken under. They are stashed on the
   * entry rather than looked up at release time because the policy may have
   * been edited in between — crediting the unspent tokens back under a *new*
   * capacity would return tokens the old bucket never issued.
   */
  capacity: number;
  ratePerSec: number;
  /** Unspent whole tokens held by this instance. */
  tokens: number;
  /** Size of the lease currently held, for the adaptive sizing loop. */
  size: number;
  /** Tokens spent out of the current lease. */
  spent: number;
  /** Size to ask for next time — see `adaptUp`/`adaptDown`. */
  nextSize: number;
  expiresAt: number;
  acquiredAt: number;
  /** Bucket reset horizon at acquire time, decayed for reporting. */
  resetAfterMsAtAcquire: number;
  /** Set when the entry is drained or expired; the reaper drops it later. */
  idleSince: number;
  /**
   * Local memo of a refusal: Redis said this client cannot afford a request of
   * `deniedCost` until this instant. Honouring it locally is not a guess —
   * `retryAfterMs` is the exact time the bucket needs to accrue the deficit —
   * and without it the throttled path costs a round trip per rejected request,
   * which is precisely the traffic pattern an abusive client produces.
   */
  deniedUntil: number;
  deniedCost: number;
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

export class LeaseCache {
  private readonly opts: Required<LeaseOptions>;
  private entries = new Map<string, Entry>();
  /**
   * One acquire in flight per key. Without this, fifty concurrent requests
   * that all miss the lease would fire fifty acquires — a thundering herd on
   * the exact dependency the lease exists to protect, and fifty separate
   * withdrawals of the client's quota.
   */
  private inflight = new Map<string, Promise<AcquireResult>>();
  private timer: NodeJS.Timeout | null = null;

  public decisions = 0;
  public hits = 0;
  public denyHits = 0;
  public acquires = 0;
  public races = 0;
  public coalesced = 0;
  public tokensAcquired = 0;
  public tokensReturned = 0;
  public tokensDropped = 0;
  public evictions = 0;
  public releaseErrors = 0;
  public degradedHits = 0;

  constructor(
    private readonly redis: LimiterRedis,
    opts: LeaseOptions = {},
  ) {
    this.opts = { ...DEFAULTS, ...opts };
  }

  get enabled(): boolean {
    return this.opts.enabled;
  }

  get ttlMs(): number {
    return this.opts.ttlMs;
  }

  /**
   * Largest lease this policy permits. Returned so the limiter can decline to
   * lease at all when the answer is too small to be worth the bookkeeping — a
   * two-token ceiling saves at most one round trip in two, and costs a map
   * lookup on every request to do it.
   */
  ceilingFor(capacity: number): number {
    return Math.max(1, Math.min(Math.floor(capacity * this.opts.maxShare), this.opts.maxTokens));
  }

  /**
   * The fast path: spend from a live lease. Synchronous by design — this is
   * the entire point of the layer, so it must not touch a promise, a socket,
   * or a timer.
   */
  take(key: string, cost: number, nowMs: number): Take {
    const e = this.entries.get(key);
    if (e === undefined) return 'miss';

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
  deniedFor(key: string, nowMs: number): number {
    const e = this.entries.get(key);
    return e ? Math.max(0, e.deniedUntil - nowMs) : 0;
  }

  /**
   * Record a refusal Redis just handed down, so the next few rejected requests
   * from this client cost nothing. Capped at the lease TTL: a memo is a cache
   * of a fact about a shared bucket, and no such cache should outlive the
   * window in which the fact can change.
   */
  noteDenied(key: string, params: LeaseParams, retryAfterMs: number, nowMs: number): void {
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
  spendDegraded(key: string, cost: number): boolean {
    const e = this.entries.get(key);
    if (e === undefined || e.tokens < cost) return false;
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
  async forget(keyPrefix: string): Promise<number> {
    const doomed: Entry[] = [];
    for (const [key, e] of this.entries) {
      if (key.startsWith(keyPrefix)) {
        doomed.push(e);
        this.entries.delete(key);
      }
    }
    await Promise.all(doomed.map((e) => this.release(e).catch(() => {})));
    return doomed.length;
  }

  /** Tokens currently held for a key, for reporting a lower-bound `remaining`. */
  held(key: string): number {
    return this.entries.get(key)?.tokens ?? 0;
  }

  /** Reset horizon reported at acquire time, decayed to now. */
  resetAfterMs(key: string, nowMs: number, fallback: number): number {
    const e = this.entries.get(key);
    if (e === undefined) return fallback;
    return Math.max(0, e.resetAfterMsAtAcquire - (nowMs - e.acquiredAt));
  }

  /**
   * Refill a lease. Concurrent callers for the same key share one round trip
   * and one withdrawal; they each try `spend` again afterwards.
   */
  async acquire(key: string, params: LeaseParams, nowMs: number): Promise<AcquireResult> {
    const pending = this.inflight.get(key);
    if (pending) {
      this.coalesced++;
      return pending;
    }

    const p = this.runAcquire(key, params, nowMs).finally(() => this.inflight.delete(key));
    this.inflight.set(key, p);
    return p;
  }

  private async runAcquire(key: string, params: LeaseParams, nowMs: number): Promise<AcquireResult> {
    const ceiling = this.ceilingFor(params.capacity);
    const existing = this.entries.get(key);
    const want = Math.max(
      params.need,
      Math.min(ceiling, existing?.nextSize ?? this.initialSize(params.ratePerSec, ceiling)),
    );

    this.acquires++;
    const [granted, retryAfterMs, resetAfterMs, bucketRemaining, limit] = await this.redis.leaseAcquire(
      key,
      String(params.capacity),
      String(params.ratePerSec),
      params.redisNow,
      String(params.need),
      String(want),
    );

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
  private initialSize(ratePerSec: number, ceiling: number): number {
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
  private adaptUp(e: Entry): void {
    e.nextSize = Math.max(1, e.size * 2);
  }

  private adaptDown(e: Entry): void {
    e.nextSize = Math.max(1, Math.ceil(e.spent * 1.25));
  }

  private put(key: string, entry: Entry): void {
    if (this.entries.size >= this.opts.maxEntries) this.evictOldest();
    this.entries.set(key, entry);
  }

  /**
   * Map iteration is insertion order, and every lease has the same TTL, so the
   * first entry is the oldest acquire — an adequate victim without paying for
   * LRU bookkeeping on the hot path. Its tokens go back to Redis rather than
   * being dropped.
   */
  private evictOldest(): void {
    const victim = this.entries.entries().next();
    if (victim.done) return;
    const [key, e] = victim.value;
    this.entries.delete(key);
    this.evictions++;
    if (e.tokens > 0) void this.release(e).catch(() => {});
  }

  private async release(e: Entry): Promise<void> {
    const amount = e.tokens;
    if (amount <= 0) return;
    e.tokens = 0;
    try {
      await this.redis.leaseRelease(
        e.key,
        String(e.capacity),
        String(e.ratePerSec),
        '-1',
        String(amount),
      );
      this.tokensReturned += amount;
    } catch {
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
  async sweep(nowMs = Date.now()): Promise<number> {
    const expiring: Entry[] = [];
    const dropping: string[] = [];

    for (const [key, e] of this.entries) {
      if (e.expiresAt <= nowMs && e.tokens > 0) {
        this.adaptDown(e);
        e.idleSince = nowMs;
        expiring.push(e);
      } else if (e.idleSince > 0 && nowMs - e.idleSince > this.opts.ttlMs * 2) {
        // Keep a drained entry around for a couple of TTLs so its adapted size
        // survives a short gap in traffic, then let it go.
        dropping.push(key);
      }
    }
    for (const key of dropping) this.entries.delete(key);
    if (expiring.length === 0) return 0;

    const pipe = this.redis.pipeline() as LeasePipeline;
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
      for (const r of results) if (r?.[0]) failed++;
      this.tokensReturned += returned;
      if (failed) this.releaseErrors += failed;
    } catch {
      this.releaseErrors++;
      this.tokensDropped += returned;
    }
    return returned;
  }

  start(): void {
    if (this.timer || !this.opts.enabled) return;
    this.timer = setInterval(() => void this.sweep().catch(() => {}), this.opts.sweepMs);
    this.timer.unref?.();
  }

  /**
   * Return everything before the process exits. A rolling deploy without this
   * would strand a lease per client per instance on every restart — invisible
   * in a test, extremely visible as a quota dip during a deploy.
   */
  async stop(): Promise<void> {
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
    await this.sweep(Number.MAX_SAFE_INTEGER).catch(() => {});
    this.entries.clear();
  }

  /** Called once per decision that takes the leased path. */
  countDecision(): void {
    this.decisions++;
  }

  stats(): LeaseStats {
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
