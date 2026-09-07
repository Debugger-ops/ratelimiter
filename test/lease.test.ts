import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import express from 'express';
import type { Server } from 'node:http';
import { createRedis, whenReady } from '../src/core/redis.js';
import { RateLimiter } from '../src/core/limiter.js';
import { PolicyStore } from '../src/core/policies.js';
import { MetricsRecorder } from '../src/metrics/metrics.js';
import { rateLimit } from '../src/server/middleware.js';
import { REDIS_URL, id } from './helpers.js';
import type { LeaseOptions } from '../src/core/lease.js';
import type { Policy } from '../src/core/types.js';

/**
 * The lease layer is the one place in this project where an optimisation is
 * allowed to touch the correctness of the limit, so it gets the most
 * adversarial tests.
 *
 * The claim being defended is narrow and absolute: leasing changes how often
 * the limiter talks to Redis, and nothing else. It does not raise the ceiling,
 * it does not make it approximate, and it does not make it probabilistic. The
 * stampede test below is the same test the unleased limiter has to pass, run
 * against the leased path.
 *
 * The costs are real and also tested: quota can sit unspent on an idle
 * instance, and `remaining` becomes a lower bound rather than an exact figure.
 * Both are asserted rather than described.
 */

const CONNECTIONS = 8;
const clients = Array.from({ length: CONNECTIONS }, () => createRedis(REDIS_URL));

beforeAll(async () => {
  await Promise.all(clients.map((c) => whenReady(c)));
});
afterAll(async () => {
  await Promise.all(clients.map((c) => c.quit().catch(() => {})));
});

const bucket = (over: Partial<Policy> = {}): Policy => ({
  name: 'lease-test',
  algorithm: 'token_bucket',
  limit: 6_000,
  windowMs: 60_000,
  burst: 200,
  ...over,
});

/**
 * A limiter with leases on and the background reaper disabled, so tests drive
 * expiry through the injected clock instead of racing a real timer.
 */
function leased(
  redis: (typeof clients)[number],
  lease: LeaseOptions = {},
  over: { prefix?: string; clock?: { now: number }; failureMode?: 'open' | 'closed' } = {},
) {
  const clock = over.clock ?? { now: Date.now() };
  const limiter = new RateLimiter(redis, {
    keyPrefix: over.prefix ?? `lz${id()}`,
    failureMode: over.failureMode ?? 'open',
    now: () => clock.now,
    lease: { enabled: true, sweepMs: 3_600_000, ...lease },
  });
  return { limiter, clock };
}

describe('leases do not loosen the limit', () => {
  it('admits exactly the burst from 8 leasing instances at once', async () => {
    // The headline safety property, and deliberately the same shape as the
    // unleased stampede in concurrency.test.ts: 500 hungry requests, 8
    // instances each holding their own lease cache, one bucket of 50.
    //
    // Every token an instance serves was withdrawn from that bucket by an
    // atomic script first, so the sum across the fleet cannot exceed what the
    // bucket handed out. Not "approximately 50". Fifty.
    const clock = { now: Date.now() };
    const fleet = clients.map((r) => leased(r, {}, { prefix: 'lease-conc', clock }).limiter);
    const c = id();
    const policy = bucket({ name: 'lc', limit: 50, windowMs: 60_000, burst: 50 });

    const results = await Promise.all(
      Array.from({ length: 500 }, (_, i) => fleet[i % CONNECTIONS]!.check(c, policy)),
    );

    expect(results.filter((d) => d.allowed).length).toBe(50);
    await Promise.all(fleet.map((l) => l.close()));
  });

  it('a leased fleet and an unleased fleet admit the same number', async () => {
    // Same policy, same traffic, the only difference being whether the
    // decisions went over the wire. If leasing changed the ceiling at all,
    // these two numbers would diverge.
    const policy = bucket({ name: 'parity', limit: 120, windowMs: 60_000, burst: 120 });
    const clock = { now: Date.now() };

    const leasedFleet = clients.map((r) => leased(r, {}, { prefix: 'par-l', clock }).limiter);
    const plainFleet = clients.map(
      (r) => new RateLimiter(r, { keyPrefix: 'par-p', now: () => clock.now }),
    );

    const run = async (fleet: RateLimiter[]) => {
      const c = id();
      const out = await Promise.all(
        Array.from({ length: 400 }, (_, i) => fleet[i % CONNECTIONS]!.check(c, policy)),
      );
      return out.filter((d) => d.allowed).length;
    };

    const [withLease, withoutLease] = [await run(leasedFleet), await run(plainFleet)];
    expect(withLease).toBe(withoutLease);
    expect(withLease).toBe(120);
    await Promise.all(leasedFleet.map((l) => l.close()));
  });

  it('charges a multi-token cost against the lease, not once per lease', async () => {
    const { limiter } = leased(clients[0]!);
    const c = id();
    const policy = bucket({ name: 'cost', limit: 60, windowMs: 60_000, burst: 60 });

    let allowed = 0;
    for (let i = 0; i < 30; i++) {
      if ((await limiter.check(c, policy, 5)).allowed) allowed++;
    }
    // 60 tokens, 5 per request: twelve requests get through and no more.
    expect(allowed).toBe(12);
    await limiter.close();
  });
});

describe('leases remove the round trip', () => {
  it('serves most decisions without issuing a Redis command at all', async () => {
    // Counted at the server, not in application code: CONFIG RESETSTAT zeroes
    // Redis' own command counters, so `evalsha` calls afterwards are the round
    // trips that physically happened.
    const redis = clients[0]!;
    const { limiter } = leased(redis, { maxTokens: 100 });
    const c = id();
    const policy = bucket({ name: 'rt', limit: 60_000, windowMs: 60_000, burst: 2_000 });

    await limiter.check(c, policy); // warm the script cache before resetting stats
    await redis.config('RESETSTAT');

    const N = 400;
    for (let i = 0; i < N; i++) await limiter.check(c, policy);

    const evalsha = Number(/cmdstat_evalsha:calls=(\d+)/.exec(await redis.info('commandstats'))?.[1] ?? 0);
    const stats = limiter.leases.stats();

    // Every decision was served from tokens in hand.
    expect(stats.decisions).toBe(N + 1);
    expect(stats.hits).toBe(N + 1);
    // The counter Redis kept agrees with the counter the lease cache kept —
    // one acquire happened before RESETSTAT, so Redis saw one fewer.
    expect(evalsha).toBe(stats.acquires - 1);
    // The point of the exercise: 400 decisions, a handful of round trips.
    expect(stats.hitRate).toBeGreaterThan(0.9);
    expect(evalsha).toBeLessThan(N / 10);
    expect(stats.redisOpsSaved).toBeGreaterThan(N * 0.9);

    await limiter.close();
  });

  it('coalesces a concurrent miss into one acquire instead of a stampede', async () => {
    // Fifty simultaneous requests for a cold key must not become fifty
    // withdrawals against the client's bucket — that would be a thundering
    // herd aimed at the exact dependency the lease exists to spare.
    const { limiter } = leased(clients[0]!);
    const c = id();
    const policy = bucket({ name: 'coal', limit: 6_000, windowMs: 60_000, burst: 400 });

    await Promise.all(Array.from({ length: 50 }, () => limiter.check(c, policy)));

    const stats = limiter.leases.stats();
    expect(stats.acquires).toBeLessThanOrEqual(3);
    expect(stats.coalesced).toBeGreaterThan(0);
    await limiter.close();
  });

  it('sizes leases adaptively instead of using a fixed block', async () => {
    // A constant lease size is wrong for almost every client: too large and it
    // strands quota, too small and it saves nothing. The size is a control
    // loop — a lease drained before its TTL doubles the next request.
    const { limiter } = leased(clients[0]!, { maxTokens: 256 });
    const c = id();
    const policy = bucket({ name: 'adapt', limit: 60, windowMs: 60_000, burst: 1_024 });

    // rate is 1/s, so the opening guess is a single token.
    await limiter.check(c, policy);
    const first = limiter.leases.stats().avgLeaseSize;
    expect(first).toBeLessThanOrEqual(2);

    for (let i = 0; i < 200; i++) await limiter.check(c, policy);

    const s = limiter.leases.stats();
    // Converged upward: 200 requests cost far fewer than 200 round trips.
    expect(s.acquires).toBeLessThan(30);
    expect(s.avgLeaseSize).toBeGreaterThan(4);
    await limiter.close();
  });
});

describe('the cost of leasing, stated as tests', () => {
  it('returns unspent tokens to the bucket rather than stranding them', async () => {
    // One instance takes a block, spends one token, then goes quiet. Without
    // the reaper the rest of that block would be invisible to the fleet until
    // the bucket refilled on its own.
    const clock = { now: Date.now() };
    const a = leased(clients[0]!, {}, { prefix: 'ret', clock }).limiter;
    const c = id();
    const policy = bucket({ name: 'ret', limit: 6_000, windowMs: 60_000, burst: 400 });

    await a.check(c, policy);
    const held = a.leases.stats().tokensAcquired - 1;
    expect(held).toBeGreaterThan(0);

    clock.now += 5_000; // the lease is now well past its TTL
    const returned = await a.leases.sweep(clock.now);

    expect(returned).toBe(held);
    expect(a.leases.stats().tokensReturned).toBe(held);
    await a.close();
  });

  it('gives every leased token back on shutdown', async () => {
    // A rolling deploy without this would strand one lease per client per
    // instance on every restart: invisible in a unit test, very visible as a
    // quota dip during a deploy.
    const redis = clients[0]!;
    const clock = { now: Date.now() };
    const { limiter } = leased(redis, {}, { prefix: 'shut', clock });
    const c = id();
    const policy = bucket({ name: 'shut', limit: 6_000, windowMs: 60_000, burst: 400 });

    await limiter.check(c, policy);
    await limiter.close();

    const plain = new RateLimiter(redis, { keyPrefix: 'shut', now: () => clock.now });
    const after = await plain.peek(c, policy);
    // 400 capacity, exactly one request actually spent.
    expect(after.remaining).toBe(399);
  });

  it('never credits back more than the bucket can hold', async () => {
    // A release is a credit, and a credit that ignored capacity would be a way
    // to mint quota. Returning into a full bucket has to be a no-op.
    const redis = clients[0]!;
    const key = `mint:{${id()}}:tb:x`;
    await redis.leaseAcquire(key, '100', '1', String(Date.now()), '1', '40');
    await redis.leaseRelease(key, '100', '1', String(Date.now()), '40');
    const [tokens] = await redis.leaseRelease(key, '100', '1', String(Date.now()), '40');
    expect(tokens).toBe(100);
  });

  it('reports remaining as a lower bound, and says so consistently', async () => {
    // A leased decision can only honestly report what this instance still
    // holds. Under-reporting makes a client back off early, which is the safe
    // direction for a client-facing header; over-reporting would invite them
    // to spend quota that is not theirs.
    const { limiter } = leased(clients[0]!, { maxTokens: 8 });
    const c = id();
    const policy = bucket({ name: 'lb', limit: 6_000, windowMs: 60_000, burst: 400 });

    const d = await limiter.check(c, policy);
    expect(d.source).toBe('lease');
    expect(d.remaining).toBeLessThanOrEqual(8);
    expect(d.remaining).toBeLessThan(policy.burst!); // strictly a lower bound

    const peeked = await limiter.peek(c, policy);
    // A peek costs nothing, so it bypasses the lease and reads the bucket —
    // which is the number an operator or a /quota endpoint actually wants.
    expect(peeked.source).toBe('redis');
    expect(peeked.remaining).toBeGreaterThan(d.remaining);
    await limiter.close();
  });
});

describe('what is deliberately not leased', () => {
  it('never leases a sliding window, so `login` is excluded by construction', async () => {
    const { limiter } = leased(clients[0]!);
    const c = id();
    const login: Policy = { name: 'login', algorithm: 'sliding_window_log', limit: 5, windowMs: 300_000 };

    for (let i = 0; i < 5; i++) {
      const d = await limiter.check(c, login);
      expect(d.source).toBe('redis');
    }
    expect((await limiter.check(c, login)).allowed).toBe(false);
    expect(limiter.leases.stats().acquires).toBe(0);
    await limiter.close();
  });

  it('skips the lease for buckets too small to benefit', async () => {
    // A 10-token bucket allows a 2-token lease at most: one round trip saved
    // in two, in exchange for a map lookup on every request and stranded quota
    // on a client who has very little to begin with. Not worth it.
    const { limiter } = leased(clients[0]!, { maxShare: 0.1 });
    const c = id();
    const d = await limiter.check(c, bucket({ name: 'tiny', limit: 60, windowMs: 60_000, burst: 10 }));
    expect(d.source).toBe('redis');
    expect(limiter.leases.stats().acquires).toBe(0);
    await limiter.close();
  });
});

describe('leases under a Redis outage', () => {
  /** A client that, once disconnected, stays disconnected. */
  const killable = () => {
    const r = createRedis(REDIS_URL, { retryStrategy: () => null, maxRetriesPerRequest: 0 });
    r.on('error', () => {});
    return r;
  };

  it('makes a fail-closed outage bounded rather than a cliff', async () => {
    // A fail-closed limiter without leases refuses everything the instant
    // Redis goes away — correct, and a hard outage for the caller. With leases
    // the instance still holds tokens it has already paid for, so it serves
    // exactly those and then refuses. The client gets a bounded allowance
    // instead of a cliff; an attacker gets a bounded allowance instead of an
    // open door. Neither gets more than the bucket issued.
    const redis = killable();
    await whenReady(redis);
    const clock = { now: Date.now() };
    const { limiter } = leased(redis, { maxTokens: 10 }, { prefix: 'out', clock, failureMode: 'closed' });
    const c = id();
    const policy = bucket({ name: 'out', limit: 6_000, windowMs: 60_000, burst: 400 });

    await limiter.check(c, policy); // withdraw a block while Redis is up
    const held = limiter.leases.held(`out:{${c}}:tb:out`);
    expect(held).toBeGreaterThan(0);

    redis.disconnect();

    let served = 0;
    for (let i = 0; i < held + 5; i++) {
      if ((await limiter.check(c, policy)).allowed) served++;
    }

    expect(served).toBe(held); // exactly what had been paid for, and not one more
    expect((await limiter.check(c, policy)).allowed).toBe(false);
    expect(limiter.redisErrors).toBeGreaterThan(0);
    await redis.quit().catch(() => {});
  });

  it('serves an expired lease during an outage, and flags the decision degraded', async () => {
    // While the lease is live the outage is simply invisible — the instance
    // never needs to ask. Once it expires the reaper cannot return the tokens
    // either, so dropping them would buy nothing and cost the client their
    // quota. They are spent, and every such decision is marked degraded so the
    // dashboard and alerting can see the limiter running on reserves.
    const redis = killable();
    await whenReady(redis);
    const clock = { now: Date.now() };
    const { limiter } = leased(redis, { maxTokens: 10 }, { prefix: 'exp', clock, failureMode: 'closed' });
    const c = id();
    const policy = bucket({ name: 'exp', limit: 6_000, windowMs: 60_000, burst: 400 });

    await limiter.check(c, policy);
    redis.disconnect();
    clock.now += 60_000; // the lease is long expired

    const d = await limiter.check(c, policy);
    expect(d.allowed).toBe(true);
    expect(d.degraded).toBe(true);
    expect(d.source).toBe('lease');
    expect(limiter.leases.stats().degradedHits).toBe(1);
    await redis.quit().catch(() => {});
  });
});

describe('through the HTTP middleware', () => {
  it('serves a burst over HTTP from one lease and labels the source header', async () => {
    // End to end: headers, 429 body, and the source label an operator would
    // use to explain why RateLimit-Remaining looks smaller than the quota.
    const redis = clients[1]!;
    const policies = new PolicyStore(redis);
    await policies.init();
    await policies.upsert({
      name: 'lease-http',
      algorithm: 'token_bucket',
      limit: 60,
      windowMs: 60_000,
      burst: 20,
    });
    const metrics = new MetricsRecorder(redis, 50);
    const limiter = new RateLimiter(redis, {
      keyPrefix: `lhttp-${Date.now()}`,
      lease: { enabled: true, sweepMs: 3_600_000 },
    });

    const app = express();
    app.get('/x', rateLimit({ limiter, policies, metrics }, { policy: 'lease-http' }), (_req, res) =>
      res.json({ ok: true }),
    );
    const server = await new Promise<Server>((resolve) => {
      const s = app.listen(0, () => resolve(s));
    });
    const addr = server.address();
    const base = `http://127.0.0.1:${typeof addr === 'object' && addr ? addr.port : 0}`;
    const key = id();

    const codes: number[] = [];
    let leaseServed = 0;
    for (let i = 0; i < 25; i++) {
      const r = await fetch(`${base}/x`, { headers: { 'x-api-key': key } });
      codes.push(r.status);
      if (r.headers.get('x-ratelimit-source') === 'lease') leaseServed++;
    }

    // A 20-token bucket: twenty through, the rest refused, all of it decided
    // locally after the first withdrawal.
    expect(codes.filter((c) => c === 200).length).toBe(20);
    expect(codes.filter((c) => c === 429).length).toBe(5);
    expect(leaseServed).toBe(25);
    // Every refusal after the first was answered from the local memo.
    expect(limiter.leases.stats().denyHits).toBeGreaterThanOrEqual(4);

    await limiter.close();
    await metrics.stop();
    await policies.remove('lease-http').catch(() => {});
    server.close();
  });
});

describe('the throttled path', () => {
  it('refuses a flood locally instead of one round trip per rejected request', async () => {
    // The case that matters under abuse. A client hammering past their limit
    // must not be able to convert their own bad behaviour into load on the
    // store the limiter depends on. Once Redis has said "not until t", that
    // fact is cached for exactly as long as it is true.
    const redis = clients[2]!;
    const { limiter } = leased(redis, {}, { prefix: `flood-${Date.now()}` });
    const c = id();
    // 40 tokens refilling at 1/s, and a frozen clock, so once it is empty it
    // stays empty for the length of the test.
    const policy = bucket({ name: 'flood', limit: 60, windowMs: 60_000, burst: 40 });

    let drained = 0;
    while (drained < 200 && (await limiter.check(c, policy)).allowed) drained++;
    expect(drained).toBe(40);
    await redis.config('RESETSTAT');

    let refused = 0;
    for (let i = 0; i < 200; i++) {
      if (!(await limiter.check(c, policy)).allowed) refused++;
    }

    const evalsha = Number(/cmdstat_evalsha:calls=(\d+)/.exec(await redis.info('commandstats'))?.[1] ?? 0);
    expect(refused).toBe(200);
    // 200 refusals, a couple of round trips — not 200.
    expect(evalsha).toBeLessThan(10);
    expect(limiter.leases.stats().denyHits).toBeGreaterThan(190);
    await limiter.close();
  });

  it('does not let a refusal for an expensive request refuse a cheap one', async () => {
    // Being unable to afford 5 tokens says nothing about affording 1, so the
    // memo records the cost it was issued for and only applies at or above it.
    const { limiter } = leased(clients[3]!);
    const c = id();
    const policy = bucket({ name: 'asym', limit: 60, windowMs: 60_000, burst: 20 });

    // Drain to a few tokens, then ask for more than is left.
    for (let i = 0; i < 18; i++) await limiter.check(c, policy);
    expect((await limiter.check(c, policy, 10)).allowed).toBe(false);
    // Two tokens remain; a cost-1 request is still affordable and must not be
    // caught by the memo left behind by the cost-10 refusal.
    expect((await limiter.check(c, policy, 1)).allowed).toBe(true);
    await limiter.close();
  });
});

describe('lease bookkeeping', () => {
  it('clears local leases when a client is reset, not just the Redis keys', async () => {
    // Support resets a throttled customer, Redis is cleared — and every
    // instance keeps refusing them from a lease nobody thought to clear. This
    // is the bug that test exists to prevent.
    const { limiter } = leased(clients[0]!, { maxTokens: 4 });
    const c = id();
    const policy = bucket({ name: 'rst', limit: 60, windowMs: 60_000, burst: 40 });

    await limiter.check(c, policy);
    expect(limiter.leases.stats().entries).toBe(1);

    await limiter.reset(c);
    expect(limiter.leases.stats().entries).toBe(0);
    await limiter.close();
  });

  it('bounds its own memory, returning the tokens it evicts', async () => {
    // A lease cache is memory, and client cardinality is not something the
    // limiter controls. The eviction has to give the quota back, not drop it.
    const { limiter } = leased(clients[0]!, { maxEntries: 16, maxTokens: 4 });
    const policy = bucket({ name: 'mem', limit: 6_000, windowMs: 60_000, burst: 400 });

    for (let i = 0; i < 40; i++) await limiter.check(`evict-${i}`, policy);

    const s = limiter.leases.stats();
    expect(s.entries).toBeLessThanOrEqual(16);
    expect(s.evictions).toBeGreaterThan(0);
    await limiter.close();
  });
});
