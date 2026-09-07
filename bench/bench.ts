/**
 * Two benchmarks, because they answer different questions.
 *
 *   1. Core   — how long does one limiter decision take, with HTTP out of the
 *               picture? This is the number that decides whether the limiter
 *               is affordable.
 *   2. HTTP   — what does the service sustain end to end, and what does the
 *               limiter add on top of the same route without it?
 *
 * Run:  npm run bench            (core only, no server needed)
 *       npm run bench -- --http  (also drives a running server with autocannon)
 */

import autocannon from 'autocannon';
import { createRedis, whenReady } from '../src/core/redis.js';
import { RateLimiter } from '../src/core/limiter.js';
import { Histogram } from '../src/metrics/histogram.js';
import type { Algorithm, Policy } from '../src/core/types.js';

const BASE = process.env.BASE_URL ?? 'http://localhost:8080';
const CORE_ITERATIONS = Number(process.env.ITERATIONS ?? 20_000);
const CONCURRENCY = Number(process.env.CONCURRENCY ?? 50);

const POLICIES: Record<Algorithm, Policy> = {
  token_bucket: { name: 'bench-tb', algorithm: 'token_bucket', limit: 1e9, windowMs: 60_000, burst: 1e9 },
  sliding_window_log: { name: 'bench-swl', algorithm: 'sliding_window_log', limit: 100_000, windowMs: 60_000 },
  sliding_window_counter: { name: 'bench-swc', algorithm: 'sliding_window_counter', limit: 1e9, windowMs: 60_000 },
};

async function benchCore() {
  const redis = createRedis();
  await whenReady(redis);
  const limiter = new RateLimiter(redis, { keyPrefix: `bench-${Date.now()}` });

  console.log(`\nCore decision cost — ${CORE_ITERATIONS.toLocaleString()} calls, ${CONCURRENCY} in flight`);
  console.log('─'.repeat(78));
  console.log(
    'algorithm'.padEnd(24) +
      'throughput'.padStart(14) +
      'p50'.padStart(10) +
      'p95'.padStart(10) +
      'p99'.padStart(10) +
      'max'.padStart(10),
  );

  const results: Record<string, { rps: number; p50: number; p95: number; p99: number }> = {};

  for (const [name, policy] of Object.entries(POLICIES)) {
    const hist = new Histogram();
    // Warm up: load the script, fill connection buffers, let the JIT settle.
    for (let i = 0; i < 500; i++) await limiter.check(`warm-${i % 50}`, policy);

    const started = performance.now();
    let issued = 0;
    await Promise.all(
      Array.from({ length: CONCURRENCY }, async (_, lane) => {
        while (issued < CORE_ITERATIONS) {
          issued++;
          const t = performance.now();
          await limiter.check(`bench-client-${lane % 20}`, policy);
          hist.record(performance.now() - t);
        }
      }),
    );
    const elapsed = (performance.now() - started) / 1000;
    const s = hist.snapshot();
    const rps = Math.round(CORE_ITERATIONS / elapsed);
    results[name] = { rps, p50: s.p50Ms, p95: s.p95Ms, p99: s.p99Ms };

    console.log(
      name.padEnd(24) +
        `${rps.toLocaleString()}/s`.padStart(14) +
        `${s.p50Ms.toFixed(2)}ms`.padStart(10) +
        `${s.p95Ms.toFixed(2)}ms`.padStart(10) +
        `${s.p99Ms.toFixed(2)}ms`.padStart(10) +
        `${s.maxMs.toFixed(1)}ms`.padStart(10),
    );
  }

  // Baseline: a bare PING is the floor any Redis-backed limiter can reach.
  const pingHist = new Histogram();
  for (let i = 0; i < 2000; i++) {
    const t = performance.now();
    await redis.ping();
    pingHist.record(performance.now() - t);
  }
  console.log('─'.repeat(78));
  console.log(`bare PING (floor)`.padEnd(24) + ''.padStart(14) + `${pingHist.snapshot().p50Ms.toFixed(2)}ms`.padStart(10));

  await redis.quit();
  return results;
}

async function benchHttp() {
  const ok = await fetch(`${BASE}/health`).then((r) => r.ok).catch(() => false);
  if (!ok) {
    console.error(`\nSkipping HTTP benchmark — ${BASE} is not up. Start it with: npm start`);
    return;
  }

  console.log(`\nHTTP end to end — ${BASE}, ${CONCURRENCY} connections, 10s per case`);
  console.log('─'.repeat(78));
  console.log('route'.padEnd(28) + 'req/s'.padStart(12) + 'p50'.padStart(10) + 'p99'.padStart(10) + 'non-2xx'.padStart(12));

  // Put the benchmark client on a quota it will not exhaust, so the number
  // being measured is the cost of deciding rather than the cost of refusing.
  // (Refusing is cheaper, which would flatter the limited row.)
  const admin: Record<string, string> = { 'content-type': 'application/json' };
  if (process.env.ADMIN_TOKEN) admin.authorization = `Bearer ${process.env.ADMIN_TOKEN}`;
  await fetch(`${BASE}/admin/policies/bench`, {
    method: 'PUT',
    headers: admin,
    body: JSON.stringify({ algorithm: 'token_bucket', limit: 6_000_000, windowMs: 60_000, burst: 500_000 }),
  }).catch(() => {});
  await fetch(`${BASE}/admin/assignments/key:bench-hot`, {
    method: 'PUT',
    headers: admin,
    body: JSON.stringify({ policy: 'bench' }),
  }).catch(() => {});

  const cases: { label: string; path: string; headers?: Record<string, string> }[] = [
    { label: '/health (no limiter)', path: '/health' },
    { label: '/api/echo (limited)', path: '/api/echo', headers: { 'x-api-key': 'bench-hot' } },
  ];

  for (const c of cases) {
    const r = await autocannon({
      url: BASE + c.path,
      connections: CONCURRENCY,
      duration: 10,
      headers: c.headers,
    });
    console.log(
      c.label.padEnd(28) +
        Math.round(r.requests.average).toLocaleString().padStart(12) +
        `${r.latency.p50}ms`.padStart(10) +
        `${r.latency.p99}ms`.padStart(10) +
        String(r.non2xx).padStart(12),
    );
  }
  console.log('\nNote: /api/echo returns 429 once the bench client exhausts its quota — the');
  console.log('non-2xx column is the limiter working, not errors. Throughput is what matters here.');
}

/**
 * The lease benchmark answers one question: what does a decision cost when it
 * does not go over the network?
 *
 * Both rows use the same limiter class, the same bucket and the same policy —
 * the only difference is whether the instance withdraws tokens in blocks. The
 * round-trip column is read from Redis' own INFO commandstats rather than
 * counted in application code, so it is what the server actually saw, not what
 * this process believes it sent.
 */
async function benchLease() {
  const redis = createRedis();
  await whenReady(redis);

  // Deliberately an unlimited bucket: this measures the cost of the mechanism,
  // not how fast a small quota runs out. A real client's saving depends on
  // their request rate relative to their quota — see the note in the README.
  const policy: Policy = {
    name: 'bench-lease',
    algorithm: 'token_bucket',
    limit: 1e9,
    windowMs: 60_000,
    burst: 1e9,
  };

  const iterations = Number(process.env.LEASE_ITERATIONS ?? 20_000);

  console.log(`\nLease layer — ${iterations.toLocaleString()} decisions, one client, serial`);
  console.log('─'.repeat(90));
  console.log(
    'mode'.padEnd(22) +
      'throughput'.padStart(14) +
      'p50'.padStart(11) +
      'p95'.padStart(11) +
      'p99'.padStart(11) +
      'redis ops'.padStart(12) +
      'no-RTT'.padStart(9),
  );

  const run = async (label: string, leaseOn: boolean) => {
    const limiter = new RateLimiter(redis, {
      keyPrefix: `benchlease-${leaseOn ? 'on' : 'off'}-${Date.now()}`,
      lease: { enabled: leaseOn },
    });
    const client = 'bench-lease-client';

    for (let i = 0; i < 500; i++) await limiter.check(client, policy); // warm
    await redis.config('RESETSTAT');

    const hist = new Histogram();
    const started = performance.now();
    for (let i = 0; i < iterations; i++) {
      const t = performance.now();
      await limiter.check(client, policy);
      hist.record(performance.now() - t);
    }
    const elapsed = (performance.now() - started) / 1000;

    const evalsha = Number(
      /cmdstat_evalsha:calls=(\d+)/.exec(await redis.info('commandstats'))?.[1] ?? 0,
    );
    const s = hist.snapshot();
    const rps = Math.round(iterations / elapsed);
    const hitRate = leaseOn ? limiter.leases.stats().hitRate : 0;

    console.log(
      label.padEnd(22) +
        `${rps.toLocaleString()}/s`.padStart(14) +
        `${s.p50Ms.toFixed(3)}ms`.padStart(11) +
        `${s.p95Ms.toFixed(3)}ms`.padStart(11) +
        `${s.p99Ms.toFixed(3)}ms`.padStart(11) +
        evalsha.toLocaleString().padStart(12) +
        (leaseOn ? `${(hitRate * 100).toFixed(1)}%` : '—').padStart(9),
    );

    await limiter.close();
    return { rps, p50: s.p50Ms, p99: s.p99Ms, evalsha };
  };

  const off = await run('direct (per-request)', false);
  const on = await run('leased', true);

  console.log('─'.repeat(90));
  console.log(
    `${(on.rps / off.rps).toFixed(1)}x throughput · ` +
      `${(off.p50 / Math.max(on.p50, 1e-6)).toFixed(0)}x lower p50 · ` +
      `${(100 * (1 - on.evalsha / Math.max(off.evalsha, 1))).toFixed(1)}% fewer Redis round trips`,
  );
  console.log('The limit is identical in both rows — see test/lease.test.ts. What changes is how');
  console.log('often the limiter has to ask, not what it is allowed to answer.');

  await redis.quit();
}

async function main() {
  await benchCore();
  if (!process.argv.includes('--no-lease')) await benchLease();
  if (process.argv.includes('--http')) await benchHttp();
  console.log('');
  process.exit(0);
}

void main();
