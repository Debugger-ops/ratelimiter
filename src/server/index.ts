import express from 'express';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createRedis, whenReady } from '../core/redis.js';
import { RateLimiter } from '../core/limiter.js';
import { PolicyStore } from '../core/policies.js';
import { MetricsRecorder, MetricsReader } from '../metrics/metrics.js';
import { rateLimit, defaultIdentify } from './middleware.js';
import { adminRouter } from './admin.js';

const here = dirname(fileURLToPath(import.meta.url));
const PORT = Number(process.env.PORT ?? 8080);
const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';

async function main() {
  const redis = createRedis(REDIS_URL);
  const subscriber = createRedis(REDIS_URL); // pub/sub connections cannot issue normal commands
  await Promise.all([whenReady(redis), whenReady(subscriber)]);

  // Local token leases are opt-in. They do not change the limit — see
  // core/lease.ts and test/lease.test.ts — but they do change the latency
  // profile and the shape of `RateLimit-Remaining`, and a limiter should not
  // change its observable behaviour without being asked.
  const lease = {
    enabled: process.env.LEASE_ENABLED === '1',
    ttlMs: Number(process.env.LEASE_TTL_MS ?? 1000),
  };

  // Two limiters, two failure modes. Most endpoints prefer availability; the
  // ones where the limit is the security control prefer refusing service.
  const limiter = new RateLimiter(redis, { failureMode: 'open', lease });
  // The strict limiter fronts `login`, a sliding window, which the lease layer
  // declines to touch by construction. Passing the option anyway keeps the two
  // limiters configured identically rather than relying on that fact.
  const strictLimiter = new RateLimiter(redis, { failureMode: 'closed', lease });

  const policies = new PolicyStore(redis);
  await policies.init(subscriber);

  const metrics = new MetricsRecorder(redis, Number(process.env.METRICS_FLUSH_MS ?? 250));
  metrics.start();

  const app = express();
  app.set('trust proxy', Number(process.env.TRUST_PROXY_HOPS ?? 0));
  app.disable('x-powered-by');
  app.use(express.json({ limit: '64kb' }));



  const deps = { limiter, policies, metrics };
  const strictDeps = { limiter: strictLimiter, policies, metrics };

  // ---- unlimited ----------------------------------------------------------

  app.get('/health', async (_req, res) => {
    const ok = redis.status === 'ready';
    res.status(ok ? 200 : 503).json({ status: ok ? 'ok' : 'degraded', redis: redis.status });
  });

  // ---- the demo API, behind the limiter -----------------------------------

  app.get('/api/echo', rateLimit(deps), (req, res) => {
    res.json({ ok: true, client: req.rateLimit?.clientId, policy: req.rateLimit?.policy });
  });

  // A report costs 5 tokens: one endpoint that is 5x more expensive to serve
  // should consume 5x the quota, otherwise the limit does not track real cost.
  app.post('/api/report', rateLimit(deps, { cost: 5 }), (req, res) => {
    res.json({ ok: true, cost: 5, remaining: req.rateLimit?.remaining });
  });

  // Fixed `login` policy regardless of tier, exact algorithm, fail closed.
  app.post('/api/login', rateLimit(strictDeps, { policy: 'login' }), (_req, res) => {
    res.json({ ok: true, note: 'demo endpoint — always succeeds' });
  });

  // cost 0 reads state without spending it.
  app.get('/api/quota', async (req, res) => {
    const clientId = defaultIdentify(req);
    const policy = policies.resolve(clientId);
    const d = await limiter.peek(clientId, policy);
    res.json({
      clientId,
      policy: policy.name,
      algorithm: policy.algorithm,
      limit: d.limit,
      remaining: d.remaining,
      resetInMs: d.resetAfterMs,
    });
  });

  // ---- control plane + dashboard ------------------------------------------

  app.use('/admin', adminRouter({ redis, limiter, policies, metrics, startedAt: Date.now() }));

  // Prometheus scrape endpoint, so this drops into an existing stack.
  const reader = new MetricsReader(redis);
  const metricsLimiter = limiter;
  app.get('/metrics', async (_req, res) => {
    const totals = await reader.totals();
    const l = metrics.latency.snapshot();
    const lines: string[] = [
      '# HELP ratelimit_requests_total Requests evaluated by the limiter.',
      '# TYPE ratelimit_requests_total counter',
    ];
    for (const [client, row] of Object.entries(totals.byClient)) {
      const c = escapeLabel(client);
      lines.push(`ratelimit_requests_total{client="${c}",verdict="allowed"} ${row.allowed}`);
      lines.push(`ratelimit_requests_total{client="${c}",verdict="throttled"} ${row.throttled}`);
    }
    const lease = metricsLimiter.leases.stats();
    if (lease.enabled) {
      lines.push(
        '# HELP ratelimit_lease_decisions_total Decisions taken on the leased path.',
        '# TYPE ratelimit_lease_decisions_total counter',
        `ratelimit_lease_decisions_total ${lease.decisions}`,
        '# HELP ratelimit_lease_acquires_total Redis round trips spent refilling leases.',
        '# TYPE ratelimit_lease_acquires_total counter',
        `ratelimit_lease_acquires_total ${lease.acquires}`,
        '# HELP ratelimit_lease_redis_ops_saved_total Round trips the lease layer removed.',
        '# TYPE ratelimit_lease_redis_ops_saved_total counter',
        `ratelimit_lease_redis_ops_saved_total ${lease.redisOpsSaved}`,
        '# HELP ratelimit_lease_tokens_returned_total Unspent leased tokens handed back.',
        '# TYPE ratelimit_lease_tokens_returned_total counter',
        `ratelimit_lease_tokens_returned_total ${lease.tokensReturned}`,
        '# HELP ratelimit_lease_entries Clients currently holding a lease on this instance.',
        '# TYPE ratelimit_lease_entries gauge',
        `ratelimit_lease_entries ${lease.entries}`,
      );
    }
    lines.push(
      '# HELP ratelimit_decision_latency_ms Middleware decision latency.',
      '# TYPE ratelimit_decision_latency_ms summary',
      `ratelimit_decision_latency_ms{quantile="0.5"} ${l.p50Ms}`,
      `ratelimit_decision_latency_ms{quantile="0.95"} ${l.p95Ms}`,
      `ratelimit_decision_latency_ms{quantile="0.99"} ${l.p99Ms}`,
      `ratelimit_decision_latency_ms_count ${l.count}`,
      '',
    );
    res.type('text/plain; version=0.0.4').send(lines.join('\n'));
  });

  app.use(express.static(join(here, '..', '..', 'public')));

  const server = app.listen(PORT, "0.0.0.0", () => {
    console.log(`flowgate listening on http://localhost:${PORT}`);
    console.log(`  dashboard  http://localhost:${PORT}/`);
    console.log(`  redis      ${REDIS_URL}`);
    console.log(`  leases     ${lease.enabled ? `on (ttl ${lease.ttlMs}ms)` : 'off'}`);
    if (!process.env.ADMIN_TOKEN) {
      console.warn('  WARNING: ADMIN_TOKEN unset — /admin is open. Do not run this way outside dev.');
    }
  });

  const shutdown = async (signal: string) => {
    console.log(`\n${signal} — draining`);
    server.close();
    await metrics.stop();       // flush buffered counts before exiting
    // Hand back every leased token. Without this, a rolling deploy strands one
    // lease per client per instance on each restart — invisible in a test, and
    // very visible as a quota dip while the fleet cycles.
    await Promise.all([limiter.close(), strictLimiter.close()]);
    await policies.close();
    await Promise.all([redis.quit(), subscriber.quit()]).catch(() => {});
    process.exit(0);
  };
  process.on('SIGINT', () => void shutdown('SIGINT'));
  process.on('SIGTERM', () => void shutdown('SIGTERM'));
}

const escapeLabel = (s: string) => s.replace(/\\/g, '\\\\').replace(/"/g, '\\"').replace(/\n/g, ' ');

main().catch((err) => {
  console.error('fatal:', err);
  process.exit(1);
});
