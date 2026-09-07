import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import express from 'express';
import { createRedis, whenReady } from '../src/core/redis.js';
import { RateLimiter } from '../src/core/limiter.js';
import { PolicyStore } from '../src/core/policies.js';
import { MetricsRecorder, MetricsReader } from '../src/metrics/metrics.js';
import { rateLimit } from '../src/server/middleware.js';
import { REDIS_URL, id } from './helpers.js';
const redis = createRedis(REDIS_URL);
const policies = new PolicyStore(redis);
const metrics = new MetricsRecorder(redis, 50);
const limiter = new RateLimiter(redis, { keyPrefix: `http-${Date.now()}` });
let server;
let base;
beforeAll(async () => {
    await whenReady(redis);
    await policies.init();
    await policies.upsert({
        name: 'http-test',
        algorithm: 'token_bucket',
        limit: 3,
        windowMs: 60_000,
        burst: 3,
    });
    metrics.start();
    const app = express();
    app.use(express.json());
    const deps = { limiter, policies, metrics };
    app.get('/open', rateLimit(deps, { policy: 'http-test' }), (_req, res) => res.json({ ok: true }));
    app.get('/skipped', rateLimit(deps, { policy: 'http-test', skip: () => true }), (_req, res) => res.json({ ok: true }));
    app.get('/pricey', rateLimit(deps, { policy: 'http-test', cost: 3 }), (_req, res) => res.json({ ok: true }));
    await new Promise((resolve) => {
        server = app.listen(0, () => resolve());
    });
    const addr = server.address();
    base = `http://127.0.0.1:${typeof addr === 'object' && addr ? addr.port : 0}`;
});
afterAll(async () => {
    await metrics.stop();
    await policies.remove('http-test').catch(() => { });
    server?.close();
    await redis.quit().catch(() => { });
});
const get = (path, key) => fetch(`${base}${path}`, { headers: { 'x-api-key': key } });
describe('express middleware', () => {
    it('serves the quota, then answers 429 with a Retry-After', async () => {
        const key = id();
        for (let i = 0; i < 3; i++) {
            const res = await get('/open', key);
            expect(res.status).toBe(200);
            expect(res.headers.get('ratelimit-limit')).toBe('3');
            expect(res.headers.get('ratelimit-remaining')).toBe(String(2 - i));
        }
        const blocked = await get('/open', key);
        expect(blocked.status).toBe(429);
        expect(blocked.headers.get('ratelimit-remaining')).toBe('0');
        expect(Number(blocked.headers.get('retry-after'))).toBeGreaterThan(0);
        const body = (await blocked.json());
        expect(body.error).toBe('rate_limit_exceeded');
        expect(body.algorithm).toBe('token_bucket');
        expect(body.policy).toBe('http-test');
    });
    it('emits both the IETF and the legacy header families', async () => {
        const res = await get('/open', id());
        expect(res.headers.get('ratelimit')).toMatch(/limit=3, remaining=2, reset=\d+/);
        expect(res.headers.get('ratelimit-policy')).toContain('policy="http-test"');
        expect(res.headers.get('x-ratelimit-algorithm')).toBe('token_bucket');
        expect(res.headers.get('x-ratelimit-remaining')).toBe('2');
    });
    it('charges an expensive endpoint its real cost', async () => {
        const key = id();
        expect((await get('/pricey', key)).status).toBe(200); // spends all 3
        expect((await get('/open', key)).status).toBe(429);
    });
    it('separates clients by API key', async () => {
        const a = id();
        const b = id();
        for (let i = 0; i < 3; i++)
            await get('/open', a);
        expect((await get('/open', a)).status).toBe(429);
        expect((await get('/open', b)).status).toBe(200);
    });
    it('lets skipped routes through without touching Redis', async () => {
        const key = id();
        for (let i = 0; i < 10; i++)
            expect((await get('/skipped', key)).status).toBe(200);
        expect((await get('/open', key)).status).toBe(200); // quota untouched
    });
    it('records both verdicts in the metrics series', async () => {
        const key = id();
        for (let i = 0; i < 5; i++)
            await get('/open', key); // 3 allowed, 2 throttled
        await metrics.flush();
        const reader = new MetricsReader(redis);
        const totals = await reader.totals();
        const row = totals.byClient[`key:${key}`];
        expect(row).toBeDefined();
        expect(row.allowed).toBe(3);
        expect(row.throttled).toBe(2);
        const series = await reader.series(10);
        const summed = series.reduce((n, b) => n + b.allowed + b.throttled, 0);
        expect(summed).toBeGreaterThanOrEqual(5);
    });
    it('resetting a client clears its limiter state', async () => {
        const key = id();
        for (let i = 0; i < 3; i++)
            await get('/open', key);
        expect((await get('/open', key)).status).toBe(429);
        const deleted = await limiter.reset(`key:${key}`);
        expect(deleted).toBeGreaterThan(0);
        expect((await get('/open', key)).status).toBe(200);
    });
});
//# sourceMappingURL=http.test.js.map