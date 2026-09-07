import { Router } from 'express';
import { MetricsReader } from '../metrics/metrics.js';
import { validatePolicy } from '../core/types.js';
export function adminRouter(deps) {
    const router = Router();
    const reader = new MetricsReader(deps.redis);
    const adminToken = process.env.ADMIN_TOKEN;
    // The control plane mutates limits for every client on every instance.
    // It is not a public endpoint.
    router.use((req, res, next) => {
        // The live dashboard subscribes to /admin/stream via the browser
        // EventSource API, which cannot attach an Authorization header. The stream
        // is read-only telemetry — no client's limit can be changed through it — so
        // it is exempt from the token. Every mutating route below stays protected.
        if (req.method === 'GET' && req.path === '/stream')
            return next();
        if (!adminToken)
            return next(); // dev mode; server logs a warning at boot
        const auth = req.header('authorization');
        if (auth === `Bearer ${adminToken}`)
            return next();
        return res.status(401).json({ error: 'unauthorized' });
    });
    // ---- policies -----------------------------------------------------------
    router.get('/policies', (_req, res) => {
        res.json({ policies: deps.policies.list(), assignments: deps.policies.listAssignments() });
    });
    router.put('/policies/:name', async (req, res) => {
        const body = { ...req.body, name: req.params.name };
        try {
            validatePolicy(body);
            const saved = await deps.policies.upsert(body);
            res.json({ policy: saved });
        }
        catch (err) {
            res.status(400).json({ error: 'invalid_policy', message: msg(err) });
        }
    });
    router.delete('/policies/:name', async (req, res) => {
        try {
            const removed = await deps.policies.remove(req.params.name);
            res.json({ removed });
        }
        catch (err) {
            res.status(400).json({ error: 'cannot_delete', message: msg(err) });
        }
    });
    // ---- client -> tier assignment ------------------------------------------
    router.put('/assignments/:clientId', async (req, res) => {
        const policy = req.body?.policy;
        if (!policy)
            return res.status(400).json({ error: 'policy is required' });
        try {
            await deps.policies.assign(req.params.clientId, policy);
            res.json({ clientId: req.params.clientId, policy });
        }
        catch (err) {
            res.status(400).json({ error: 'invalid_assignment', message: msg(err) });
        }
    });
    router.delete('/assignments/:clientId', async (req, res) => {
        await deps.policies.unassign(req.params.clientId);
        res.json({ ok: true });
    });
    // ---- operations ---------------------------------------------------------
    router.post('/clients/:clientId/reset', async (req, res) => {
        const deleted = await deps.limiter.reset(req.params.clientId);
        res.json({ clientId: req.params.clientId, keysDeleted: deleted });
    });
    router.post('/metrics/reset', async (_req, res) => {
        await reader.resetAll();
        deps.metrics.latency.reset();
        deps.metrics.redisLatency.reset();
        res.json({ ok: true });
    });
    // ---- read models --------------------------------------------------------
    router.get('/stats', async (_req, res) => {
        res.json(await snapshot(deps, reader, 60));
    });
    router.get('/series', async (req, res) => {
        const seconds = clamp(Number(req.query.seconds ?? 60), 5, 300);
        res.json({ series: await reader.series(seconds) });
    });
    /**
     * SSE rather than polling: the dashboard wants a steady 1 Hz push and SSE
     * gives that over plain HTTP with automatic browser reconnect. WebSockets
     * would add a protocol upgrade and a second server for one-way data.
     */
    router.get('/stream', async (req, res) => {
        res.set({
            'Content-Type': 'text/event-stream',
            'Cache-Control': 'no-cache, no-transform',
            Connection: 'keep-alive',
            'X-Accel-Buffering': 'no',
        });
        res.flushHeaders?.();
        let closed = false;
        req.on('close', () => {
            closed = true;
            clearInterval(timer);
        });
        const push = async () => {
            if (closed)
                return;
            try {
                const payload = await snapshot(deps, reader, 60);
                res.write(`event: tick\ndata: ${JSON.stringify(payload)}\n\n`);
            }
            catch (err) {
                res.write(`event: error\ndata: ${JSON.stringify({ message: msg(err) })}\n\n`);
            }
        };
        const timer = setInterval(() => void push(), 1000);
        await push();
    });
    return router;
}
async function snapshot(deps, reader, seconds) {
    const [series, totals] = await Promise.all([reader.series(seconds), reader.totals()]);
    const decided = totals.allowed + totals.throttled;
    const info = await deps.redis.info('memory').catch(() => '');
    const usedMemory = /used_memory_human:(\S+)/.exec(info)?.[1] ?? 'n/a';
    return {
        now: Date.now(),
        uptimeSec: Math.round((Date.now() - deps.startedAt) / 1000),
        totals: {
            ...totals,
            decided,
            throttleRate: decided ? totals.throttled / decided : 0,
        },
        series,
        policies: deps.policies.list(),
        assignments: deps.policies.listAssignments(),
        latency: {
            endToEnd: deps.metrics.latency.snapshot(),
            redis: deps.metrics.redisLatency.snapshot(),
        },
        // How much of the traffic never reached Redis at all. `hitRate` is the
        // fraction of leased decisions that cost no round trip; `races` staying at
        // zero is the sign the lease size is keeping up with the concurrency.
        leases: deps.limiter.leases.stats(),
        health: {
            redisStatus: deps.redis.status,
            redisMemory: usedMemory,
            redisErrors: deps.limiter.redisErrors,
            lastRedisError: deps.limiter.lastRedisError,
            metricFlushes: deps.metrics.flushes,
            metricFlushErrors: deps.metrics.flushErrors,
        },
    };
}
const clamp = (n, lo, hi) => Number.isFinite(n) ? Math.min(hi, Math.max(lo, n)) : lo;
const msg = (e) => (e instanceof Error ? e.message : String(e));
//# sourceMappingURL=admin.js.map