"use strict";
/**
 * Demo traffic generator — drives the dashboard with a mix that actually looks
 * like production: mostly polite clients, one deliberate abuser, and a batch
 * job that bursts.
 *
 *   npm run traffic            # 60s against http://localhost:8080
 *   DURATION=120 npm run traffic
 */
const BASE = process.env.BASE_URL ?? 'http://localhost:8080';
const DURATION_MS = Number(process.env.DURATION ?? 60) * 1000;
const SIMS = [
    { key: 'acme-prod', tier: 'pro', rps: 6 },
    { key: 'acme-staging', tier: 'free', rps: 0.8 },
    { key: 'globex', tier: 'enterprise', rps: 20 },
    { key: 'initech-scraper', tier: 'free', rps: 14 }, // well over a 60/min limit
    { key: 'nightly-batch', tier: 'pro', rps: 0.4, burst: { every: 12, size: 40 } },
    { key: 'brute-forcer', tier: 'free', rps: 2, path: '/api/login' },
];
const counts = new Map();
function tally(key, status) {
    const row = counts.get(key) ?? { ok: 0, limited: 0, err: 0 };
    if (status === 200)
        row.ok++;
    else if (status === 429)
        row.limited++;
    else
        row.err++;
    counts.set(key, row);
}
async function hit(sim) {
    const path = sim.path ?? '/api/echo';
    try {
        const res = await fetch(BASE + path, {
            method: path === '/api/login' ? 'POST' : 'GET',
            headers: { 'x-api-key': sim.key, 'content-type': 'application/json' },
            body: path === '/api/login' ? '{}' : undefined,
        });
        tally(sim.key, res.status);
    }
    catch {
        tally(sim.key, 0);
    }
}
async function assignTiers() {
    for (const sim of SIMS) {
        await fetch(`${BASE}/admin/assignments/${encodeURIComponent(`key:${sim.key}`)}`, {
            method: 'PUT',
            headers: {
                'content-type': 'application/json',
                ...(process.env.ADMIN_TOKEN ? { authorization: `Bearer ${process.env.ADMIN_TOKEN}` } : {}),
            },
            body: JSON.stringify({ policy: sim.tier }),
        }).catch(() => { });
    }
}
async function main() {
    const health = await fetch(`${BASE}/health`).catch(() => null);
    if (!health?.ok) {
        console.error(`Cannot reach ${BASE}. Start the server first: npm start`);
        process.exit(1);
    }
    await assignTiers();
    console.log(`Driving ${SIMS.length} simulated clients at ${BASE} for ${DURATION_MS / 1000}s`);
    console.log(`Watch ${BASE}/\n`);
    const started = Date.now();
    const timers = [];
    for (const sim of SIMS) {
        timers.push(setInterval(() => void hit(sim), Math.max(10, 1000 / sim.rps)));
        if (sim.burst) {
            timers.push(setInterval(() => {
                for (let i = 0; i < sim.burst.size; i++)
                    void hit(sim);
            }, sim.burst.every * 1000));
        }
    }
    const report = setInterval(() => {
        const elapsed = Math.round((Date.now() - started) / 1000);
        const rows = [...counts.entries()]
            .map(([k, v]) => {
            const total = v.ok + v.limited;
            const pct = total ? ((v.limited / total) * 100).toFixed(0) : '0';
            return `${k} ${v.ok}/${total} (${pct}% limited)`;
        })
            .join('  |  ');
        process.stdout.write(`\r[${elapsed}s] ${rows}`.slice(0, 200));
    }, 1000);
    setTimeout(() => {
        for (const t of timers)
            clearInterval(t);
        clearInterval(report);
        console.log('\n\nDone.');
        for (const [k, v] of counts) {
            const total = v.ok + v.limited;
            console.log(`  ${k.padEnd(18)} allowed ${String(v.ok).padStart(5)}  throttled ${String(v.limited).padStart(5)}  ` +
                `(${total ? ((v.limited / total) * 100).toFixed(1) : '0.0'}%)${v.err ? `  errors ${v.err}` : ''}`);
        }
        process.exit(0);
    }, DURATION_MS);
}
void main();
//# sourceMappingURL=traffic.js.map