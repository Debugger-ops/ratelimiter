export const ALGORITHMS = [
    'token_bucket',
    'sliding_window_log',
    'sliding_window_counter',
];
export function normalizePolicy(p) {
    return {
        name: p.name,
        algorithm: p.algorithm,
        limit: p.limit,
        windowMs: p.windowMs,
        burst: p.burst ?? p.limit,
        note: p.note ?? '',
    };
}
export function validatePolicy(p) {
    if (!p.name || typeof p.name !== 'string')
        throw new Error('policy.name is required');
    if (!/^[a-zA-Z0-9_.:-]{1,64}$/.test(p.name)) {
        throw new Error('policy.name must match /^[a-zA-Z0-9_.:-]{1,64}$/');
    }
    if (!ALGORITHMS.includes(p.algorithm)) {
        throw new Error(`policy.algorithm must be one of ${ALGORITHMS.join(', ')}`);
    }
    if (!Number.isFinite(p.limit) || p.limit <= 0) {
        throw new Error('policy.limit must be a positive number');
    }
    if (!Number.isFinite(p.windowMs) || p.windowMs <= 0) {
        throw new Error('policy.windowMs must be a positive number');
    }
    if (p.burst !== undefined && (!Number.isFinite(p.burst) || p.burst <= 0)) {
        throw new Error('policy.burst must be a positive number');
    }
    if (p.algorithm === 'sliding_window_log' && p.limit > 100_000) {
        // One sorted-set member per request; this would be ~100k members per client.
        throw new Error('sliding_window_log limit above 100000 would be a memory hazard');
    }
}
//# sourceMappingURL=types.js.map