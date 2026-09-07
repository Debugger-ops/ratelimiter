import type { LeaseOptions } from './lease.js';
export declare const ALGORITHMS: readonly ["token_bucket", "sliding_window_log", "sliding_window_counter"];
export type Algorithm = (typeof ALGORITHMS)[number];
export interface Policy {
    /** Stable name, e.g. "free", "pro", "login-endpoint". */
    name: string;
    algorithm: Algorithm;
    /** Requests permitted per `windowMs`. For token_bucket this sets the refill rate. */
    limit: number;
    windowMs: number;
    /**
     * token_bucket only: bucket capacity, i.e. how large a burst may be spent at
     * once. Defaults to `limit`. Setting burst > limit is the whole reason to
     * choose a token bucket: 100/min sustained, 300 in one go after idling.
     */
    burst?: number;
    /** Description surfaced in the dashboard. */
    note?: string;
}
export interface Decision {
    allowed: boolean;
    /** Requests left in the current window/bucket. */
    remaining: number;
    /**
     * What `remaining` counts against. For a window algorithm this is the
     * window quota; for a token bucket it is the bucket capacity, which is the
     * only number `remaining` can honestly be measured against.
     */
    limit: number;
    /** The policy's sustained quota per `windowMs`, for RateLimit-Policy. */
    quota: number;
    windowMs: number;
    /** ms until this request would succeed. 0 when allowed. */
    retryAfterMs: number;
    /** ms until the limiter state fully resets for this client. */
    resetAfterMs: number;
    policy: string;
    algorithm: Algorithm;
    clientId: string;
    /** True when Redis failed and the configured failure mode decided the outcome. */
    degraded: boolean;
    /**
     * Where the decision came from. 'lease' means it was served out of tokens
     * this instance had already withdrawn from Redis, without a round trip —
     * see core/lease.ts. Surfaced so the dashboard, the tests and an operator
     * can all tell the two paths apart.
     */
    source: DecisionSource;
    /** Time spent inside the limiter, in ms. */
    latencyMs: number;
}
export type FailureMode = 'open' | 'closed';
export type DecisionSource = 'redis' | 'lease';
export interface LimiterOptions {
    keyPrefix?: string;
    /** What to do when Redis is unreachable. 'open' allows traffic through. */
    failureMode?: FailureMode;
    /**
     * Pass -1 to have Redis supply the clock (default, and correct for a fleet
     * of app servers). Tests inject a fixed timestamp instead.
     */
    now?: () => number;
    /**
     * Local token leases: withdraw a block of quota from Redis and spend it in
     * process, so most decisions cost no round trip at all. Off by default —
     * it changes the latency profile, not the limit, and a limiter should not
     * change its behaviour without being asked. See core/lease.ts.
     */
    lease?: LeaseOptions;
}
export declare function normalizePolicy(p: Policy): Required<Omit<Policy, 'note'>> & {
    note: string;
};
export declare function validatePolicy(p: Partial<Policy>): asserts p is Policy;
