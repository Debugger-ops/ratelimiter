import type { LeaseOptions } from './lease.js';

export const ALGORITHMS = [
  'token_bucket',
  'sliding_window_log',
  'sliding_window_counter',
] as const;

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

export function normalizePolicy(p: Policy): Required<Omit<Policy, 'note'>> & { note: string } {
  return {
    name: p.name,
    algorithm: p.algorithm,
    limit: p.limit,
    windowMs: p.windowMs,
    burst: p.burst ?? p.limit,
    note: p.note ?? '',
  };
}

export function validatePolicy(p: Partial<Policy>): asserts p is Policy {
  if (!p.name || typeof p.name !== 'string') throw new Error('policy.name is required');
  if (!/^[a-zA-Z0-9_.:-]{1,64}$/.test(p.name)) {
    throw new Error('policy.name must match /^[a-zA-Z0-9_.:-]{1,64}$/');
  }
  if (!ALGORITHMS.includes(p.algorithm as Algorithm)) {
    throw new Error(`policy.algorithm must be one of ${ALGORITHMS.join(', ')}`);
  }
  if (!Number.isFinite(p.limit) || (p.limit as number) <= 0) {
    throw new Error('policy.limit must be a positive number');
  }
  if (!Number.isFinite(p.windowMs) || (p.windowMs as number) <= 0) {
    throw new Error('policy.windowMs must be a positive number');
  }
  if (p.burst !== undefined && (!Number.isFinite(p.burst) || p.burst <= 0)) {
    throw new Error('policy.burst must be a positive number');
  }
  if (p.algorithm === 'sliding_window_log' && (p.limit as number) > 100_000) {
    // One sorted-set member per request; this would be ~100k members per client.
    throw new Error('sliding_window_log limit above 100000 would be a memory hazard');
  }
}
