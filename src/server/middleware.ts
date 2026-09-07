import type { NextFunction, Request, Response } from 'express';
import type { RateLimiter } from '../core/limiter.js';
import type { PolicyStore } from '../core/policies.js';
import type { MetricsRecorder } from '../metrics/metrics.js';
import type { Decision } from '../core/types.js';

declare global {
  // eslint-disable-next-line @typescript-eslint/no-namespace
  namespace Express {
    interface Request {
      rateLimit?: Decision;
    }
  }
}

export interface MiddlewareOptions {
  /** How to identify the caller. Default: API key header, then bearer token, then IP. */
  identify?: (req: Request) => string;
  /** Force a specific policy for this mount point, ignoring the client's tier. */
  policy?: string;
  /** Token cost of a request — expensive endpoints can charge more than 1. */
  cost?: number | ((req: Request) => number);
  /** Skip limiting entirely (health checks, internal traffic). */
  skip?: (req: Request) => boolean;
  /** Called when a request is rejected, before the response is written. */
  onThrottle?: (req: Request, decision: Decision) => void;
}

export interface MiddlewareDeps {
  limiter: RateLimiter;
  policies: PolicyStore;
  metrics: MetricsRecorder;
}

export function defaultIdentify(req: Request): string {
  const apiKey = req.header('x-api-key');
  if (apiKey) return `key:${apiKey}`;

  const auth = req.header('authorization');
  if (auth?.toLowerCase().startsWith('bearer ')) {
    // Never key on the raw credential — a limiter key ends up in logs, in
    // SCAN output, and on a dashboard. Prefix + length is enough to be unique
    // in practice for a demo; production would key on the resolved account id.
    const token = auth.slice(7);
    return `token:${token.slice(0, 8)}…${token.length}`;
  }

  // req.ip honours `trust proxy`. Behind a load balancer, X-Forwarded-For is
  // client-controlled unless the proxy count is configured — getting this
  // wrong lets an attacker mint a fresh identity per request.
  return `ip:${req.ip ?? 'unknown'}`;
}

export function rateLimit(deps: MiddlewareDeps, opts: MiddlewareOptions = {}) {
  const identify = opts.identify ?? defaultIdentify;

  return async function rateLimitMiddleware(req: Request, res: Response, next: NextFunction) {
    if (opts.skip?.(req)) return next();

    const started = performance.now();
    const clientId = identify(req);
    const policy = deps.policies.resolve(clientId, opts.policy);
    const cost = typeof opts.cost === 'function' ? opts.cost(req) : (opts.cost ?? 1);

    const decision = await deps.limiter.check(clientId, policy, cost);
    req.rateLimit = decision;

    applyHeaders(res, decision);
    deps.metrics.record(decision, performance.now() - started);

    if (decision.allowed) return next();

    opts.onThrottle?.(req, decision);
    const retryAfterSec = Math.max(1, Math.ceil(decision.retryAfterMs / 1000));
    res.setHeader('Retry-After', retryAfterSec);
    return res.status(429).json({
      error: 'rate_limit_exceeded',
      message: `Rate limit exceeded for policy "${decision.policy}". Retry in ${retryAfterSec}s.`,
      policy: decision.policy,
      algorithm: decision.algorithm,
      limit: decision.limit,
      retryAfterMs: decision.retryAfterMs,
    });
  };
}

function applyHeaders(res: Response, d: Decision): void {
  const resetSec = Math.ceil(d.resetAfterMs / 1000);

  // IETF draft-ietf-httpapi-ratelimit-headers, both the field-per-value form
  // (widely deployed) and the structured-field form (current draft).
  res.setHeader('RateLimit-Limit', d.limit);
  res.setHeader('RateLimit-Remaining', Math.max(0, d.remaining));
  res.setHeader('RateLimit-Reset', resetSec);
  res.setHeader('RateLimit', `limit=${d.limit}, remaining=${Math.max(0, d.remaining)}, reset=${resetSec}`);
  // The policy header advertises the *sustained* quota and its window, which
  // for a token bucket is a different number from RateLimit-Limit: the client
  // may spend `limit` tokens at once but only earns `quota` per window.
  res.setHeader(
    'RateLimit-Policy',
    `${d.quota};w=${Math.round(d.windowMs / 1000)};burst=${d.limit};policy="${d.policy}"`,
  );

  // Legacy X- headers: still what most SDKs and dashboards actually read.
  res.setHeader('X-RateLimit-Limit', d.limit);
  res.setHeader('X-RateLimit-Remaining', Math.max(0, d.remaining));
  res.setHeader('X-RateLimit-Algorithm', d.algorithm);
  // Which path answered: 'redis' for a round trip, 'lease' for a decision
  // served out of tokens this instance had already withdrawn. Exposed because
  // it explains an otherwise puzzling RateLimit-Remaining — a leased reply
  // reports what this instance still holds, which is a lower bound on the
  // client's real quota rather than the whole of it.
  res.setHeader('X-RateLimit-Source', d.source);

  if (d.degraded) res.setHeader('X-RateLimit-Degraded', '1');
}
