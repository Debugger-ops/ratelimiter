import type { NextFunction, Request, Response } from 'express';
import type { RateLimiter } from '../core/limiter.js';
import type { PolicyStore } from '../core/policies.js';
import type { MetricsRecorder } from '../metrics/metrics.js';
import type { Decision } from '../core/types.js';
declare global {
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
export declare function defaultIdentify(req: Request): string;
export declare function rateLimit(deps: MiddlewareDeps, opts?: MiddlewareOptions): (req: Request, res: Response, next: NextFunction) => Promise<void | Response<any, Record<string, any>>>;
