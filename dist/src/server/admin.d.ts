import { Router } from 'express';
import type { RateLimiter } from '../core/limiter.js';
import type { PolicyStore } from '../core/policies.js';
import { type MetricsRecorder } from '../metrics/metrics.js';
import type { LimiterRedis } from '../core/redis.js';
export interface AdminDeps {
    redis: LimiterRedis;
    limiter: RateLimiter;
    policies: PolicyStore;
    metrics: MetricsRecorder;
    startedAt: number;
}
export declare function adminRouter(deps: AdminDeps): Router;
