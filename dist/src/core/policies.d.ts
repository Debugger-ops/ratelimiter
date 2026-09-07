import type Redis from 'ioredis';
import { type Policy } from './types.js';
export declare const DEFAULT_POLICIES: Policy[];
export declare const DEFAULT_POLICY_NAME = "free";
/**
 * Policies live in Redis so every app instance agrees, and are cached in
 * process so the hot path never pays a round trip to read one. A pub/sub
 * message invalidates the cache on write; the TTL sweep is the backstop for a
 * dropped message (pub/sub is at-most-once).
 */
export declare class PolicyStore {
    private readonly redis;
    private cache;
    private assignments;
    private loadedAt;
    private readonly ttlMs;
    private subscriber;
    constructor(redis: Redis, opts?: {
        ttlMs?: number;
    });
    init(subscriber?: Redis): Promise<void>;
    refresh(): Promise<void>;
    private maybeRefresh;
    list(): Policy[];
    listAssignments(): Record<string, string>;
    get(name: string): Policy | undefined;
    /** Resolve which policy governs a client, falling back to the default tier. */
    resolve(clientId: string, override?: string): Policy;
    upsert(policy: Policy): Promise<Policy>;
    remove(name: string): Promise<boolean>;
    assign(clientId: string, policyName: string): Promise<void>;
    unassign(clientId: string): Promise<void>;
    close(): Promise<void>;
}
