/**
 * Demo traffic generator — drives the dashboard with a mix that actually looks
 * like production: mostly polite clients, one deliberate abuser, and a batch
 * job that bursts.
 *
 *   npm run traffic            # 60s against http://localhost:8080
 *   DURATION=120 npm run traffic
 */
declare const BASE: string;
declare const DURATION_MS: number;
interface Sim {
    key: string;
    tier: string;
    /** Requests per second this client attempts. */
    rps: number;
    /** Burst every N seconds, if any. */
    burst?: {
        every: number;
        size: number;
    };
    path?: string;
}
declare const SIMS: Sim[];
declare const counts: Map<string, {
    ok: number;
    limited: number;
    err: number;
}>;
declare function tally(key: string, status: number): void;
declare function hit(sim: Sim): Promise<void>;
declare function assignTiers(): Promise<void>;
declare function main(): Promise<void>;
