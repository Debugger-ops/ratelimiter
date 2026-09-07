import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { fixture, id } from './helpers.js';
const f = fixture();
// 60 tokens/min sustained (1/sec) with a bucket that holds 10.
const policy = {
    name: 'tb-test',
    algorithm: 'token_bucket',
    limit: 60,
    windowMs: 60_000,
    burst: 10,
};
beforeAll(async () => {
    await f.ready();
    await f.redis.ping();
});
afterAll(async () => {
    await f.close();
});
describe('token bucket', () => {
    it('spends the full burst, then refuses', async () => {
        const c = id();
        for (let i = 0; i < 10; i++) {
            const d = await f.limiter.check(c, policy);
            expect(d.allowed, `request ${i + 1} of the burst`).toBe(true);
            expect(d.remaining).toBe(9 - i);
        }
        const denied = await f.limiter.check(c, policy);
        expect(denied.allowed).toBe(false);
        expect(denied.remaining).toBe(0);
    });
    it('refills continuously, not in steps', async () => {
        const c = id();
        for (let i = 0; i < 10; i++)
            await f.limiter.check(c, policy);
        expect((await f.limiter.check(c, policy)).allowed).toBe(false);
        // Rate is 1 token/sec. Half a second buys nothing; a full second buys one.
        f.advance(500);
        expect((await f.limiter.check(c, policy)).allowed).toBe(false);
        f.advance(500);
        const d = await f.limiter.check(c, policy);
        expect(d.allowed).toBe(true);
        // and only one — the next is immediately refused again
        expect((await f.limiter.check(c, policy)).allowed).toBe(false);
    });
    it('never refills past capacity, however long the client idles', async () => {
        const c = id();
        await f.limiter.check(c, policy);
        f.advance(24 * 60 * 60 * 1000); // a day of silence
        let allowed = 0;
        for (let i = 0; i < 50; i++)
            if ((await f.limiter.check(c, policy)).allowed)
                allowed++;
        expect(allowed).toBe(10); // capacity, not a day's worth of tokens
    });
    it('reports a retry-after that is actually long enough', async () => {
        const c = id();
        for (let i = 0; i < 10; i++)
            await f.limiter.check(c, policy);
        const denied = await f.limiter.check(c, policy);
        expect(denied.allowed).toBe(false);
        expect(denied.retryAfterMs).toBeGreaterThan(0);
        // One ms short of the advice must still fail; the advised wait must work.
        f.advance(denied.retryAfterMs - 1);
        expect((await f.limiter.check(c, policy)).allowed).toBe(false);
        f.advance(1);
        expect((await f.limiter.check(c, policy)).allowed).toBe(true);
    });
    it('charges a multi-token cost as one atomic decision', async () => {
        const c = id();
        const d1 = await f.limiter.check(c, policy, 7);
        expect(d1.allowed).toBe(true);
        expect(d1.remaining).toBe(3);
        const d2 = await f.limiter.check(c, policy, 5); // only 3 left
        expect(d2.allowed).toBe(false);
        expect(d2.remaining).toBe(3); // a refused request spends nothing
        const d3 = await f.limiter.check(c, policy, 3);
        expect(d3.allowed).toBe(true);
        expect(d3.remaining).toBe(0);
    });
    it('peek reads the bucket without spending it', async () => {
        const c = id();
        await f.limiter.check(c, policy, 4);
        const a = await f.limiter.peek(c, policy);
        const b = await f.limiter.peek(c, policy);
        expect(a.remaining).toBe(6);
        expect(b.remaining).toBe(6);
    });
    it('keeps clients independent', async () => {
        const a = id();
        const b = id();
        for (let i = 0; i < 10; i++)
            await f.limiter.check(a, policy);
        expect((await f.limiter.check(a, policy)).allowed).toBe(false);
        expect((await f.limiter.check(b, policy)).allowed).toBe(true);
    });
    it('expires idle state so memory is bounded by active clients, not total clients', async () => {
        const c = id();
        await f.limiter.check(c, policy);
        const keys = await f.redis.keys(`*{${c}}*`);
        expect(keys).toHaveLength(1);
        const ttl = await f.redis.pttl(keys[0]);
        // Bucket is 9/10 full at 1 token/sec => ~1s to refill, plus the 1s slack.
        expect(ttl).toBeGreaterThan(0);
        expect(ttl).toBeLessThanOrEqual(2100);
    });
});
//# sourceMappingURL=token-bucket.test.js.map