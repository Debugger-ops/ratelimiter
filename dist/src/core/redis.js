import Redis from 'ioredis';
import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
const here = dirname(fileURLToPath(import.meta.url));
const luaDir = join(here, '..', 'lua');
const lua = (name) => readFileSync(join(luaDir, `${name}.lua`), 'utf8');
export function createRedis(url = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379', opts = {}) {
    const client = new Redis(url, {
        // Managed Redis on Railway (and Fly, and Render's private network) is
        // reachable only over IPv6. ioredis inherits Node's default of an IPv4-only
        // lookup, so a host like `redis.railway.internal` resolves to nothing and
        // the client sits in `connecting` until whenReady() gives up — which reads
        // as "Redis is down" when in fact it was never dialled. family 0 asks the
        // resolver for both families and takes whichever answers. A literal address
        // such as 127.0.0.1 skips DNS entirely, so local dev is unchanged.
        family: Number(process.env.REDIS_FAMILY ?? 0),
        // A rate limiter must never become the reason a request hangs. During a
        // reconnect, reject immediately so the configured failure mode decides in
        // microseconds, instead of queueing commands behind a connection that may
        // not come back. The cost is that callers must wait for `ready` at boot —
        // see whenReady() below.
        maxRetriesPerRequest: 2,
        enableOfflineQueue: false,
        connectTimeout: 2000,
        lazyConnect: false,
        ...opts,
    });
    client.defineCommand('tokenBucket', { numberOfKeys: 1, lua: lua('token_bucket') });
    client.defineCommand('slidingWindowLog', { numberOfKeys: 1, lua: lua('sliding_window_log') });
    client.defineCommand('slidingWindowCounter', { numberOfKeys: 1, lua: lua('sliding_window_counter') });
    client.defineCommand('leaseAcquire', { numberOfKeys: 1, lua: lua('lease_acquire') });
    client.defineCommand('leaseRelease', { numberOfKeys: 1, lua: lua('lease_release') });
    return client;
}
/**
 * Resolve once the connection can accept commands. Because the offline queue
 * is disabled, a command issued in the same tick as the constructor would be
 * rejected outright — boot has to wait for this, the request path never does.
 */
export function whenReady(client, timeoutMs = 10_000) {
    if (client.status === 'ready')
        return Promise.resolve();
    return new Promise((resolve, reject) => {
        const timer = setTimeout(() => {
            cleanup();
            reject(new Error(`Redis not ready after ${timeoutMs}ms (status: ${client.status})`));
        }, timeoutMs);
        const onReady = () => {
            cleanup();
            resolve();
        };
        const onEnd = () => {
            cleanup();
            reject(new Error('Redis connection ended before becoming ready'));
        };
        function cleanup() {
            clearTimeout(timer);
            client.off('ready', onReady);
            client.off('end', onEnd);
        }
        client.once('ready', onReady);
        client.once('end', onEnd);
    });
}
//# sourceMappingURL=redis.js.map