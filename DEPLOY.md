# Deploying FlowGate

FlowGate is a long-lived stateful process, not a collection of request handlers.
That is a deliberate design choice — see [local token leases](README.md#cutting-the-round-trip-local-token-leases)
— but it decides where the service can run, so it is worth being explicit about.

Five things in this service outlive a single request:

| What | Where it lives | Why it needs a process |
|---|---|---|
| `/admin/stream` | `src/server/admin.ts` | One SSE response held open, written to at 1 Hz |
| Metrics buffer | `src/metrics/metrics.ts` | Counts accumulate in a `Map` and flush on a timer |
| Latency histograms | `src/metrics/histogram.ts` | The p99 tiles are computed from in-process samples |
| Policy cache | `src/core/policies.ts` | A Redis pub/sub subscriber invalidates it on change |
| Token leases | `src/core/lease.ts` | Withdrawn quota is spent in memory, returned on shutdown |

Any host that gives the app a container and keeps it running is fine: Railway,
Render, Fly.io, Google Cloud Run with minimum instances, ECS, a VM, or the
`docker-compose.yml` in this repo.

**Serverless platforms — Vercel, Netlify, Cloudflare Workers — are not.** Not
because they are worse, but because every one of the five rows above assumes
memory that survives between requests, and a serverless function has none. A
deploy there does not fail loudly; it succeeds at serving `public/index.html`
as a static page while every API route 404s, and the dashboard sits on
"reconnecting…" with zeros in every tile.

---

## Railway

`railway.json` is already in the repo: NIXPACKS builder, `npm run start`,
health check on `/health` with a 120 s grace period.

### 1. Create the project

```bash
npm i -g @railway/cli
railway login
railway init                       # or: link an existing project
```

Or point Railway at the GitHub repo from the dashboard and let it deploy on push.

### 2. Add Redis

In the project, **New → Database → Add Redis**. Railway creates the service and
exposes `REDIS_URL` as a shared variable.

### 3. Set the app's variables

On the **app** service (not the Redis one):

| Variable | Value | Why |
|---|---|---|
| `REDIS_URL` | `${{Redis.REDIS_URL}}` | Reference the Redis service; do not paste the literal string |
| `ADMIN_TOKEN` | a long random string | `/admin` mutates every client's limits. Unset means open |
| `TRUST_PROXY_HOPS` | `1` | **Required.** See below |
| `LEASE_ENABLED` | `0`, or `1` to demo leases | Off by default |

Generate a token with `openssl rand -hex 32`.

`PORT` is injected by Railway and read by `src/server/index.ts` — do not set it.

**`TRUST_PROXY_HOPS=1` is not optional here.** Railway terminates TLS at an
edge proxy, so with the default of `0`, `req.ip` is the proxy's address and
*every* caller collapses into one client. The limiter would still work, it would
just be rate limiting the whole internet as a single identity, and the Clients
table would show one row. One hop is the right count for Railway's single proxy;
setting it higher would let a caller forge `X-Forwarded-For` and mint a fresh
identity per request, which defeats IP-based limiting entirely.

### 4. Deploy and check

```bash
railway up                         # or push to the linked branch
railway domain                     # generates a public URL
```

```bash
curl -s https://<your-app>.up.railway.app/health
# {"status":"ok","redis":"ready"}
```

`"redis":"ready"` is the one to read. Anything else means the app is up but the
limiter is not — check the connection notes below.

Open the URL in a browser. The status pill should go to **live** within a
second or two.

### 5. Put traffic through it

`bench/traffic.ts` takes a base URL, so you can drive the deployed dashboard
from your laptop:

```bash
BASE_URL=https://<your-app>.up.railway.app \
ADMIN_TOKEN=<the same token> \
DURATION=120 \
npm run traffic
```

Without `ADMIN_TOKEN` the tier assignments are rejected with a 401 and every
simulated client falls back to the default policy — the traffic still flows and
the dashboard still fills, but the per-tier differences are the interesting part.

---

## Connection notes

**`redis` stuck on `connecting`, or boot fails with "Redis not ready after
10000ms".** Railway's private network is IPv6-only, and ioredis inherits Node's
IPv4-only DNS lookup, so `redis.railway.internal` resolves to nothing. This is
handled in `src/core/redis.ts` by passing `family: 0`, which asks the resolver
for both families. Override with `REDIS_FAMILY=4` if you ever point the service
at an IPv4-only Redis whose DNS also answers AAAA.

**The dashboard says "no API at this URL — static deploy?"** The page is being
served by something that isn't this app — a static host, or a CDN in front of a
service that is down. Nothing is wrong with the client.

**The dashboard says "server up — stream blocked".** `/health` answered but the
SSE stream did not, which means a proxy is buffering `text/event-stream`. The
app already sends `X-Accel-Buffering: no` and `Cache-Control: no-transform`;
if you have added a CDN, exempt `/admin/stream` from it.

**Cold Redis, empty dashboard.** Per-second metrics carry a 15-minute TTL
(`RETENTION_SEC` in `src/metrics/metrics.ts`). A dashboard opened on an idle
service shows zeros because there is genuinely nothing in the window — this is
correct, not a fault. Run the traffic generator.

---

## Other hosts

**Render** — Web Service, build `npm install`, start `npm run start`, health
check `/health`, plus a Key Value instance for `REDIS_URL`. Same
`TRUST_PROXY_HOPS=1`.

**Fly.io** — `fly launch` picks up the `Dockerfile`. Set
`min_machines_running = 1`; a machine suspended to zero drops the SSE stream and
strands leases. `fly redis create` for the store.

**Cloud Run** — deploy the `Dockerfile` with `--min-instances=1` and
`--no-cpu-throttling`, or the metrics flush timer stops between requests.

**Docker anywhere** — `docker-compose.yml` brings up Redis and two app instances
on `:8080` and `:8081`, which is also the setup for the shared-limit demo in the
README.
