# flowgate

A distributed rate limiter as a service. Token bucket and sliding window
implemented as atomic Lua scripts on Redis, exposed as Express middleware, with
a live dashboard of requests allowed vs throttled per client.

The interesting part is not "reject requests past N." It is that the limit holds
when eight connections check it in the same millisecond, that it survives Redis
going away, and that — once an instance starts leasing quota instead of asking
per request — **99.8 % of decisions never touch the network at all**, without
the ceiling moving by a single token.

```
                      ┌──────────────┐        ┌──────────────┐
   client ─────────►  │  api-1       │        │  api-2       │  ◄──── client
                      │  middleware  │        │  middleware  │
                      │  ┌────────┐  │        │  ┌────────┐  │  99.8% of
                      │  │ lease  │  │        │  │ lease  │  │  decisions
                      │  │ 500 tk │  │        │  │ 500 tk │  │  stop here
                      │  └───┬────┘  │        │  └───┬────┘  │
                      └──────┼───────┘        └──────┼───────┘
                             │  EVALSHA — a block,   │
                             │  not a request        │
                             └───────────┬───────────┘
                                         ▼
                              ┌────────────────────┐
                              │       Redis        │
                              │  buckets · windows │
                              │  policies · counts │
                              └────────────────────┘
                                         ▲
                              ┌──────────┴─────────┐
                              │  dashboard (SSE)   │
                              │  control API       │
                              └────────────────────┘
```

---

## Quick start

```bash
docker compose up --build          # Redis + two API instances (:8080, :8081)
```

or locally, against a Redis you already have:

```bash
npm install
npm start                          # http://localhost:8080
npm run traffic                    # drives the dashboard with simulated clients
```

Open <http://localhost:8080/> for the dashboard.

To run it somewhere other than your laptop, see **[DEPLOY.md](DEPLOY.md)**. The short version: this is a stateful process — an open SSE stream, a metrics
buffer, in-process histograms, a pub/sub subscriber and in-memory leases — so it
needs a host that keeps it running. Railway, Render, Fly, Cloud Run and plain
Docker all work; serverless platforms do not.

```bash
LEASE_ENABLED=1 docker compose up --build   # same limits, ~1% of the Redis traffic
```

**The demo worth running.** Both instances share one Redis, so the limit is a
property of the client, not the process:

```bash
K=demo
for i in $(seq 1 6); do curl -so /dev/null -w '8080 %{http_code}\n' localhost:8080/api/echo -H "x-api-key: $K"; done
for i in $(seq 1 6); do curl -so /dev/null -w '8081 %{http_code}\n' localhost:8081/api/echo -H "x-api-key: $K"; done
```

```
8080 200 ×6      ← six tokens spent on one instance
8081 200 ×4      ← four more on the other
8081 429 ×2      ← the 10-token burst is gone, wherever you ask
```

**The second demo worth running.** Restart with `LEASE_ENABLED=1` and run the
same loop. The verdicts are identical — the burst is still 10, still shared
across both instances — but `X-RateLimit-Source` now reads `lease`, and
`/admin/stats` shows the round trips that never happened:

```bash
curl -s localhost:8080/admin/stats | jq .leases
# { "hitRate": 0.998, "decisions": 59408, "acquires": 119, "redisOpsSaved": 59289, ... }
```

---

## The three algorithms

All three live in `src/lua/`. Each is one script, one round trip, and runs to
completion without interleaving — Redis executes Lua single-threaded, which is
what makes "check then decrement" safe without a lock.

| | `token_bucket` | `sliding_window_log` | `sliding_window_counter` |
|---|---|---|---|
| **Redis structure** | hash: `tokens`, `ts` | sorted set of timestamps | two integer counters |
| **Memory per client** | 2 fields | **O(limit)** — one member per request | 2 keys, always |
| **Exact?** | exact | exact | approximate — up to +8.7% high, measured below |
| **Allows bursts?** | yes, by design (`burst` > `limit`) | no | no |
| **Leasable?** | **yes** — a bucket is a credit pool | no | no |
| **Use it for** | user-facing APIs where a client should be able to spend saved-up quota | login, password reset, anything where the limit *is* the security control | high-cardinality traffic where per-client memory is the constraint |

The last row is the subject of [its own section below](#cutting-the-round-trip-local-token-leases): a token bucket can hand out
its quota in blocks, which removes the network round trip from 99.8 % of
decisions without changing what the limit is.

### Token bucket

Refill is computed lazily from elapsed time, not by a background timer:

```lua
tokens = math.min(capacity, tokens + (elapsed * rate / 1000.0))
```

Two consequences worth knowing. First, cost is O(1) with no sweeper process.
Second, **an absent key is indistinguishable from a full bucket**, which is why
expiring idle state is safe rather than a correctness hole — the TTL is set to
exactly the time the bucket would take to refill to capacity.

The separation of `limit` from `burst` is the reason to reach for this one. The
`pro` tier is 600/min with a burst of 120: a batch job can fire 120 requests
immediately after idling, then settles to 10/s. A window algorithm cannot
express that.

### Sliding window log

One sorted-set member per accepted request, trimmed by `ZREMRANGEBYSCORE` on
every call. There is no window boundary, so the classic fixed-counter failure —
5 requests at 11:59:59.999 and 5 more at 12:00:00.000, i.e. **2× the limit in
one millisecond** — cannot happen. There is a test that specifically asserts
this (`refuses the boundary burst a fixed window would allow`).

The price is memory: a 10,000/min policy holds up to 10,000 members per client.
`validatePolicy` refuses limits above 100,000 for this algorithm rather than
letting someone configure a memory incident through the admin API.

### Sliding window counter

The O(1) approximation. Keep one counter per fixed window and estimate the
rolling count by weighting the previous window:

```
estimate = current + previous × (1 − elapsed_in_current / window)
```

Two integers per client regardless of the limit. It assumes the previous
window's requests were spread evenly, so a burst clustered at that window's end
is under-counted.

**Measured error.** The size of that error depends on where traffic sits
relative to a window boundary, so one sample would be luck. The test replays an
identical 5-minute trace at 2 req/s against a 60/min limit through both
algorithms at eight phase offsets and keeps the worst case
(`test/sliding-window.test.ts`):

| | admitted, worst phase |
|---|---|
| `sliding_window_log` (exact) | 300 — phase-independent |
| `sliding_window_counter` | 326 (**+8.7 %**) |

That is the trade stated as a number rather than a vibe: up to ~9%
over-admission in exchange for constant memory. Bounded and reproducible is the
property that matters — which is also why `login` does *not* use it.

---

## Cutting the round trip: local token leases

Everything above costs one network round trip per request. At 0.04 ms that is
affordable. At 100k req/s across a fleet it is 100k round trips per second
landing on one shared node, and Redis — not the app — becomes the ceiling on the
whole service.

A lease moves the decision off the network. An instance withdraws a **block** of
a client's tokens in one round trip and then serves requests out of that block
from process memory: a map lookup and a subtraction.

```
without leases                          with leases
─────────────────                       ─────────────────
req ──► EVALSHA ──► Redis  0.044 ms     req ──► map lookup       0.001 ms
req ──► EVALSHA ──► Redis  0.044 ms     req ──► map lookup       0.001 ms
req ──► EVALSHA ──► Redis  0.044 ms     req ──► map lookup       0.001 ms
  … 20,000 requests, 20,000 ops           … every ~500th: EVALSHA, one block
```

### Why this does not weaken the limit

The withdrawal is a hard debit performed by the same kind of atomic script as
everything else. Once tokens leave the bucket they are gone from every other
instance's point of view, so:

> an instance can only admit what it has already paid Redis for,
> therefore **total admitted ≤ total withdrawn ≤ what the bucket allowed**.

Leasing cannot over-admit. This is worth stating precisely because it is *not*
the same kind of claim as `sliding_window_counter`'s ±8.7 %: that algorithm
trades exactness for memory, whereas a lease trades **promptness** for round
trips. The ceiling is exactly as hard as before.

`test/lease.test.ts` fires the same stampede as the unleased test — 500
requests, 8 instances each with their own lease cache, one bucket of 50 — and
asserts **exactly 50** allowed. A second test runs a leased fleet and an
unleased fleet through identical traffic and asserts the two admit the *same
number*, so a regression that loosened the limit could not pass quietly.

### What it actually costs

Two things, both real, both tested rather than described:

**Quota can sit idle.** An instance that leases 40 tokens, spends 3, and goes
quiet is holding 37 tokens the client cannot use anywhere else. Two mechanisms
bound this: every lease has a TTL (default 1 s), and a background reaper returns
unspent tokens with `lease_release.lua` — pipelined, off the request path, in
the same spirit as the buffered metrics writer. A single lease is also capped at
25 % of the bucket, so no one instance can corner a client's burst. `SIGTERM`
returns everything, because a rolling deploy without that strands one lease per
client per instance on every restart.

**`RateLimit-Remaining` becomes a lower bound.** A leased reply can only
honestly report what *this* instance still holds. Under-reporting is the right
direction for a client-facing header — it makes callers back off early rather
than late — and `GET /api/quota` still reads the true shared figure, because a
zero-cost peek bypasses the lease by design. `X-RateLimit-Source: lease` marks
which replies are affected.

### Three details that make it work in practice

**The lease size is a control loop, not a constant.** The right block size
depends on a client's actual request rate, which nothing knows in advance. A
lease drained before its TTL doubles the next one; a lease that expires with
tokens left shrinks toward what was actually spent. Both directions converge on
roughly one acquire per TTL — high hit rate for busy clients, almost no stranded
quota for quiet ones.

**Concurrent misses coalesce.** Fifty simultaneous requests for a cold key would
otherwise fire fifty acquires: a thundering herd aimed at exactly the dependency
the lease exists to spare, and fifty separate withdrawals from the client's
bucket. One acquire is in flight per key; the rest wait on it.

**Refusals are cached too.** The throttled path is the one that matters under
abuse — a client hammering past their limit must not be able to convert their
own bad behaviour into load on Redis. When Redis refuses, the instance memoises
that refusal for exactly `retryAfterMs`, which is not a guess but the precise
time the bucket needs to accrue the deficit. Measured: **200 rejected requests,
fewer than 10 Redis commands.** The memo records the cost it was issued for,
since being unable to afford 5 tokens says nothing about affording 1.

### The failure mode gets better, not worse

Without leases, losing Redis means choosing between unlimited traffic (fail
open) and no traffic (fail closed). With leases there is a third state: an
instance holding tokens it has *already paid for* can keep serving exactly those
and then stop. A fail-closed endpoint degrades to a bounded allowance instead of
a cliff — and an attacker gets a bounded allowance instead of an open door.
Those decisions are flagged `degraded` so the outage is never hidden.

### What is deliberately excluded

Only `token_bucket` can be leased. A bucket is a credit pool and withdrawing
credit from a pool is well defined; a sliding window is a statement about the
arrival times of individual requests, and there is no coherent way to withdraw
part of one. That means `login` — the policy where the limit *is* the security
control — is excluded **by construction** rather than by remembering to exclude
it, which is the kind of safety property worth arranging on purpose.

Buckets too small to benefit are skipped as well: a 10-token bucket permits at
most a 2-token lease, which saves one round trip in two while costing a map
lookup on every request and stranding quota from a client who has little to
begin with. Those go straight to Redis.

```bash
LEASE_ENABLED=1 npm start        # or LEASE_ENABLED=1 docker compose up
curl -sD- localhost:8080/api/echo -H 'x-api-key: demo' | grep -i source
# X-RateLimit-Source: lease
```

---

## Benchmarks

One machine, Node 22, Redis 7 on loopback, no network hop — so these are a floor
for the *added* cost, not capacity planning for your hardware. Everything below
comes from `npm run bench`; re-run it and the tables print in this shape.

Loopback matters for reading the lease numbers honestly: it is the setup least
favourable to them. A real deployment pays 0.3–1 ms per round trip rather than
0.04 ms, so removing the round trip is worth proportionally more, not less.

**Cost of one decision, serial (`CONCURRENCY=1 npm run bench`)** — this is the
latency a single request actually pays:

| algorithm | p50 | p95 | p99 |
|---|---|---|---|
| `token_bucket` | 0.08 ms | 0.14 ms | 0.20 ms |
| `sliding_window_log` | 0.09 ms | 0.14 ms | 0.20 ms |
| `sliding_window_counter` | 0.08 ms | 0.12 ms | 0.17 ms |
| *bare `PING` (the floor)* | *0.07 ms* | | |

**The script itself is under 0.02 ms.** Almost all of the decision latency is
the round trip that any Redis-backed limiter has to pay — which is what the next
table is about.

**With and without the lease layer, serial, same bucket and same policy.** The
"redis ops" column is read from Redis' own `INFO commandstats`, not counted in
application code, so it is what the server actually saw:

| mode | throughput | p50 | p95 | p99 | redis ops | decided locally |
|---|---|---|---|---|---|---|
| direct, per request | 22,400 /s | 0.044 ms | 0.056 ms | 0.070 ms | 20,000 | — |
| **leased** | **745,858 /s** | **0.001 ms** | **0.001 ms** | **0.002 ms** | **40** | **99.8 %** |

**33× the throughput, 44× lower p50, 99.8 % fewer round trips** — and the same
limit, which is the part `test/lease.test.ts` exists to prove. The saving scales
with a client's request rate relative to their quota: a lease is capped at 25 %
of the bucket, so a client sending 10 req/s against a 60/min quota saves little,
and a client sending 5,000 req/s against a large one saves nearly everything.

**Throughput, 50 decisions in flight (`npm run bench`):**

| algorithm | throughput | p99 |
|---|---|---|
| `token_bucket` | 113,015 /s | 1.54 ms |
| `sliding_window_log` | 90,165 /s | 2.07 ms |
| `sliding_window_counter` | 110,567 /s | 0.97 ms |

The p99 here is queueing, not service time — 50 concurrent requests against one
Redis connection. The serial table above is the honest per-request number.

**End to end over HTTP (`npm run bench -- --http`, autocannon, 50 connections).**
The benchmark puts its client on a quota it will not exhaust, so this measures
the cost of *deciding*, not the cost of refusing — refusing is cheaper, and
measuring that would flatter the limiter:

| route | req/s | p50 | p99 | limiter cost |
|---|---|---|---|---|
| `/health` (no limiter) | 5,715 | 7 ms | 24 ms | — |
| `/api/echo` (limited) | 4,695 | 10 ms | 19 ms | **17.8 %** |
| `/api/echo` (limited, `LEASE_ENABLED=1`) | 5,397 | 8 ms | 16 ms | **6.6 %** |

Leasing cuts the limiter's throughput cost from 17.8 % to 6.6 % and takes 3 ms
off p99. Over that run the service made **59,408 decisions using 119 Redis round
trips**, with zero lease races.

---

## Design decisions

Roughly in the order an interviewer tends to ask about them.

**Why Lua instead of `WATCH`/`MULTI` or a lock?**
The decision is read-modify-write: read the bucket, compute the refill, decide,
write back. Doing that in application code across N processes is a race — two
instances read `tokens = 1` and both allow. `WATCH`/`MULTI` turns the race into
a retry loop that degrades exactly when contention is highest, which for a rate
limiter is exactly when the limit matters. A Lua script runs to completion as
one unit, so the whole read-decide-write is atomic in a single round trip.
`test/concurrency.test.ts` fires 500 requests from 8 connections at a limit of
50 and asserts **exactly 50** allowed, for all three algorithms.

**Whose clock?**
The scripts take `now` as an argument but pass `-1` in production, which makes
the script read `TIME` from the Redis server. If each app instance supplied its
own wall clock, a client could gain capacity by landing on a node whose clock
had drifted forward. One server, one timeline. Tests pass a fixed timestamp so
refill behaviour is deterministic instead of sleep-based.

**Key layout: `rl:{client}:algo:policy`**
The braces are a Redis Cluster hash tag, so everything for one client lands in
one slot. `sliding_window_counter` needs this — it touches two derived keys in
one script, and Redis refuses a script whose keys span slots. Including the
policy name means moving a client from `free` to `pro` gives them a clean
bucket rather than carrying debt across tiers.

**Fail open or fail closed?**
Both, per mount point. The default is **open**: a limiter outage should degrade
protection, not availability, and the decision is flagged `degraded` with an
`X-RateLimit-Degraded` header so the dashboard and your alerting can see it.
`/api/login` is mounted on a **closed** limiter, because there the limit *is*
the brute-force protection — serving 503s beats serving unlimited password
guesses. The offline queue is disabled so a failing command rejects in
microseconds instead of piling up behind a reconnect.

**Why is a lease safe when a read-modify-write in application code is not?**
Because a lease is not a read-modify-write. The instance does not read the
bucket, decide, and write back — it asks Redis to *atomically hand it N tokens*,
and Redis either does or does not. The result is that local state can only ever
be a claim on tokens already removed from the shared bucket, never an opinion
about what the shared bucket contains. That is the whole difference between this
and the naive "cache the counter locally" optimisation, which does over-admit.

**Why lease at all rather than just adding Redis replicas?**
Replicas do not help. Every limiter decision is a write, so it goes to the
primary regardless of how many replicas exist. The two real options are sharding
by client — which the hash tags already permit — or reducing the number of
writes, which is what leasing does. They compose: leasing cuts the write rate by
two orders of magnitude first, so sharding gets postponed rather than replaced.

**Why is `RateLimit-Remaining` allowed to be wrong under leasing?**
It is not wrong, it is a lower bound, and the direction matters. A client told
they have fewer requests left than they do will back off early; a client told
they have more will be surprised by a 429. Headers that guide client behaviour
should fail toward caution. The exact figure is still available from
`GET /api/quota`, which uses a zero-cost peek that bypasses the lease.

**Why buffer metrics instead of `HINCRBY` per request?**
Writing a counter per request would double the Redis round trips the limiter
costs — the observability would be more expensive than the thing it observes.
Counts are aggregated in process and pipelined every 250 ms. The trade is at
most 250 ms of counts lost on a hard crash, and a failed flush is merged back
into the buffer rather than dropped. `SIGTERM` flushes before exit.

**Why SSE and not WebSockets?**
The dashboard needs a 1 Hz push in one direction. SSE gives that over plain
HTTP with browser-native reconnect and no protocol upgrade.

**Why is `RateLimit-Limit` different from `RateLimit-Policy` on a bucket?**
`RateLimit-Limit`/`Remaining` describe the bucket — tokens available now.
`RateLimit-Policy` advertises the sustained quota and window plus the burst
size. For `pro` that reads `600;w=60;burst=120`: you earn 600/min, you may
spend 120 at once. Both the IETF draft headers and the legacy `X-` family are
emitted, because most SDKs still read the latter.

**Client identity**
API key, then bearer token, then IP. The bearer case keys on a prefix and
length, never the raw credential — limiter keys end up in logs, `SCAN` output,
and on a dashboard. `TRUST_PROXY_HOPS` matters more than it looks: behind a load
balancer with the wrong hop count, `X-Forwarded-For` is attacker-controlled and
IP limiting is decorative.

**Memory bounds**
Every key gets a TTL sized to the moment its state becomes equivalent to absent.
Redis in `docker-compose.yml` runs `allkeys-lru` with no persistence — limiter
state is disposable, and shedding cold buckets under pressure is better than
failing writes with OOM.

---

## API

### Protected demo routes

| route | behaviour |
|---|---|
| `GET /api/echo` | limited by the caller's tier |
| `POST /api/report` | same, but **costs 5 tokens** — expensive endpoints should charge accordingly |
| `POST /api/login` | fixed `login` policy, exact algorithm, fail-closed |
| `GET /api/quota` | reads remaining quota with `cost: 0` — does not spend it |
| `GET /health` | not limited |
| `GET /metrics` | Prometheus exposition, including lease hit rate and round trips saved |

Every limited response carries `X-RateLimit-Source: redis | lease`, so it is
visible per request whether the decision cost a round trip.

### Control plane (`Authorization: Bearer $ADMIN_TOKEN`)

```bash
# Create or update a policy — every instance picks it up via pub/sub, no restart
curl -X PUT localhost:8080/admin/policies/partner \
  -H 'content-type: application/json' \
  -d '{"algorithm":"token_bucket","limit":1200,"windowMs":60000,"burst":200}'

curl -X PUT localhost:8080/admin/assignments/key:acme \
  -H 'content-type: application/json' -d '{"policy":"partner"}'

curl -X POST localhost:8080/admin/clients/key:acme/reset   # support tooling
curl localhost:8080/admin/stats
curl -N localhost:8080/admin/stream                        # SSE, 1 Hz
```

Policies live in Redis and are cached in process, so the request path never
reads one over the network. A write publishes an invalidation; a 30-second TTL
sweep is the backstop, because pub/sub is at-most-once.

### Using the middleware in your own app

```ts
import { createRedis, whenReady } from './src/core/redis.js';
import { RateLimiter } from './src/core/limiter.js';
import { PolicyStore } from './src/core/policies.js';
import { MetricsRecorder } from './src/metrics/metrics.js';
import { rateLimit } from './src/server/middleware.js';

const redis = createRedis(process.env.REDIS_URL);
await whenReady(redis);

const policies = new PolicyStore(redis);
await policies.init();

const deps = {
  limiter: new RateLimiter(redis, {
    failureMode: 'open',
    // Optional. Serves most decisions from process memory without changing the
    // limit; see "Local token leases" above for what it trades away.
    lease: { enabled: true, ttlMs: 1000 },
  }),
  policies,
  metrics: new MetricsRecorder(redis),
};

app.use('/v1', rateLimit(deps));
app.post('/v1/search', rateLimit(deps, { cost: (req) => req.body.pageSize ?? 1 }));
app.post('/v1/login', rateLimit(
  { ...deps, limiter: new RateLimiter(redis, { failureMode: 'closed' }) },
  { policy: 'login' },
));

// Leases are held per process. Return them on the way out, or a rolling deploy
// strands one per client per instance on every restart.
process.on('SIGTERM', () => void deps.limiter.close());
```

---

## Tests

```bash
npm test        # 53 tests, against a real Redis
```

They run against a real Redis on purpose — a mock cannot tell you whether the
Lua is atomic under concurrency, and that is the property the whole design rests
on. Time is injected, so refill, window-expiry and lease-expiry tests are
deterministic rather than `sleep`-based. Two of the lease tests go further and
read Redis' own `INFO commandstats`, because "this optimisation avoids round
trips" is a claim about the server, and the server is the only honest witness.

| file | what it pins down |
|---|---|
| `token-bucket.test.ts` | burst, continuous refill, capacity ceiling after a day idle, `retryAfterMs` accurate to the millisecond, multi-token cost, TTL bounds |
| `sliding-window.test.ts` | exactness, one-at-a-time sliding, the boundary-burst case, member count ≤ limit, counter weighting, measured approximation error |
| `concurrency.test.ts` | 500 requests / 8 connections / limit 50 → exactly 50, all three algorithms |
| `resilience.test.ts` | fail open, fail closed, no hang on a dead Redis, policy pub/sub propagation, config validation |
| `http.test.ts` | headers, 429 body, per-key isolation, cost, skip, metrics recording, client reset |
| `lease.test.ts` | that leasing does not move the ceiling — same stampede, same answer, and parity with an unleased fleet — plus round trips counted at the Redis server, coalescing, adaptive sizing, the deny memo, return-on-expiry and on-shutdown, capacity-capped credits, bounded degradation during an outage, and memory bounds |

---

## Limitations, and what I'd do next

Being explicit about what this does *not* do:

- **One Redis is still a single point of contention.** Leasing cut the write
  rate by two orders of magnitude, which postpones the problem rather than
  removing it. The next step is Cluster — the hash tags are already in place —
  or sharding by client with a consistent hash.
- **Leased quota is per instance, and instances do not talk to each other.** A
  client's tokens can sit on an idle instance for up to the lease TTL. The
  reaper and the 25 % cap bound it, but a fleet with very many instances and
  very low per-client traffic would see the effect grow; the fix is a smaller
  TTL, or turning leases off for those tiers, both of which are configuration.
- **Sliding windows still pay a round trip each.** That is a real limit of the
  approach, not an omission: leasing a window is not a well-defined operation.
  Bringing the same saving to `sliding_window_counter` would mean batching
  increments and accepting genuine over-admission, which is a different trade
  and would need its own error bound.
- **Metrics are per-second for 15 minutes.** There is no rollup, so the
  dashboard cannot show a day. The Prometheus endpoint is the answer for
  retention.
- **No per-endpoint policy routing beyond the mount point.** Policies attach to
  a client tier or a mounted route; a pattern-matching route table would be the
  natural extension.
- **The dashboard is read-only.** Editing policies means the control API. Wiring
  the form to `PUT /admin/policies/:name` is small, and deliberately not done
  yet — the API is the contract.

## Layout

```
src/lua/            the algorithms, plus lease acquire/release — the substance
src/core/           limiter, lease cache, policy store, Redis wiring, validation
src/server/         Express middleware, control API, SSE, entrypoint
src/metrics/        buffered recorder, log-bucketed latency histogram
public/index.html   dashboard, self-contained, no build step
test/               53 tests against a real Redis
bench/              benchmark harness and demo traffic generator
```
