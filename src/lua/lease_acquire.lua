--[[
  Lease acquire — withdraw a block of tokens from the shared bucket in one go.

  This is the same token bucket as token_bucket.lua, with one difference in
  intent: instead of spending exactly what this request costs, the caller
  withdraws a block it intends to spend locally over the next few hundred
  milliseconds without coming back.

  The withdrawal is a hard debit. Once these tokens leave the bucket they are
  gone from every other instance's point of view, which is what makes the
  scheme safe: an instance can only serve what it has already paid for, so the
  sum of everything admitted across the fleet can never exceed what the bucket
  handed out. Leasing cannot over-admit. What it can do is strand quota on an
  instance that goes idle holding tokens — lease_release.lua and the reaper in
  lease.ts exist to bound how long that lasts.

  Partial fills are deliberate. Asking for 50 and being handed 12 is a useful
  answer; failing the whole request because the bucket is nearly empty is not.
  The only refusal is when the bucket cannot cover `need`, the cost of the
  request that triggered this acquire — that is a genuine throttle.

  Fractional tokens stay in the bucket. Refill is continuous so the bucket
  holds real numbers, but a lease is a count of whole requests, so the grant is
  floored and the remainder keeps accruing where every instance can see it.

  KEYS[1] = bucket key
  ARGV[1] = capacity   (bucket size, tokens)
  ARGV[2] = rate       (tokens per second, float)
  ARGV[3] = now        (unix ms; pass -1 to use the Redis server clock)
  ARGV[4] = need       (minimum useful grant — the triggering request's cost)
  ARGV[5] = want       (preferred grant; must be >= need)

  RETURN  = { granted, retry_after_ms, reset_after_ms, bucket_remaining, limit }
]]

local key      = KEYS[1]
local capacity = tonumber(ARGV[1])
local rate     = tonumber(ARGV[2])
local now      = tonumber(ARGV[3])
local need     = tonumber(ARGV[4])
local want     = tonumber(ARGV[5])

if capacity == nil or rate == nil or need == nil or want == nil then
  return redis.error_reply('ERR lease_acquire: non-numeric argument')
end
if rate <= 0 or capacity <= 0 then
  return redis.error_reply('ERR lease_acquire: capacity and rate must be > 0')
end
if need < 1 then
  return redis.error_reply('ERR lease_acquire: need must be >= 1')
end
if want < need then
  return redis.error_reply('ERR lease_acquire: want must be >= need')
end

if now < 0 then
  local t = redis.call('TIME')
  now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
end

local state  = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(state[1])
local ts     = tonumber(state[2])

-- Absent key == full bucket, exactly as in token_bucket.lua. A lease and a
-- direct check share the same key and the same state; lease mode changes how
-- often the bucket is touched, never what the bucket means.
if tokens == nil or ts == nil then
  tokens = capacity
  ts = now
end

local elapsed = now - ts
if elapsed < 0 then elapsed = 0 end
tokens = math.min(capacity, tokens + (elapsed * rate / 1000.0))

local granted = 0
local retry_after = 0

if tokens >= need then
  -- want >= need and tokens >= need, so the floor can never land below need.
  granted = math.floor(math.min(want, tokens))
  tokens = tokens - granted
else
  retry_after = math.ceil(((need - tokens) * 1000.0) / rate)
  if retry_after < 1 then retry_after = 1 end
end

local reset_after = math.ceil(((capacity - tokens) * 1000.0) / rate)

redis.call('HSET', key, 'tokens', tokens, 'ts', now)
-- The TTL is the moment the bucket would be full again, i.e. the moment its
-- state becomes indistinguishable from absent. Expiring earlier would hand
-- back capacity the client has not earned.
redis.call('PEXPIRE', key, reset_after + 1000)

return { granted, retry_after, reset_after, math.floor(tokens), math.floor(capacity) }
