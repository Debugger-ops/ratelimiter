--[[
  Token bucket.

  A bucket holds up to `capacity` tokens and refills continuously at `rate`
  tokens/second. A request costs `cost` tokens and is allowed only if the
  bucket holds at least that many.

  We do NOT run a refill timer. Refill is computed lazily from the elapsed
  time since the last write, which is what makes this O(1) in both time and
  memory per client: two fields, one round trip, no background job.

  KEYS[1] = bucket key
  ARGV[1] = capacity        (max burst, tokens)
  ARGV[2] = rate            (tokens per second, float)
  ARGV[3] = now             (unix ms; pass -1 to use the Redis server clock)
  ARGV[4] = cost            (tokens this request consumes)

  RETURN  = { allowed, remaining, retry_after_ms, reset_after_ms, limit }
]]

local key      = KEYS[1]
local capacity = tonumber(ARGV[1])
local rate     = tonumber(ARGV[2])
local now      = tonumber(ARGV[3])
local cost     = tonumber(ARGV[4])

if capacity == nil or rate == nil or cost == nil then
  return redis.error_reply('ERR token_bucket: non-numeric argument')
end
if rate <= 0 or capacity <= 0 then
  return redis.error_reply('ERR token_bucket: capacity and rate must be > 0')
end

-- Using the Redis clock keeps every app instance on one timeline, so a client
-- cannot gain capacity by hitting a node whose wall clock has drifted forward.
-- Tests pass an explicit `now` so time can be controlled.
if now < 0 then
  local t = redis.call('TIME')
  now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
end

local state  = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(state[1])
local ts     = tonumber(state[2])

-- Absent key == full bucket. This is why expiry is safe: an evicted bucket
-- is indistinguishable from an idle client that has refilled to capacity.
if tokens == nil or ts == nil then
  tokens = capacity
  ts = now
end

local elapsed = now - ts
if elapsed < 0 then elapsed = 0 end          -- clamp: never refill on a clock step back
tokens = math.min(capacity, tokens + (elapsed * rate / 1000.0))

local allowed = 0
local retry_after = 0

if tokens >= cost then
  allowed = 1
  tokens = tokens - cost
else
  -- Time for the deficit to accrue, rounded up to the next whole ms.
  retry_after = math.ceil(((cost - tokens) * 1000.0) / rate)
  if retry_after < 1 then retry_after = 1 end
end

-- Time for the bucket to return to full, i.e. to become equivalent to absent.
local reset_after = math.ceil(((capacity - tokens) * 1000.0) / rate)

redis.call('HSET', key, 'tokens', tokens, 'ts', now)
redis.call('PEXPIRE', key, reset_after + 1000)

return { allowed, math.floor(tokens), retry_after, reset_after, math.floor(capacity) }
