--[[
  Lease release — return unspent leased tokens to the shared bucket.

  An instance that leases 40 tokens and then goes quiet after spending 3 is
  holding 37 tokens that belong to the client, not to the instance. Nothing is
  broken if they are simply dropped — the bucket refills on its own and the
  client is under-served for at most a refill interval — but returning them is
  cheap, happens off the request path, and keeps a client's quota available to
  whichever instance their next request lands on.

  The credit is capped at capacity, which is what stops a return from creating
  tokens. If the bucket has already refilled to full while this lease sat idle,
  the client has been given that capacity back by the refill, and crediting the
  lease on top would hand it to them twice. Same reasoning for an absent key:
  absent means full, so returning into it is a no-op rather than a windfall.

  Refill is recomputed here before crediting, for one reason: this script
  writes `ts`, and writing a fresh timestamp without first accounting for the
  elapsed time would silently erase the tokens that accrued while the lease was
  held.

  KEYS[1] = bucket key
  ARGV[1] = capacity
  ARGV[2] = rate      (tokens per second, float)
  ARGV[3] = now       (unix ms; pass -1 to use the Redis server clock)
  ARGV[4] = amount    (unspent tokens being returned)

  RETURN  = { bucket_tokens_after, reset_after_ms }
]]

local key      = KEYS[1]
local capacity = tonumber(ARGV[1])
local rate     = tonumber(ARGV[2])
local now      = tonumber(ARGV[3])
local amount   = tonumber(ARGV[4])

if capacity == nil or rate == nil or amount == nil then
  return redis.error_reply('ERR lease_release: non-numeric argument')
end
if rate <= 0 or capacity <= 0 then
  return redis.error_reply('ERR lease_release: capacity and rate must be > 0')
end
if amount < 0 then
  return redis.error_reply('ERR lease_release: amount must be >= 0')
end

if now < 0 then
  local t = redis.call('TIME')
  now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
end

local state  = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(state[1])
local ts     = tonumber(state[2])

if tokens == nil or ts == nil then
  -- Absent == full. There is nothing to give back to a full bucket.
  return { math.floor(capacity), 0 }
end

local elapsed = now - ts
if elapsed < 0 then elapsed = 0 end
tokens = math.min(capacity, tokens + (elapsed * rate / 1000.0))
tokens = math.min(capacity, tokens + amount)

local reset_after = math.ceil(((capacity - tokens) * 1000.0) / rate)

redis.call('HSET', key, 'tokens', tokens, 'ts', now)
redis.call('PEXPIRE', key, reset_after + 1000)

return { math.floor(tokens), reset_after }
