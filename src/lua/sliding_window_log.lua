--[[
  Sliding window log — exact.

  Every accepted request is recorded as one member of a sorted set scored by
  its timestamp. On each call we drop everything older than the window and
  count what is left. No fixed-window boundary exists, so the 2x burst that
  a naive fixed counter allows across a boundary is impossible here.

  The cost is memory: O(limit) members per client. A 10k/min policy holds up
  to 10k members per client. That is the trade this file exists to make
  explicit — sliding_window_counter.lua is the O(1) approximation.

  KEYS[1] = zset key
  ARGV[1] = limit
  ARGV[2] = window_ms
  ARGV[3] = now             (unix ms; -1 for the Redis server clock)
  ARGV[4] = cost
  ARGV[5] = request id      (uniquifier; members must not collide)

  RETURN  = { allowed, remaining, retry_after_ms, reset_after_ms, limit }
]]

local key    = KEYS[1]
local limit  = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local now    = tonumber(ARGV[3])
local cost   = tonumber(ARGV[4])
local reqid  = ARGV[5]

if limit == nil or window == nil or cost == nil then
  return redis.error_reply('ERR sliding_window_log: non-numeric argument')
end
if limit <= 0 or window <= 0 then
  return redis.error_reply('ERR sliding_window_log: limit and window must be > 0')
end

if now < 0 then
  local t = redis.call('TIME')
  now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
end

-- Half-open window (cutoff, now]: an entry exactly `window` old has expired.
local cutoff = now - window
redis.call('ZREMRANGEBYSCORE', key, '-inf', cutoff)

local count = redis.call('ZCARD', key)
local allowed = 0
local retry_after = 0

if count + cost <= limit then
  allowed = 1
  for i = 1, cost do
    redis.call('ZADD', key, now, now .. '-' .. reqid .. '-' .. i)
  end
  count = count + cost
else
  -- We need `need` of the oldest entries to age out before this request fits.
  -- The need-th oldest is at rank need-1; when it expires, room appears.
  local need = count + cost - limit
  local nth = redis.call('ZRANGE', key, need - 1, need - 1, 'WITHSCORES')
  if nth[2] then
    retry_after = math.ceil(tonumber(nth[2]) + window - now)
  else
    retry_after = window
  end
  if retry_after < 1 then retry_after = 1 end
end

-- Idle clients drain naturally; the TTL is a backstop so abandoned keys go away.
redis.call('PEXPIRE', key, window + 1000)

local reset_after = 0
if count > 0 then
  local oldest = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
  if oldest[2] then
    reset_after = math.ceil(tonumber(oldest[2]) + window - now)
  end
end
if reset_after < 0 then reset_after = 0 end

local remaining = limit - count
if remaining < 0 then remaining = 0 end

return { allowed, remaining, retry_after, reset_after, math.floor(limit) }
