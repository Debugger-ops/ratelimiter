--[[
  Sliding window counter — the O(1) approximation of the log.

  Keep one integer counter per fixed window. Estimate the rolling count as the
  current window's exact count plus a linearly-weighted share of the previous
  window's:

      estimate = current + previous * (1 - elapsed_in_current / window)

  Two keys and two integers per client regardless of the limit, versus one
  sorted-set member per request in the log. The approximation assumes requests
  were spread evenly across the previous window, so it admits slightly more
  than the exact algorithm would. Measured against sliding_window_log on the
  same trace, swept across window phases (test/sliding-window.test.ts): worst
  case +8.7% over-admission. Bounded and reproducible, which is what makes the
  trade defensible for high-cardinality traffic; do not use it where the limit
  is a security control.

  KEYS[1] = key prefix (window index is appended)
  ARGV[1] = limit
  ARGV[2] = window_ms
  ARGV[3] = now             (unix ms; -1 for the Redis server clock)
  ARGV[4] = cost

  RETURN  = { allowed, remaining, retry_after_ms, reset_after_ms, limit }
]]

local prefix = KEYS[1]
local limit  = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local now    = tonumber(ARGV[3])
local cost   = tonumber(ARGV[4])

if limit == nil or window == nil or cost == nil then
  return redis.error_reply('ERR sliding_window_counter: non-numeric argument')
end
if limit <= 0 or window <= 0 then
  return redis.error_reply('ERR sliding_window_counter: limit and window must be > 0')
end

if now < 0 then
  local t = redis.call('TIME')
  now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
end

local idx      = math.floor(now / window)
local elapsed  = now - (idx * window)
local weight   = 1.0 - (elapsed / window)

local cur_key  = prefix .. ':' .. idx
local prev_key = prefix .. ':' .. (idx - 1)

local cur  = tonumber(redis.call('GET', cur_key)) or 0
local prev = tonumber(redis.call('GET', prev_key)) or 0

local estimate = (prev * weight) + cur
local allowed = 0
local retry_after = 0

if estimate + cost <= limit then
  allowed = 1
  if cost > 0 then                      -- cost 0 is a peek; do not create state
    cur = redis.call('INCRBY', cur_key, cost)
    -- Must outlive its role as "previous window" for the next window.
    redis.call('PEXPIRE', cur_key, (window * 2) + 1000)
    estimate = estimate + cost
  end
else
  if prev > 0 then
    -- The previous window's contribution decays at prev/window per ms.
    -- Solve estimate(t) + cost = limit for t.
    local overflow = estimate + cost - limit
    retry_after = math.ceil((overflow * window) / prev)
    local until_rollover = window - elapsed
    if retry_after > until_rollover then retry_after = until_rollover end
  else
    -- Nothing decays inside this window; wait for the rollover.
    retry_after = window - elapsed
  end
  if retry_after < 1 then retry_after = 1 end
end

local remaining = math.floor(limit - estimate)
if remaining < 0 then remaining = 0 end

return { allowed, remaining, retry_after, window - elapsed, math.floor(limit) }
