-- Atomic admission: request rate limit + token budget check + reservation, in one round trip.
-- Nothing is committed unless every check passes, so a rejected request costs the tenant nothing.
--
-- KEYS[1] rate-limit state (hash)
-- KEYS[2] balance for the budget window (hash: in, out, rin, rout)
-- KEYS[3] reservation expiry index (zset: id -> expiry ms)
-- KEYS[4] reservation amounts (hash: id -> "in:out")
--
-- ARGV[1]  mode            "reserve" | "posthoc"
-- ARGV[2]  reservation id
-- ARGV[3]  est_in          input tokens to reserve
-- ARGV[4]  est_out         output tokens to reserve (the minimum needed to admit)
-- ARGV[5]  est_out_max     grant up to this many output tokens if headroom allows (>= est_out)
-- ARGV[6]  lim_in          0 = unlimited
-- ARGV[7]  lim_out
-- ARGV[8]  lim_total
-- ARGV[9]  reservation ttl ms
-- ARGV[10] key ttl s
-- ARGV[11] rate algorithm  "none" | "sw" | "tb"
-- ARGV[12] rate limit      requests per period
-- ARGV[13] rate period ms
-- ARGV[14] bucket capacity (token bucket only)
--
-- Returns {1, granted_out, used_total, reserved_total}
--      or {0, reason, retry_after_ms} with reason in rate | busy | input | output | total
--      (busy: only in-flight reservations are in the way; retry shortly)

local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)

local mode = ARGV[1]
local rid = ARGV[2]
local est_in = tonumber(ARGV[3])
local est_out = tonumber(ARGV[4])
local est_out_max = tonumber(ARGV[5])
local lim_in = tonumber(ARGV[6])
local lim_out = tonumber(ARGV[7])
local lim_total = tonumber(ARGV[8])
local resv_ttl = tonumber(ARGV[9])
local key_ttl = tonumber(ARGV[10])
local algo = ARGV[11]
local rl_limit = tonumber(ARGV[12])
local rl_period = tonumber(ARGV[13])
local rl_cap = tonumber(ARGV[14])

-- 1. Request rate limit (checked, not yet committed).
local rl_commit = nil
if algo == 'sw' and rl_limit > 0 then
  -- Sliding-window counter: weight the previous fixed window by how much of it still overlaps.
  local idx = math.floor(now / rl_period)
  local s = redis.call('HMGET', KEYS[1], 'w', 'c', 'p')
  local w, c, p = tonumber(s[1]), tonumber(s[2]) or 0, tonumber(s[3]) or 0
  if w == nil then
    c, p = 0, 0
  elseif w == idx - 1 then
    p, c = c, 0
  elseif w ~= idx then
    c, p = 0, 0
  end
  local elapsed = (now - idx * rl_period) / rl_period
  local estimate = p * (1 - elapsed) + c
  if estimate + 1 > rl_limit then
    local retry = math.ceil(rl_period * (1 - elapsed))
    return {0, 'rate', retry}
  end
  rl_commit = function()
    redis.call('HSET', KEYS[1], 'w', idx, 'c', c + 1, 'p', p)
    redis.call('PEXPIRE', KEYS[1], rl_period * 2)
  end
elseif algo == 'tb' and rl_limit > 0 then
  local rate = rl_limit / rl_period -- tokens per ms
  local s = redis.call('HMGET', KEYS[1], 'tk', 'ts')
  local tk, ts = tonumber(s[1]), tonumber(s[2])
  if tk == nil then
    tk, ts = rl_cap, now
  end
  tk = math.min(rl_cap, tk + (now - ts) * rate)
  if tk < 1 then
    return {0, 'rate', math.ceil((1 - tk) / rate)}
  end
  rl_commit = function()
    redis.call('HSET', KEYS[1], 'tk', tostring(tk - 1), 'ts', now)
    redis.call('PEXPIRE', KEYS[1], math.ceil(rl_cap / rate) + 1000)
  end
end

-- 2. Reclaim reservations left behind by replicas that died mid-request.
local expired = redis.call('ZRANGEBYSCORE', KEYS[3], '-inf', now, 'LIMIT', 0, 100)
for _, id in ipairs(expired) do
  local amt = redis.call('HGET', KEYS[4], id)
  if amt then
    local sep = string.find(amt, ':', 1, true)
    redis.call('HINCRBY', KEYS[2], 'rin', -tonumber(string.sub(amt, 1, sep - 1)))
    redis.call('HINCRBY', KEYS[2], 'rout', -tonumber(string.sub(amt, sep + 1)))
    redis.call('HDEL', KEYS[4], id)
  end
  redis.call('ZREM', KEYS[3], id)
end

-- 3. Budget check.
local b = redis.call('HMGET', KEYS[2], 'in', 'out', 'rin', 'rout')
local used_in, used_out = tonumber(b[1]) or 0, tonumber(b[2]) or 0
local rin, rout = tonumber(b[3]) or 0, tonumber(b[4]) or 0

if mode == 'posthoc' then
  -- Count-after-the-fact baseline: admit while anything is left, charge at the end.
  if lim_in > 0 and used_in >= lim_in then return {0, 'input', 0} end
  if lim_out > 0 and used_out >= lim_out then return {0, 'output', 0} end
  if lim_total > 0 and used_in + used_out >= lim_total then return {0, 'total', 0} end
  if rl_commit then rl_commit() end
  return {1, est_out_max, used_in + used_out, 0}
end

-- Headroom for output after this request's input is reserved.
local head = est_out_max
if lim_out > 0 then head = math.min(head, lim_out - used_out - rout) end
if lim_total > 0 then head = math.min(head, lim_total - used_in - used_out - rin - rout - est_in) end
local in_ok = lim_in == 0 or used_in + rin + est_in <= lim_in
if not in_ok or head < est_out then
  -- Would it fit if the in-flight reservations were released? Then the rejection is temporary
  -- ("busy"); otherwise the window's budget is spent.
  if lim_in > 0 and used_in + est_in > lim_in then return {0, 'input', 0} end
  if lim_out > 0 and used_out + est_out > lim_out then return {0, 'output', 0} end
  if lim_total > 0 and used_in + used_out + est_in + est_out > lim_total then return {0, 'total', 0} end
  return {0, 'busy', 0}
end
local grant = head

if rl_commit then rl_commit() end
redis.call('HINCRBY', KEYS[2], 'rin', est_in)
redis.call('HINCRBY', KEYS[2], 'rout', grant)
redis.call('ZADD', KEYS[3], now + resv_ttl, rid)
redis.call('HSET', KEYS[4], rid, est_in .. ':' .. grant)
for i = 2, 4 do redis.call('EXPIRE', KEYS[i], key_ttl) end
return {1, grant, used_in + used_out, rin + rout + est_in + grant}
