-- Grow a live reservation mid-stream, or refresh its expiry (add = 0).
--
-- KEYS[1] balance hash, KEYS[2] reservation expiry zset, KEYS[3] reservation amounts hash
-- ARGV[1] reservation id
-- ARGV[2] add_out         extra output tokens wanted
-- ARGV[3] lim_out         0 = unlimited
-- ARGV[4] lim_total
-- ARGV[5] reservation ttl ms
--
-- Returns 1 when granted, 0 when the budget cannot cover it, -1 when the reservation is gone
-- (it expired and was reclaimed; the caller should stop relying on it).

local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local rid = ARGV[1]
local add = tonumber(ARGV[2])
local lim_out = tonumber(ARGV[3])
local lim_total = tonumber(ARGV[4])

local amt = redis.call('HGET', KEYS[3], rid)
if not amt then return -1 end
local sep = string.find(amt, ':', 1, true)
local r_in = tonumber(string.sub(amt, 1, sep - 1))
local r_out = tonumber(string.sub(amt, sep + 1))

if add > 0 then
  local b = redis.call('HMGET', KEYS[1], 'in', 'out', 'rin', 'rout')
  local used_in, used_out = tonumber(b[1]) or 0, tonumber(b[2]) or 0
  local rin, rout = tonumber(b[3]) or 0, tonumber(b[4]) or 0
  if lim_out > 0 and used_out + rout + add > lim_out then return 0 end
  if lim_total > 0 and used_in + used_out + rin + rout + add > lim_total then return 0 end
  redis.call('HINCRBY', KEYS[1], 'rout', add)
  redis.call('HSET', KEYS[3], rid, r_in .. ':' .. (r_out + add))
end
redis.call('ZADD', KEYS[2], now + tonumber(ARGV[5]), rid)
return 1
