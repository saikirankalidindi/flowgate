-- Release a reservation and charge what the request really used.
-- Charging happens even if the reservation already expired, so accounting stays exact.
--
-- KEYS[1] balance hash, KEYS[2] reservation expiry zset, KEYS[3] reservation amounts hash
-- ARGV[1] reservation id ("" in posthoc mode)
-- ARGV[2] actual input tokens
-- ARGV[3] actual output tokens
-- ARGV[4] key ttl s
--
-- Returns {used_in, used_out}

local rid = ARGV[1]
if rid ~= '' then
  local amt = redis.call('HGET', KEYS[3], rid)
  if amt then
    local sep = string.find(amt, ':', 1, true)
    redis.call('HINCRBY', KEYS[1], 'rin', -tonumber(string.sub(amt, 1, sep - 1)))
    redis.call('HINCRBY', KEYS[1], 'rout', -tonumber(string.sub(amt, sep + 1)))
    redis.call('HDEL', KEYS[3], rid)
    redis.call('ZREM', KEYS[2], rid)
  end
end
local used_in = redis.call('HINCRBY', KEYS[1], 'in', tonumber(ARGV[2]))
local used_out = redis.call('HINCRBY', KEYS[1], 'out', tonumber(ARGV[3]))
redis.call('EXPIRE', KEYS[1], tonumber(ARGV[4]))
return {used_in, used_out}
