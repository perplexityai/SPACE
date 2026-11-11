-- Cluster-wide per-client concurrency slot: atomic purge + count + admit.
--
-- One ZSET per client bucket holds the in-flight slots: member = a unique
-- per-request id, score = an absolute deadline (Redis server clock) after which
-- the slot is presumed abandoned (holder crashed / request timed out) and is
-- reclaimed. Every replica shares one ZSET, so the cap is cluster-wide.
--
-- KEYS[1] = bucket ZSET key.
-- ARGV[1] = cap (max concurrent slots). ARGV[2] = hold seconds (slot lifetime).
-- ARGV[3] = member id for this request. ARGV[4] = key TTL seconds.
-- ARGV[5] = force (1 = observe mode: add the slot even when over cap).
-- Uses the Redis server clock (`TIME`) so deadlines are skew-free across replicas.
-- Returns { over_cap (0|1), added (0|1), count (in-flight after this call) }.

local key = KEYS[1]
local cap = tonumber(ARGV[1])
local hold = tonumber(ARGV[2])
local member = ARGV[3]
local key_ttl = tonumber(ARGV[4])
local force = tonumber(ARGV[5])

local now_parts = redis.call('TIME')
local now = tonumber(now_parts[1]) + tonumber(now_parts[2]) / 1000000.0

-- Reclaim slots whose deadline has passed (crashed / timed-out holders).
redis.call('ZREMRANGEBYSCORE', key, '-inf', now)

local count = redis.call('ZCARD', key)
local over_cap = 0
if count >= cap then
  over_cap = 1
end

local added = 0
if over_cap == 0 or force == 1 then
  redis.call('ZADD', key, now + hold, member)
  added = 1
  count = count + 1
end

redis.call('EXPIRE', key, key_ttl)

return { over_cap, added, count }
