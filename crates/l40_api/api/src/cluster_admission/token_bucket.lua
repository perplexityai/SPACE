-- Cluster-wide sandbox-activation token bucket: atomic refill + consume + TTL.
--
-- KEYS[1] = bucket key.
-- ARGV[1] = refill rate (tokens/sec). ARGV[2] = burst (bucket capacity).
-- ARGV[3] = tokens requested by this call.
-- Uses the Redis server clock (`TIME`) so all replicas share one timeline.
-- Returns { allowed (0|1), remaining tokens as a decimal string }.

local key = KEYS[1]
local rate = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])
local requested = tonumber(ARGV[3])

local now_parts = redis.call('TIME')
local now = tonumber(now_parts[1]) + tonumber(now_parts[2]) / 1000000.0

local bucket = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(bucket[1])
local ts = tonumber(bucket[2])
if tokens == nil or ts == nil then
  tokens = burst
  ts = now
end

local elapsed = now - ts
if elapsed < 0 then
  elapsed = 0
end
tokens = math.min(burst, tokens + elapsed * rate)

local allowed = 0
if tokens >= requested then
  tokens = tokens - requested
  allowed = 1
end

redis.call('HSET', key, 'tokens', tokens, 'ts', now)
local ttl = math.ceil(burst / rate) + 10
redis.call('EXPIRE', key, ttl)

return { allowed, tostring(tokens) }
