-- Sandbox-activation token buckets: atomic refill + consume + TTL over one or
-- more buckets, all or nothing.
--
-- KEYS[i] = bucket key i (the cluster bucket, then the client bucket if any).
-- ARGV[1] = tokens requested from every bucket by this call.
-- ARGV[2i] = bucket i's refill rate (tokens/sec). ARGV[2i+1] = its burst.
-- Uses the Redis server clock (`TIME`) so all replicas share one timeline.
--
-- Takes `requested` from every bucket when each holds that many, and from none
-- otherwise: tokens aren't refundable, so consuming one bucket and then finding
-- the next one empty would spend a token on an activation that never ran.
-- Every bucket still records its refill on a rejection, which changes no
-- future answer while its rate and burst stay the same, because refill is a
-- function of elapsed time alone.
--
-- A key expires once it's gone untouched long enough to refill completely.
-- At a refill rate small enough, `burst / rate` is past what EXPIRE accepts
-- (up to infinity), and the script would fail after its writes, so the TTL is
-- capped at MAX_TTL. The cap is about 31,700 years rather than a policy
-- length: an expired key comes back full, so a shorter cap would hand a slow
-- bucket tokens its rate never refilled. It stays an exact integer when Lua
-- formats it, which EXPIRE requires.
--
-- Returns { allowed (0|1), bucket 1's tokens, bucket 2's tokens, ... }, the
-- token counts as decimal strings that parse back to the exact values the
-- script compared. On an admit they're what's left after the consume; on a
-- reject nothing was consumed, so a bucket is the empty one exactly when its
-- count is below `requested`.

local requested = tonumber(ARGV[1])
local MAX_TTL = 1000000000000

local now_parts = redis.call('TIME')
local now = tonumber(now_parts[1]) + tonumber(now_parts[2]) / 1000000.0

local tokens = {}
local ttls = {}
local allowed = 1
for i, key in ipairs(KEYS) do
  local rate = tonumber(ARGV[2 * i])
  local burst = tonumber(ARGV[2 * i + 1])

  local bucket = redis.call('HMGET', key, 'tokens', 'ts')
  local current = tonumber(bucket[1])
  local ts = tonumber(bucket[2])
  if current == nil or ts == nil then
    current = burst
    ts = now
  end

  local elapsed = now - ts
  if elapsed < 0 then
    elapsed = 0
  end
  current = math.min(burst, current + elapsed * rate)
  tokens[i] = current
  ttls[i] = math.min(math.ceil(burst / rate) + 10, MAX_TTL)
  if current < requested then
    allowed = 0
  end
end

local result = { allowed }
for i, key in ipairs(KEYS) do
  if allowed == 1 then
    tokens[i] = tokens[i] - requested
  end
  redis.call('HSET', key, 'tokens', tokens[i], 'ts', now)
  redis.call('EXPIRE', key, ttls[i])
  -- `%.17g` round-trips a double; `tostring` keeps 14 digits, which can
  -- print a short count such as 0.999999999999999 as "1".
  result[i + 1] = string.format('%.17g', tokens[i])
end

return result
