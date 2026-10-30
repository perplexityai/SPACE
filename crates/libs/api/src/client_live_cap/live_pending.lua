-- The live cap's pending set: recent admissions whose pods the reflectors
-- may not have seen yet.
--
-- Every key shares the hash tag in KEYS[1], so one script can read every
-- client's set in a clustered Redis. Keys, with P = KEYS[1]:
--   P .. ':ids'                  ZSET  client id -> its latest member expiry
--                                      (the registry an `unknown` check reads)
--   P .. ':members:' .. id       ZSET  sandbox id -> expiry
--   P .. ':owners:' .. id        HASH  sandbox id -> owner tokens, ','-joined
--   P .. ':clients:' .. sandbox  SET   client ids holding a member for it
--
-- A member counts while its expiry is after the check's cutoff. Pruning
-- deletes members and registry entries only `grace` seconds after they
-- expire, so pruning never removes a member a check read before the same
-- check's reserve runs; a reserve whose cutoff is older than the grace
-- refuses to decide. A forget, or the last owner's release, still deletes a
-- member at once.
--
-- ARGV[1] is the operation; the rest depends on it (see each branch).
-- Times are the Redis server clock (`TIME`), so replicas never disagree.

local prefix = KEYS[1]
local op = ARGV[1]

local now_parts = redis.call('TIME')
local now = tonumber(now_parts[1]) + tonumber(now_parts[2]) / 1000000.0

local function ids_key() return prefix .. ':ids' end
local function members_key(id) return prefix .. ':members:' .. id end
local function owners_key(id) return prefix .. ':owners:' .. id end
local function clients_key(sandbox) return prefix .. ':clients:' .. sandbox end

local function split(list)
  local out = {}
  if list then
    for token in string.gmatch(list, '[^,]+') do
      table.insert(out, token)
    end
  end
  return out
end

-- Keys outlive their newest member by the grace plus a margin, so an
-- abandoned key goes away on its own.
local function touch(key, ttl, grace)
  redis.call('EXPIRE', key, math.ceil(ttl + grace + 60))
end

-- Drop `sandbox`'s member from `id`'s set, and the registry entry once the
-- set is empty.
local function drop_member(id, sandbox)
  redis.call('ZREM', members_key(id), sandbox)
  redis.call('HDEL', owners_key(id), sandbox)
  redis.call('SREM', clients_key(sandbox), id)
  if redis.call('ZCARD', members_key(id)) == 0 then
    redis.call('ZREM', ids_key(), id)
    redis.call('DEL', members_key(id), owners_key(id))
  end
end

-- Delete `id`'s members that expired before `horizon`.
local function prune(id, horizon)
  local expired = redis.call('ZRANGEBYSCORE', members_key(id), '-inf', '(' .. horizon)
  for _, sandbox in ipairs(expired) do
    drop_member(id, sandbox)
  end
end

local function prune_registry(horizon)
  local expired = redis.call('ZRANGEBYSCORE', ids_key(), '-inf', '(' .. horizon)
  for _, id in ipairs(expired) do
    prune(id, horizon)
    redis.call('ZREM', ids_key(), id)
  end
end

-- Add `owner` to `id`'s member for `sandbox`, expiring at `expiry`.
local function add_member(id, sandbox, owner, expiry, ttl, grace)
  redis.call('ZADD', members_key(id), expiry, sandbox)
  local owners = redis.call('HGET', owners_key(id), sandbox)
  local present = false
  for _, token in ipairs(split(owners)) do
    if token == owner then present = true end
  end
  if not present then
    if owners and owners ~= '' then
      owners = owners .. ',' .. owner
    else
      owners = owner
    end
    redis.call('HSET', owners_key(id), sandbox, owners)
  end
  redis.call('SADD', clients_key(sandbox), id)
  local registered = tonumber(redis.call('ZSCORE', ids_key(), id))
  if not registered or registered < expiry then
    redis.call('ZADD', ids_key(), expiry, id)
  end
  touch(members_key(id), ttl, grace)
  touch(owners_key(id), ttl, grace)
  touch(clients_key(sandbox), ttl, grace)
  touch(ids_key(), ttl, grace)
end

local function owns(id, sandbox, owner)
  for _, token in ipairs(split(redis.call('HGET', owners_key(id), sandbox))) do
    if token == owner then return true end
  end
  return false
end

-- Whether `owner` holds a member for `sandbox` that's still live. A lapsed
-- member keeps its owners until it's pruned, but its slot may have gone to a
-- sibling, so renewing or moving it would put the client over its cap.
local function holds_live(id, sandbox, owner)
  local expiry = tonumber(redis.call('ZSCORE', members_key(id), sandbox))
  return expiry ~= nil and expiry > now and owns(id, sandbox, owner)
end

-- Remove `owner` from `id`'s member for `sandbox`; the member goes with its
-- last owner. Returns the owners left.
local function release(id, sandbox, owner)
  local kept = {}
  for _, token in ipairs(split(redis.call('HGET', owners_key(id), sandbox))) do
    if token ~= owner then table.insert(kept, token) end
  end
  if #kept == 0 then
    drop_member(id, sandbox)
  else
    redis.call('HSET', owners_key(id), sandbox, table.concat(kept, ','))
  end
  return #kept
end

-- The ids a check selects. `mode` is 'exactly' (the listed ids) or
-- 'all_except' (every registered id with a member live at `cutoff`, except
-- the listed ids and `native`).
local function selected_ids(mode, listed, cutoff)
  if mode == 'exactly' then
    return listed
  end
  local excluded = { native = true }
  for _, id in ipairs(listed) do excluded[id] = true end
  local out = {}
  for _, id in ipairs(redis.call('ZRANGEBYSCORE', ids_key(), '(' .. cutoff, '+inf')) do
    if not excluded[id] then table.insert(out, id) end
  end
  return out
end

-- The distinct sandboxes the selected ids hold members for that are live at
-- `cutoff`, other than `skip`.
local function pending_members(ids, cutoff, skip)
  local seen = {}
  local out = {}
  for _, id in ipairs(ids) do
    for _, sandbox in ipairs(redis.call('ZRANGEBYSCORE', members_key(id), '(' .. cutoff, '+inf')) do
      if sandbox ~= skip and not seen[sandbox] then
        seen[sandbox] = true
        table.insert(out, sandbox)
      end
    end
  end
  return out
end

local function fmt(t) return string.format('%.6f', t) end

if op == 'read' then
  -- ARGV: mode, listed ids (','-joined). Returns { cutoff, sandbox... }.
  local ids = selected_ids(ARGV[2], split(ARGV[3]), now)
  local out = { fmt(now) }
  for _, sandbox in ipairs(pending_members(ids, now, '')) do
    table.insert(out, sandbox)
  end
  return out
end

if op == 'reserve' then
  -- ARGV: mode, listed ids, cutoff, client id, sandbox, owner, cap,
  -- visible count, ttl, grace, the sandboxes the read returned (','-joined).
  -- Returns { verdict, total }: 'admitted', 'renewed', 'refused', 'retry'
  -- (the read missed members that would decide a refusal) or 'stale' (the
  -- cutoff is older than the grace).
  local mode, listed = ARGV[2], split(ARGV[3])
  local cutoff = tonumber(ARGV[4])
  local client, sandbox, owner = ARGV[5], ARGV[6], ARGV[7]
  local cap, visible = tonumber(ARGV[8]), tonumber(ARGV[9])
  local ttl, grace = tonumber(ARGV[10]), tonumber(ARGV[11])
  if now - cutoff > grace then
    return { 'stale', 0 }
  end
  prune(client, now - grace)
  prune_registry(now - grace)
  -- Renew only a member still live now: one that lapsed after the read may
  -- have freed the slot a sibling has since taken, so it rechecks the cap.
  local own = tonumber(redis.call('ZSCORE', members_key(client), sandbox))
  if own and own > now then
    add_member(client, sandbox, owner, now + ttl, ttl, grace)
    return { 'renewed', 0 }
  end
  local pending = pending_members(selected_ids(mode, listed, cutoff), cutoff, sandbox)
  local total = visible + #pending
  if total < cap then
    add_member(client, sandbox, owner, now + ttl, ttl, grace)
    return { 'admitted', total + 1 }
  end
  local read = {}
  for _, member in ipairs(split(ARGV[12])) do read[member] = true end
  for _, member in ipairs(pending) do
    if not read[member] then
      return { 'retry', total }
    end
  end
  return { 'refused', total }
end

if op == 'renew' then
  -- ARGV: client id, sandbox, owner, ttl, grace. Returns 1 when renewed, 0
  -- when the owner no longer holds a live member (released, forgotten or
  -- lapsed).
  local client, sandbox, owner = ARGV[2], ARGV[3], ARGV[4]
  local ttl, grace = tonumber(ARGV[5]), tonumber(ARGV[6])
  if not holds_live(client, sandbox, owner) then
    return 0
  end
  add_member(client, sandbox, owner, now + ttl, ttl, grace)
  return 1
end

if op == 'release' then
  -- ARGV: client id, sandbox, owner. Returns the owners left.
  return release(ARGV[2], ARGV[3], ARGV[4])
end

if op == 'swap' then
  -- ARGV: client id, from sandbox, to sandbox, owner, ttl, grace. Moves the
  -- owner's member without a cap check: the create already reserved it.
  -- Returns 1 when moved, 0 when the owner no longer held a live `from`.
  local client, from, to, owner = ARGV[2], ARGV[3], ARGV[4], ARGV[5]
  local ttl, grace = tonumber(ARGV[6]), tonumber(ARGV[7])
  if not holds_live(client, from, owner) then
    return 0
  end
  release(client, from, owner)
  add_member(client, to, owner, now + ttl, ttl, grace)
  return 1
end

if op == 'forget' then
  -- ARGV: sandbox. Removes its member from every client's set. Returns the
  -- number of sets it was removed from.
  local sandbox = ARGV[2]
  local ids = redis.call('SMEMBERS', clients_key(sandbox))
  for _, id in ipairs(ids) do
    drop_member(id, sandbox)
  end
  redis.call('DEL', clients_key(sandbox))
  return #ids
end

return redis.error_reply('unknown live pending operation: ' .. tostring(op))
