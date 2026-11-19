-- The live cap's pending set: recent admissions whose pods the reflectors
-- may not have seen yet.
--
-- Every key shares the hash tag in KEYS[1], so one script can read every
-- client's set in a clustered Redis. Keys, with P = KEYS[1]:
--   P .. ':ids'                  ZSET  client id -> its latest entry expiry
--                                      (the registry an `unknown` check reads)
--   P .. ':members:' .. id       ZSET  `sandbox|owner` -> expiry
--   P .. ':clients:' .. sandbox  SET   client ids holding an entry for it
--
-- Each reservation is its own entry, keyed by its sandbox and its owner
-- token, with its own expiry: renewing, keeping, releasing or forgetting one
-- never touches another's. A sandbox counts once while any of its entries is
-- live at the check's cutoff. Pruning deletes entries and registry entries
-- only `grace` seconds after they expire, so pruning never removes an entry a
-- check read before the same check's reserve runs; a reserve whose cutoff is
-- older than the grace refuses to decide. A release, or a forget of the
-- entries a teardown snapshotted, still deletes them at once.
--
-- ARGV[1] is the operation; the rest depends on it (see each branch).
-- Times are the Redis server clock (`TIME`), so replicas never disagree.

local prefix = KEYS[1]
local op = ARGV[1]

local now_parts = redis.call('TIME')
local now = tonumber(now_parts[1]) + tonumber(now_parts[2]) / 1000000.0

local function ids_key() return prefix .. ':ids' end
local function members_key(id) return prefix .. ':members:' .. id end
local function clients_key(sandbox) return prefix .. ':clients:' .. sandbox end

local function entry(sandbox, owner) return sandbox .. '|' .. owner end
local function entry_sandbox(e) return string.match(e, '^([^|]*)|') end
local function entry_owner(e) return string.match(e, '|(.*)$') end

local function split(list)
  local out = {}
  if list then
    for token in string.gmatch(list, '[^,]+') do
      table.insert(out, token)
    end
  end
  return out
end

-- Keys outlive their newest entry by the grace plus a margin, so an
-- abandoned key goes away on its own.
local function touch(key, ttl, grace)
  redis.call('EXPIRE', key, math.ceil(ttl + grace + 60))
end

-- Whether `id` holds an entry for `sandbox` whose expiry is after `after`
-- (any entry, when `after` is nil).
local function holds_any(id, sandbox, after)
  local from = '-inf'
  if after then from = '(' .. after end
  for _, e in ipairs(redis.call('ZRANGEBYSCORE', members_key(id), from, '+inf')) do
    if entry_sandbox(e) == sandbox then return true end
  end
  return false
end

-- Drop `entries` from `id`'s set, then, once per sandbox they were for, the
-- client from that sandbox's set if it holds no other entry for it, and the
-- registry entry once the set is empty. Removing them all before checking
-- keeps a sandbox with many owners to one scan of the set, not one per owner.
local function drop_entries(id, entries)
  if #entries == 0 then return end
  local touched = {}
  for _, e in ipairs(entries) do
    redis.call('ZREM', members_key(id), e)
    touched[entry_sandbox(e)] = true
  end
  for sandbox in pairs(touched) do
    if not holds_any(id, sandbox, nil) then
      redis.call('SREM', clients_key(sandbox), id)
    end
  end
  if redis.call('ZCARD', members_key(id)) == 0 then
    redis.call('ZREM', ids_key(), id)
    redis.call('DEL', members_key(id))
  end
end

-- Delete `id`'s entries that expired before `horizon`.
local function prune(id, horizon)
  drop_entries(id, redis.call('ZRANGEBYSCORE', members_key(id), '-inf', '(' .. horizon))
end

local function prune_registry(horizon)
  local expired = redis.call('ZRANGEBYSCORE', ids_key(), '-inf', '(' .. horizon)
  for _, id in ipairs(expired) do
    prune(id, horizon)
    redis.call('ZREM', ids_key(), id)
  end
end

-- Set `owner`'s entry for `sandbox` in `id`'s set to expire at `expiry`.
local function add_member(id, sandbox, owner, expiry, ttl, grace)
  redis.call('ZADD', members_key(id), expiry, entry(sandbox, owner))
  redis.call('SADD', clients_key(sandbox), id)
  local registered = tonumber(redis.call('ZSCORE', ids_key(), id))
  if not registered or registered < expiry then
    redis.call('ZADD', ids_key(), expiry, id)
  end
  touch(members_key(id), ttl, grace)
  touch(clients_key(sandbox), ttl, grace)
  touch(ids_key(), ttl, grace)
end

-- Whether `owner`'s entry for `sandbox` is still live. A lapsed entry stays
-- until it's pruned, but its slot may have gone to a sibling, so renewing or
-- moving it would put the client over its cap.
local function holds_live(id, sandbox, owner)
  local expiry = tonumber(redis.call('ZSCORE', members_key(id), entry(sandbox, owner)))
  return expiry ~= nil and expiry > now
end

-- Remove `owner`'s entry for `sandbox`. Returns 1 when it was there.
local function release(id, sandbox, owner)
  local e = entry(sandbox, owner)
  if redis.call('ZSCORE', members_key(id), e) == false then
    return 0
  end
  drop_entries(id, { e })
  return 1
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
    for _, e in ipairs(redis.call('ZRANGEBYSCORE', members_key(id), '(' .. cutoff, '+inf')) do
      local sandbox = entry_sandbox(e)
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
  -- Join a sandbox that already counts: another attempt's entry for it is
  -- still live now. One that lapsed after the read may have freed the slot a
  -- sibling has since taken, so it rechecks the cap.
  if holds_any(client, sandbox, now) then
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

if op == 'owners' then
  -- ARGV: sandbox. Returns its entries as `client|owner`, the snapshot a
  -- teardown takes before its substrate call and forgets after it.
  local sandbox = ARGV[2]
  local out = {}
  for _, id in ipairs(redis.call('SMEMBERS', clients_key(sandbox))) do
    for _, e in ipairs(redis.call('ZRANGE', members_key(id), 0, -1)) do
      if entry_sandbox(e) == sandbox then
        table.insert(out, id .. '|' .. entry_owner(e))
      end
    end
  end
  return out
end

if op == 'forget' then
  -- ARGV: sandbox, the `client|owner` pairs an `owners` snapshot returned
  -- (','-joined). Removes exactly those entries, so an entry added after the
  -- snapshot (a wake that reserved once the teardown committed) stays.
  -- Returns the number removed.
  local sandbox = ARGV[2]
  local by_client = {}
  for _, pair in ipairs(split(ARGV[3])) do
    local id, owner = string.match(pair, '^([^|]*)|(.*)$')
    if id then
      local e = entry(sandbox, owner)
      if redis.call('ZSCORE', members_key(id), e) ~= false then
        by_client[id] = by_client[id] or {}
        table.insert(by_client[id], e)
      end
    end
  end
  local removed = 0
  for id, entries in pairs(by_client) do
    drop_entries(id, entries)
    removed = removed + #entries
  end
  return removed
end

return redis.error_reply('unknown live pending operation: ' .. tostring(op))
