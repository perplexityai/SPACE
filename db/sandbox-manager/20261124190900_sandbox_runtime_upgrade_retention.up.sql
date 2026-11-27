-- Tracks when a retained final attempt outcome or abandoned operation was last
-- written or matched by a retry or late call, so idle records can be pruned
-- once the sandbox and operation have left this node without changing
-- `recorded_at`. Rows written by an image-only rollback binary keep
-- `last_seen_at IS NULL` and fall back to `recorded_at`.
ALTER TABLE sandbox_runtime_upgrade_attempt_outcomes ADD COLUMN last_seen_at TEXT;
ALTER TABLE sandbox_runtime_upgrade_abandoned ADD COLUMN last_seen_at TEXT;

-- space-allow-rollback-unsafe: sandbox_runtime_upgrade_attempt_outcomes: data: older releases ignore last_seen_at and newer code falls back to recorded_at when last_seen_at is NULL
UPDATE sandbox_runtime_upgrade_attempt_outcomes
   SET last_seen_at = CAST(strftime('%s', 'now') AS TEXT)
 WHERE last_seen_at IS NULL;

-- space-allow-rollback-unsafe: sandbox_runtime_upgrade_abandoned: data: older releases ignore last_seen_at and newer code falls back to recorded_at when last_seen_at is NULL
UPDATE sandbox_runtime_upgrade_abandoned
   SET last_seen_at = CAST(strftime('%s', 'now') AS TEXT)
 WHERE last_seen_at IS NULL;
