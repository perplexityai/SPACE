-- Dropping `last_seen_at` keeps every retained attempt outcome and abandonment
-- (and its `recorded_at`, `recorded_seq`, and `rowid`) intact for the prior
-- release and earlier guarded downs.
ALTER TABLE sandbox_runtime_upgrade_abandoned DROP COLUMN last_seen_at;
ALTER TABLE sandbox_runtime_upgrade_attempt_outcomes DROP COLUMN last_seen_at;
