-- The runtime upgrade attempt a row belongs to, as an opaque id: on the live
-- row in `sandbox_runtime_upgrades` and on the operation's recorded outcome
-- in `sandbox_runtime_upgrade_outcomes`. The column is nullable, and NULL
-- names no attempt: every row written before it existed is NULL, as is any
-- row a release without this column inserts. That release's updates and
-- upserts leave an existing value in place, outcome changes included.
ALTER TABLE sandbox_runtime_upgrades ADD COLUMN attempt_id TEXT;
ALTER TABLE sandbox_runtime_upgrade_outcomes ADD COLUMN attempt_id TEXT;
