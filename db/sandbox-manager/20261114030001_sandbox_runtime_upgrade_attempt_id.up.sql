-- The attempt each runtime upgrade ran as: the opaque `attempt_id` its
-- `UpgradeRuntime` request named, or the id the node generated for one that
-- named none. Kept with the live row and with the recorded outcome. A
-- rollback or refusal recorded for an attempt is final for that attempt: a
-- retry with the same attempt id gets the recorded outcome back and never
-- fences or boots again, while a new attempt starts over. NULL for every row
-- written before this column existed, which therefore proves nothing about
-- any attempt.
ALTER TABLE sandbox_runtime_upgrades ADD COLUMN attempt_id TEXT;
ALTER TABLE sandbox_runtime_upgrade_outcomes ADD COLUMN attempt_id TEXT;
