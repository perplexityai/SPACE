-- no-transaction
-- A sandbox's quarantine history, newest first, for the admin sandbox debug
-- view. The table is append-only and keyed only by its own id, so without this
-- index that read scans every sandbox's history.
--
-- CONCURRENTLY because the history rows are written inside node state
-- publication: a plain build holds SHARE on the table and would stall those
-- publications for its duration. That makes this a single-statement
-- `-- no-transaction` migration, which the migrator does not retry. A failed
-- build can leave an INVALID index behind; drop it with
-- `DROP INDEX CONCURRENTLY sandbox_quarantine_history_sandbox_occurred_idx`
-- and rerun. IF NOT EXISTS does not repair an INVALID index, so drop it first.
--
-- The statement must be the file's only one, so it cannot set its own lock or
-- statement timeout and runs under the session's (see db/README.md). Building
-- concurrently takes SHARE UPDATE EXCLUSIVE, which does not block writers.
-- squawk-ignore require-lock-timeout, require-statement-timeout
CREATE INDEX CONCURRENTLY IF NOT EXISTS sandbox_quarantine_history_sandbox_occurred_idx
    ON sandbox_quarantine_history (sandbox_id, occurred_at DESC);
