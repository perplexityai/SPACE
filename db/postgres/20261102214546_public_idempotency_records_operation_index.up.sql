-- no-transaction
-- Index the public_idempotency_records -> public_operations foreign key.
-- Deleting an operation runs the ON DELETE SET NULL action against
-- public_idempotency_records, and without this index each deleted operation
-- scans the whole table. With ~1M idempotency rows, public-operation retention
-- batches exceed the 10s statement timeout, roll back, and never delete
-- anything, so public_operations and public_operation_events grow unbounded.
--
-- CONCURRENTLY because idempotency records are written on every keyed create
-- and ECW delete; a plain build holds SHARE on the table and would stall those
-- writes. That makes this a single-statement `-- no-transaction` migration,
-- which the migrator does not retry. A failed build can leave an INVALID index
-- behind; drop it with
-- `DROP INDEX CONCURRENTLY idx_public_idempotency_records_operation` and rerun.
-- IF NOT EXISTS does not repair an INVALID index, so drop it first.
--
-- The statement must be the file's only one, so it cannot set its own lock or
-- statement timeout and runs under the session's (see db/README.md). Building
-- concurrently takes SHARE UPDATE EXCLUSIVE, which does not block writers.
-- squawk-ignore require-lock-timeout, require-statement-timeout
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_public_idempotency_records_operation
    ON public_idempotency_records (operation_id)
    WHERE operation_id IS NOT NULL;
