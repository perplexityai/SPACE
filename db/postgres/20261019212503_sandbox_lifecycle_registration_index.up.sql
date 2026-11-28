-- no-transaction
-- squawk-disable-assume-in-transaction
-- Single-statement nontransactional migration: use the migrator session's
-- lock/statement timeouts. A failed build can leave an INVALID index; repair it
-- with DROP INDEX CONCURRENTLY before retrying, never silently skip its name.
-- squawk-ignore prefer-robust-stmts,require-lock-timeout,require-statement-timeout
CREATE INDEX CONCURRENTLY idx_sandboxes_lifecycle_registration_enabled_id
    ON sandboxes ((lifecycle_registration ->> 'enabled'), id);
