-- An attempt's final outcome (its rollback, its refusal before the fence, or
-- its activation refused after the operation reached its target) is final for
-- that attempt, and its delayed retry must get it back even after the
-- sandbox's row was deleted and staged again: evicting a stale suspended copy
-- deletes the row, and a later attempt restages it under the same id. Rebuild
-- the table without the foreign key, whose cascade would delete its rows with
-- the sandbox's, as `sandbox_runtime_upgrade_abandoned` already has none. The
-- `_by_sandbox` index stays, so the down migration restores the schema as it
-- found it.
CREATE TABLE sandbox_runtime_upgrade_attempt_outcomes_unbound (
    operation_id TEXT NOT NULL,
    attempt_id TEXT NOT NULL,
    sandbox_id TEXT NOT NULL,
    outcome TEXT NOT NULL,
    failure TEXT,
    recorded_at TEXT NOT NULL,
    PRIMARY KEY (operation_id, attempt_id)
);
INSERT INTO sandbox_runtime_upgrade_attempt_outcomes_unbound
    (operation_id, attempt_id, sandbox_id, outcome, failure, recorded_at)
    SELECT operation_id, attempt_id, sandbox_id, outcome, failure, recorded_at
    FROM sandbox_runtime_upgrade_attempt_outcomes
    ORDER BY rowid;
DROP INDEX sandbox_runtime_upgrade_attempt_outcomes_by_sandbox;
DROP TABLE sandbox_runtime_upgrade_attempt_outcomes;
ALTER TABLE sandbox_runtime_upgrade_attempt_outcomes_unbound
    RENAME TO sandbox_runtime_upgrade_attempt_outcomes;
CREATE INDEX sandbox_runtime_upgrade_attempt_outcomes_by_sandbox
    ON sandbox_runtime_upgrade_attempt_outcomes (sandbox_id);
