-- The foreign key this restores can't hold a per-attempt row whose sandbox was
-- deleted, and dropping that row would let the attempt's delayed retry fence or
-- boot the sandbox again. Refuse the rollback while one is recorded.
CREATE TEMP TABLE runtime_upgrade_per_attempt_orphan_rollback_guard (count INTEGER CHECK (count = 0));
INSERT INTO runtime_upgrade_per_attempt_orphan_rollback_guard
    SELECT
        (SELECT COUNT(*) FROM sandbox_runtime_upgrade_refused_attempts
            WHERE sandbox_id NOT IN (SELECT id FROM sandboxes))
        + (SELECT COUNT(*) FROM sandbox_runtime_upgrade_attempt_outcomes
            WHERE sandbox_id NOT IN (SELECT id FROM sandboxes));
DROP TABLE runtime_upgrade_per_attempt_orphan_rollback_guard;

CREATE TABLE sandbox_runtime_upgrade_refused_attempts_bound (
    operation_id TEXT NOT NULL,
    attempt_id TEXT NOT NULL,
    sandbox_id TEXT NOT NULL REFERENCES sandboxes(id) ON DELETE CASCADE,
    failure TEXT,
    recorded_at TEXT NOT NULL,
    PRIMARY KEY (operation_id, attempt_id)
);
INSERT INTO sandbox_runtime_upgrade_refused_attempts_bound
    (operation_id, attempt_id, sandbox_id, failure, recorded_at)
    SELECT operation_id, attempt_id, sandbox_id, failure, recorded_at
    FROM sandbox_runtime_upgrade_refused_attempts
    ORDER BY rowid;
DROP INDEX sandbox_runtime_upgrade_refused_attempts_by_sandbox;
DROP TABLE sandbox_runtime_upgrade_refused_attempts;
ALTER TABLE sandbox_runtime_upgrade_refused_attempts_bound
    RENAME TO sandbox_runtime_upgrade_refused_attempts;
CREATE INDEX sandbox_runtime_upgrade_refused_attempts_by_sandbox
    ON sandbox_runtime_upgrade_refused_attempts (sandbox_id);

CREATE TABLE sandbox_runtime_upgrade_attempt_outcomes_bound (
    operation_id TEXT NOT NULL,
    attempt_id TEXT NOT NULL,
    sandbox_id TEXT NOT NULL REFERENCES sandboxes(id) ON DELETE CASCADE,
    outcome TEXT NOT NULL,
    failure TEXT,
    recorded_at TEXT NOT NULL,
    PRIMARY KEY (operation_id, attempt_id)
);
INSERT INTO sandbox_runtime_upgrade_attempt_outcomes_bound
    (operation_id, attempt_id, sandbox_id, outcome, failure, recorded_at)
    SELECT operation_id, attempt_id, sandbox_id, outcome, failure, recorded_at
    FROM sandbox_runtime_upgrade_attempt_outcomes
    ORDER BY rowid;
DROP INDEX sandbox_runtime_upgrade_attempt_outcomes_by_sandbox;
DROP TABLE sandbox_runtime_upgrade_attempt_outcomes;
ALTER TABLE sandbox_runtime_upgrade_attempt_outcomes_bound
    RENAME TO sandbox_runtime_upgrade_attempt_outcomes;
CREATE INDEX sandbox_runtime_upgrade_attempt_outcomes_by_sandbox
    ON sandbox_runtime_upgrade_attempt_outcomes (sandbox_id);
