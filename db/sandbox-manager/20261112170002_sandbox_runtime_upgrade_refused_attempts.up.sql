-- Attempts a runtime upgrade into the started lifecycle refused after the
-- operation reached its target: the node upgraded the sandbox, or found it
-- there already, but answered the attempt's `UpgradeRuntime` with an error,
-- so the sandbox doesn't run for that attempt. A refusal recorded for an
-- attempt is final for that attempt, as a rollback is: its retries, under the
-- same attempt id, get the recorded failure back and never activate the
-- sandbox, while a new attempt tries again. `GetRuntimeUpgrade` reports the
-- latest one, so a caller waiting on an attempt settles on a refusal that
-- names it.
CREATE TABLE sandbox_runtime_upgrade_refused_attempts (
    operation_id TEXT NOT NULL,
    attempt_id TEXT NOT NULL,
    sandbox_id TEXT NOT NULL REFERENCES sandboxes(id) ON DELETE CASCADE,
    failure TEXT,
    recorded_at TEXT NOT NULL,
    PRIMARY KEY (operation_id, attempt_id)
);

CREATE INDEX sandbox_runtime_upgrade_refused_attempts_by_sandbox
    ON sandbox_runtime_upgrade_refused_attempts (sandbox_id);
