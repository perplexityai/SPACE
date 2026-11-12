-- Every attempt a runtime upgrade rolled back, or refused before it fenced
-- anything, with that outcome. `sandbox_runtime_upgrade_outcomes` keeps one
-- outcome per operation, the latest, so a later attempt's rollback or refusal
-- replaces an earlier attempt's there. Finality is per attempt: a retry with
-- the attempt id of a recorded rollback or refusal gets that outcome back and
-- never fences or boots again, whatever a later attempt recorded since. Only
-- refusals a retry can't change are recorded; a live row written before
-- attempts were recorded (NULL `attempt_id`) adds none.
CREATE TABLE sandbox_runtime_upgrade_attempt_outcomes (
    operation_id TEXT NOT NULL,
    attempt_id TEXT NOT NULL,
    sandbox_id TEXT NOT NULL REFERENCES sandboxes(id) ON DELETE CASCADE,
    outcome TEXT NOT NULL,
    failure TEXT,
    recorded_at TEXT NOT NULL,
    PRIMARY KEY (operation_id, attempt_id)
);

CREATE INDEX sandbox_runtime_upgrade_attempt_outcomes_by_sandbox
    ON sandbox_runtime_upgrade_attempt_outcomes (sandbox_id);
