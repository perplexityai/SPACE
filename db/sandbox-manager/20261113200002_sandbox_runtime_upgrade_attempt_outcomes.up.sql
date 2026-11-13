-- Each attempt's final outcome, at most one per attempt: `rolled_back`, its
-- upgrade rolled back; `refused`, refused before it fenced anything for a
-- reason no retry changes; or `activation_refused`, refused after the
-- operation reached its target (the node upgraded the sandbox, or found it
-- there already), so the sandbox doesn't run for that attempt.
-- `sandbox_runtime_upgrade_outcomes` keeps one outcome per operation, the
-- latest, so a later attempt's rollback or refusal replaces an earlier
-- attempt's there. Finality is per attempt: a retry with the attempt id of a
-- recorded outcome gets that outcome back and never fences, boots or
-- activates again, whatever a later attempt recorded since, while a new
-- attempt tries again. A row is never rewritten. A live row written before
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
