-- Every pod a runtime upgrade rolled back or refused before it fenced
-- anything, with that outcome. `sandbox_runtime_upgrade_outcomes` keeps one
-- outcome per operation, the latest, so a later pod's rollback or refusal
-- replaces an earlier pod's there. Finality is per pod: kubelet retries a
-- failed create under the same pod UID, and a retry from a pod whose
-- rollback or refusal was recorded gets that outcome back and never fences
-- or boots again, whatever a later pod's attempt recorded since. Only
-- refusals a retry can't change are recorded; a pod named by no request
-- (NULL `pod_uid`) has no row.
CREATE TABLE sandbox_runtime_upgrade_pod_outcomes (
    operation_id TEXT NOT NULL,
    pod_uid TEXT NOT NULL,
    sandbox_id TEXT NOT NULL REFERENCES sandboxes(id) ON DELETE CASCADE,
    outcome TEXT NOT NULL,
    failure TEXT,
    recorded_at TEXT NOT NULL,
    PRIMARY KEY (operation_id, pod_uid)
);

CREATE INDEX sandbox_runtime_upgrade_pod_outcomes_by_sandbox
    ON sandbox_runtime_upgrade_pod_outcomes (sandbox_id);
