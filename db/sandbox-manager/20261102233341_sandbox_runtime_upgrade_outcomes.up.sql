-- The terminal outcome of a runtime upgrade that leaves no row in
-- `sandbox_runtime_upgrades`: one rolled back, whose row is deleted once its
-- preserved disk is gone, or one that found the sandbox already on its
-- target and fenced nothing. `GetRuntimeUpgrade` reads it so a caller that
-- lost the `UpgradeRuntime` response, such as a control plane waiting on a
-- pod, can tell either apart from an operation the node never saw. A live
-- row for the same operation, from a retry that started over, supersedes it.
-- The application owns the outcome vocabulary.
CREATE TABLE sandbox_runtime_upgrade_outcomes (
    operation_id TEXT PRIMARY KEY,
    sandbox_id TEXT NOT NULL REFERENCES sandboxes(id) ON DELETE CASCADE,
    outcome TEXT NOT NULL,
    -- What the operation asked for, so a retry that names something else
    -- under the same operation is refused, as for a live row.
    target_environment TEXT NOT NULL,
    target_state TEXT NOT NULL,
    authorization_id TEXT NOT NULL,
    failure TEXT,
    recorded_at TEXT NOT NULL,
    -- When an already-on-target upgrade into the started lifecycle claimed
    -- its activation, and how. `start`: it claimed the start of the
    -- sandbox, written before the start runs, with the node execution the
    -- sandbox had then; a retry of the operation starts it again only while
    -- that execution is still the sandbox's, so nothing has started it
    -- since. `attach`: it answered with the execution already running, and
    -- no retry of the operation ever starts the sandbox. A replay of a
    -- retired upgrade never moves its sandbox either. A later outcome
    -- recorded for the operation keeps the claim.
    activation_claimed_at TEXT,
    activation_claim_kind TEXT,
    activation_claimed_execution_id TEXT
);

CREATE INDEX sandbox_runtime_upgrade_outcomes_by_sandbox
    ON sandbox_runtime_upgrade_outcomes (sandbox_id);
