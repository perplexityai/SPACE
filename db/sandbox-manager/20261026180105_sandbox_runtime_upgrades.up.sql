-- One row per node-local runtime upgrade: a cold boot of a stopped or
-- suspended sandbox's disk under another execution environment, keeping the
-- old disk/checkpoint pair until the boot is verified.
--
-- `phase` moves fenced -> preserved -> booting -> verified -> retired, or to
-- rolled_back from any phase before verified. While a row for the sandbox is
-- fenced, preserved or booting, no other activation may claim the sandbox.
-- A verified row still owes the retirement of the old checkpoint; a
-- rolled_back row still owes the deletion of the preserved disk and is
-- deleted after it. Retired rows remain as the record of the authorization
-- each upgrade ran under. The application owns the phase vocabulary.
CREATE TABLE sandbox_runtime_upgrades (
    operation_id TEXT PRIMARY KEY,
    sandbox_id TEXT NOT NULL REFERENCES sandboxes(id) ON DELETE CASCADE,
    target_environment TEXT NOT NULL,
    target_state TEXT NOT NULL,
    authorization_id TEXT NOT NULL,
    authorization_reason TEXT NOT NULL,
    memory_loss_authorized INTEGER NOT NULL
        CHECK (memory_loss_authorized IN (0, 1)),
    source_state TEXT NOT NULL,
    -- The consumer a rollback restores: the saved one, or for an unbound
    -- sandbox the execution its disk records, which the boot overwrites.
    previous_execution_metadata_json TEXT NOT NULL,
    checkpoint_artifact_id TEXT,
    checkpoint_dir_identity TEXT,
    -- The sandbox execution the verified boot started; the only one the
    -- upgrade may stop.
    boot_execution_id TEXT,
    phase TEXT NOT NULL,
    failure TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX sandbox_runtime_upgrades_by_sandbox
    ON sandbox_runtime_upgrades (sandbox_id);
