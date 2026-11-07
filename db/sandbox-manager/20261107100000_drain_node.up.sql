-- The node drain's own state on the node.
--
-- `activation_seal.last_cancelled_epoch` is the highest epoch a drain has
-- cancelled. Epochs only move forward: a seal under it or below it is
-- refused, so a drain call that arrives after its own undrain cannot seal the
-- node again.
ALTER TABLE activation_seal ADD COLUMN last_cancelled_epoch INTEGER CHECK (
    last_cancelled_epoch IS NULL
    OR (typeof(last_cancelled_epoch) = 'integer' AND last_cancelled_epoch > 0)
);

-- One row per resident the node evacuated (or is evacuating) for a drain:
-- the lifecycle it had (`prior`), which the control plane restores it to,
-- and once it is durable elsewhere, the artifact that holds it. Kept until
-- the drain's epoch is cancelled, so the drain's report survives a restart
-- of sandbox-manager.
CREATE TABLE drain_evacuations (
    sandbox_id TEXT PRIMARY KEY NOT NULL,
    epoch INTEGER NOT NULL CHECK (typeof(epoch) = 'integer' AND epoch > 0),
    prior TEXT NOT NULL CHECK (prior IN ('started', 'paused', 'stopped')),
    -- NULL until the artifact is durable.
    artifact_id TEXT,
    recorded_at_ms INTEGER NOT NULL CHECK (
        typeof(recorded_at_ms) = 'integer' AND recorded_at_ms >= 0
    )
);
