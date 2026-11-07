-- The node's activation seal: while `sealed`, sandbox-manager admits no new
-- VM activation (cold boot, snapshot restore or resident resume). The service
-- reads this row before it serves, so a seal survives a restart. One row at
-- most; no row means the node was never sealed.
--
-- `epoch` is the drain epoch of the last seal or cancel, and only moves
-- forward. Once a drain is cancelled (`sealed` false), no seal at or below its
-- epoch is taken again, so a drain call that arrives after its own undrain
-- cannot seal the node again, and an epoch names one continuous seal.
CREATE TABLE activation_seal (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    epoch INTEGER NOT NULL CHECK (typeof(epoch) = 'integer' AND epoch > 0),
    sealed INTEGER NOT NULL CHECK (sealed IN (0, 1))
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
