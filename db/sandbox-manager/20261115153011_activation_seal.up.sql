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

-- The lifecycle (`started` or `paused`) each resident a drain suspended had,
-- which the control plane restores it to once its snapshot is durable.
-- Written under the sandbox's wake lock before the suspend, and kept until
-- the drain is cancelled or the sandbox's row is deleted, so the drain's
-- report survives a restart of sandbox-manager.
CREATE TABLE drain_evacuations (
    sandbox_id TEXT PRIMARY KEY NOT NULL,
    prior TEXT NOT NULL
);
