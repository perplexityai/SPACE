-- The lifecycle (`started` or `paused`) each resident a drain suspended had,
-- which the control plane restores it to once its snapshot is durable.
-- Written under the sandbox's wake lock before the suspend. A cancel keeps
-- it: a record is removed once a cancel call names its sandbox in `forget`
-- or finds nothing left to hand back, a caller's own suspend takes the
-- sandbox back, or the sandbox's row is deleted, so the drain's report
-- survives a restart of sandbox-manager.
CREATE TABLE drain_evacuations (
    sandbox_id TEXT PRIMARY KEY NOT NULL,
    prior TEXT NOT NULL
);
