-- Every runtime upgrade operation the control plane gave up on this node.
-- A Kubernetes pod's shim calls `UpgradeRuntime` from its container create,
-- and that call can still be on its way after the pod is gone, so before the
-- control plane captures the sandbox's disk it abandons the operation here:
-- no later call for it fences or boots, from any pod. The row outlives the
-- operation's own records, since a call can arrive after those settle, and
-- the sandbox's row too: a late call can stage a sandbox the node doesn't
-- hold, so the abandonment can't depend on it.
CREATE TABLE sandbox_runtime_upgrade_abandoned (
    operation_id TEXT PRIMARY KEY,
    sandbox_id TEXT NOT NULL,
    recorded_at TEXT NOT NULL
);
