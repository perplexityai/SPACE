-- Pods whose create a runtime upgrade into the started lifecycle refused
-- after the operation reached its target: the node upgraded the sandbox, or
-- found it there already, but answered the pod's `UpgradeRuntime` with an
-- error, so the sandbox doesn't run for that pod. A refusal recorded for a pod
-- is final for that pod, as a rollback is: kubelet's retries of its create,
-- under the same pod UID, get the recorded failure back and never activate
-- the sandbox, while a retry from a new pod tries again. `GetRuntimeUpgrade`
-- reports the latest one, so a control plane waiting on a pod settles on a
-- refusal that names it.
CREATE TABLE sandbox_runtime_upgrade_refused_pods (
    operation_id TEXT NOT NULL,
    pod_uid TEXT NOT NULL,
    sandbox_id TEXT NOT NULL REFERENCES sandboxes(id) ON DELETE CASCADE,
    failure TEXT,
    recorded_at TEXT NOT NULL,
    PRIMARY KEY (operation_id, pod_uid)
);

CREATE INDEX sandbox_runtime_upgrade_refused_pods_by_sandbox
    ON sandbox_runtime_upgrade_refused_pods (sandbox_id);
