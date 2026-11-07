-- The Kubernetes pod each runtime upgrade ran for: the pod UID the
-- `UpgradeRuntime` request's start named (`route_identity.pod_uid`), kept
-- with the live row and with the recorded outcome. A rollback or refusal
-- recorded for a pod is final for that pod: a retry from the same pod UID
-- gets the recorded outcome back and never fences or boots again, while a
-- retry from another pod, or one naming none, starts over. NULL when the
-- request named no pod, and for every row written before this column
-- existed, which therefore proves nothing about any pod.
ALTER TABLE sandbox_runtime_upgrades ADD COLUMN pod_uid TEXT;
ALTER TABLE sandbox_runtime_upgrade_outcomes ADD COLUMN pod_uid TEXT;
