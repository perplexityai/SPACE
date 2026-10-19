SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

-- A set-once record that an execution materialized a pod.
--
-- `pod_uid` is current state: pod re-materialization and failed-recovery pod
-- absence clear it on an active row whose pod may have run, and a cross-node
-- re-drive's reset clears it for a deleted pod. A reader asking whether an
-- execution ever ran a pod, as the teardown-suspend backup fence asks of every
-- execution newer than the capture's, can't answer from it.
--
-- The column is written by the Kubernetes substrate's execution store, in the
-- same statements that record or clear a `pod_uid`: it's stamped when a
-- non-blank `pod_uid` is recorded, or is about to be cleared, and once set it's
-- never cleared or moved. Rows that predate the writers stay NULL until a
-- bounded backfill stamps every row still carrying a `pod_uid`; readers treat
-- a non-blank `pod_uid` as materialized as well, so a NULL reads as it did
-- before this column existed.
--
-- Adding a nullable column without a default changes the catalog only; no row
-- is rewritten.
ALTER TABLE sandbox_k8s_executions
    ADD COLUMN pod_materialized_at TIMESTAMPTZ;
