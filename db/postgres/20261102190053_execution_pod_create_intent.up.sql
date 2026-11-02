SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

-- A record, written before any pod is created for an execution, that a pod
-- create may have been sent for it.
--
-- `pod_materialized_at` and `pod_uid` are written only once the pod watcher
-- records a pod, so a NULL there can't prove an execution never ran: a pod
-- created and gone before the watcher saw it leaves no trace. A reader that
-- needs that proof, as node drain's pod-less give-up does for a stalled
-- recovery, reads these two columns instead.
--
-- `pod_create_intent_tracked` is set when the execution row is inserted by a
-- writer that stamps `pod_create_requested_at` before every pod create for
-- that row. The Kubernetes substrate stamps it in the same write that precedes
-- each create (the materialization claim, the cross-node re-drive's reset, the
-- cold create's intent write), and warm-pool adoption, which binds a pod that
-- already exists, stamps it on insert. Once set it's never cleared.
--
-- Only `pod_create_intent_tracked AND pod_create_requested_at IS NULL` proves
-- no pod was ever requested. Rows that predate the writers, and rows a writer
-- without them inserts, default to untracked, which reads as unknown.
--
-- A constant default on a new column changes the catalog only; no row is
-- rewritten.
ALTER TABLE sandbox_k8s_executions
    ADD COLUMN pod_create_intent_tracked BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN pod_create_requested_at TIMESTAMPTZ;
