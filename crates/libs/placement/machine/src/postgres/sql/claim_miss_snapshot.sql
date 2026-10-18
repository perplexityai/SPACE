-- Post-miss snapshot of the restore gate, read to classify a claim that
-- matched zero rows. Adapted from `fetch_restore_ownership_snapshot` in the
-- api substrate store: every deadline is rendered as an absolute epoch-ms
-- instant so the classification never mixes the database clock with the
-- process clock.

SELECT s.lifecycle,
       -- the lease deadline; no minted lease reads as already lapsed (NOW())
       CEIL(EXTRACT(EPOCH FROM COALESCE(s.restore_lease_expires_at, NOW())) * 1000)::BIGINT,
       -- how long a fresh active execution keeps ownership past the lease:
       -- its last transition plus the ownership TTL ($2); an execution in no
       -- ownable state holds nothing (NOW(): already lapsed)
       CEIL(EXTRACT(EPOCH FROM (
         CASE
           WHEN e.state IN ('creating', 'created', 'recovering', 'running')
           THEN e.updated_at + make_interval(secs => $2)
           ELSE NOW()
         END
       )) * 1000)::BIGINT,
       -- the cohort restore-backoff deadline (NULL when unthrottled)
       CEIL(EXTRACT(EPOCH FROM s.next_restore_attempt_at) * 1000)::BIGINT,
       -- whether the node-drain marker still admits an evacuation claim
       (s.evacuation_restore_target IS NOT NULL),
       -- whether the marker is a stopped evacuation's, and whether the row is
       -- still on a node: together they say whether a `stopped` row is the
       -- released stopped evacuation the evacuation claim admits
       (s.evacuation_restore_target IS NOT DISTINCT FROM 'stopped'),
       (s.active_node_id IS NOT NULL)
FROM sandboxes s
-- at most one execution row is active per sandbox
LEFT JOIN sandbox_k8s_executions e
  ON e.sandbox_id = s.id AND e.active
WHERE s.id = $1
