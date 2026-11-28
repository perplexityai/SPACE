//! Main's source-release and give-up statements, frozen as they rendered at
//! the base commit of the change that added the capture-order rules, from the
//! `pplx_space_sandbox_portability` and `pplx_space_sandbox_store` builders
//! as they were then. A rolled-back control plane, or an old cluster-controller
//! leader mid-deploy, runs exactly these, so they're copies, not calls.

/// `pplx_space_sandbox_store::durably_restorable`: the `DurablyRestorable`
/// answer a node's eviction asks for. `$1` is the sandbox.
pub(super) const DURABLY_RESTORABLE: &str = r#"SELECT COALESCE((
                     SELECT CASE
                                WHEN s.lifecycle = 'restoring' THEN EXISTS (
SELECT 1
FROM snapshots sn
JOIN snapshot_storage_locations ssl ON ssl.snapshot_id = sn.id
WHERE sn.sandbox_id = s.id
AND ssl.orphaned_at IS NULL
AND sn.type = 'full'
AND sn.state = 'ready'
AND ((
(s.lifecycle_changed_at IS NULL
OR sn.timestamp < s.lifecycle_changed_at)
AND (
s.placement_changed_at IS NULL
OR s.lifecycle_changed_at IS NULL
OR s.placement_changed_at >= s.lifecycle_changed_at
OR sn.timestamp >= s.placement_changed_at
)
AND (
s.current_local_artifact_id IS NULL
OR (sn.local_artifact_id IS NOT NULL
AND sn.artifact_kind = 'full')
)
)
AND NOT EXISTS (
SELECT 1
FROM snapshots newer
WHERE newer.sandbox_id = s.id
AND newer.type = 'full'
AND newer.timestamp > sn.timestamp
AND (
(s.lifecycle_changed_at IS NULL
OR newer.timestamp < s.lifecycle_changed_at)
AND (
s.placement_changed_at IS NULL
OR s.lifecycle_changed_at IS NULL
OR s.placement_changed_at >= s.lifecycle_changed_at
OR newer.timestamp >= s.placement_changed_at
)
AND (
s.current_local_artifact_id IS NULL
OR (newer.local_artifact_id IS NOT NULL
AND newer.artifact_kind = 'full')
)
)
AND NOT (
sn.local_artifact_id IS NOT NULL
AND newer.state = 'failed'
AND newer.local_artifact_id = sn.local_artifact_id
AND newer.artifact_kind IS NOT DISTINCT FROM sn.artifact_kind
AND COALESCE(newer.artifact_origin, 'suspend') =
COALESCE(sn.artifact_origin, 'suspend')
)
))
)
                                ELSE (EXISTS (
             SELECT 1
               FROM snapshots sn
               JOIN snapshot_storage_locations ssl ON ssl.snapshot_id = sn.id
              WHERE sn.sandbox_id = s.id
                AND sn.type = 'full'
                AND sn.state = 'ready'
                AND ssl.orphaned_at IS NULL
                AND (
                    (
s.current_local_artifact_id IS NOT NULL
AND sn.local_artifact_id IS NOT NULL
AND (
sn.local_artifact_id = s.current_local_artifact_id
AND sn.artifact_kind = s.current_local_artifact_kind
)
)
                    OR (
                        s.current_local_artifact_id IS NULL
                        AND (
                            (s.evacuation_restore_target IS NOT NULL AND NOT EXISTS (
SELECT 1
FROM snapshots newer
WHERE newer.sandbox_id = s.id
AND newer.type = 'full'
AND newer.timestamp > sn.timestamp
))
                            OR (
                                s.evacuation_restore_target IS NULL
                                AND (
                                    s.placement_changed_at IS NULL
                                    OR sn.timestamp >= s.placement_changed_at
                                )
                            )
                        )
                    )
                )
         )) AND ((
(s.current_local_artifact_id IS NOT NULL AND EXISTS (
SELECT 1
FROM snapshots sn
JOIN snapshot_storage_locations ssl ON ssl.snapshot_id = sn.id
WHERE sn.sandbox_id = s.id
AND ssl.orphaned_at IS NULL
AND (
s.current_local_artifact_id IS NOT NULL
AND sn.local_artifact_id IS NOT NULL
AND (
sn.local_artifact_id = s.current_local_artifact_id
AND sn.artifact_kind = s.current_local_artifact_kind
)
)
AND sn.type = 'full'
AND sn.state = 'ready'
))
OR (s.current_local_artifact_id IS NULL AND EXISTS (
SELECT 1
FROM snapshots sn
JOIN snapshot_storage_locations ssl ON ssl.snapshot_id = sn.id
WHERE sn.sandbox_id = s.id
AND ssl.orphaned_at IS NULL
AND sn.type = 'full'
AND sn.state = 'ready'
AND NOT EXISTS (
SELECT 1
FROM snapshots newer
WHERE newer.sandbox_id = s.id
AND newer.type = 'full'
AND newer.timestamp > sn.timestamp
)
))
))
                            END
                       FROM sandboxes s
                      WHERE s.id = $1
                 ), FALSE)"#;

/// `pplx_space_sandbox_store::drain_gate_blockers::count_on_node`'s statement:
/// the sandboxes that hold a draining node. `$1` is the node.
pub(super) const DRAIN_GATE: &str = r#"SELECT COUNT(*)::BIGINT
           FROM (
SELECT gate_blockers.sandbox_id
           FROM (
         SELECT s.target_node_id AS node_id, s.id AS sandbox_id
FROM sandboxes s
WHERE s.target_node_id = ANY(ARRAY[$1]::UUID[])
AND (
s.target_node_id = s.target_node_id
AND (
s.lifecycle = 'suspending'
OR (s.lifecycle = 'restoring'
AND s.evacuation_restore_target IS NULL)
OR (s.lifecycle = 'suspended' AND NOT EXISTS (
             SELECT 1
               FROM snapshots sn
               JOIN snapshot_storage_locations ssl ON ssl.snapshot_id = sn.id
              WHERE sn.sandbox_id = s.id
                AND sn.type = 'full'
                AND sn.state = 'ready'
                AND ssl.orphaned_at IS NULL
                AND (
                    (
s.current_local_artifact_id IS NOT NULL
AND sn.local_artifact_id IS NOT NULL
AND (
sn.local_artifact_id = s.current_local_artifact_id
AND sn.artifact_kind = s.current_local_artifact_kind
)
)
                    OR (
                        s.current_local_artifact_id IS NULL
                        AND (
                            (s.evacuation_restore_target IS NOT NULL AND NOT EXISTS (
SELECT 1
FROM snapshots newer
WHERE newer.sandbox_id = s.id
AND newer.type = 'full'
AND newer.timestamp > sn.timestamp
))
                            OR (
                                s.evacuation_restore_target IS NULL
                                AND (
                                    s.placement_changed_at IS NULL
                                    OR sn.timestamp >= s.placement_changed_at
                                )
                            )
                        )
                    )
                )
         ))
)
)
UNION
SELECT driver.source_node_id AS node_id, s.id AS sandbox_id
FROM snapshots driver
JOIN sandboxes s ON s.id = driver.sandbox_id
WHERE driver.source_node_id = ANY(ARRAY[$1]::UUID[])
AND driver.type = 'full'
AND driver.state = 'creating'
AND EXISTS (
SELECT 1
FROM snapshots sn
WHERE sn.sandbox_id = s.id
AND sn.source_node_id = driver.source_node_id
AND sn.type = 'full'
AND sn.state = 'creating'
AND (
(
s.current_local_artifact_id IS NOT NULL
AND sn.local_artifact_id IS NOT NULL
AND (
sn.local_artifact_id = s.current_local_artifact_id
AND sn.artifact_kind = s.current_local_artifact_kind
)
)
OR (
s.current_local_artifact_id IS NULL
AND (
s.placement_changed_at IS NULL
OR sn.timestamp >= s.placement_changed_at
)
)
)
)
AND NOT EXISTS (
             SELECT 1
               FROM snapshots sn
               JOIN snapshot_storage_locations ssl ON ssl.snapshot_id = sn.id
              WHERE sn.sandbox_id = s.id
                AND sn.type = 'full'
                AND sn.state = 'ready'
                AND ssl.orphaned_at IS NULL
                AND (
                    (
s.current_local_artifact_id IS NOT NULL
AND sn.local_artifact_id IS NOT NULL
AND (
sn.local_artifact_id = s.current_local_artifact_id
AND sn.artifact_kind = s.current_local_artifact_kind
)
)
                    OR (
                        s.current_local_artifact_id IS NULL
                        AND (
                            (s.evacuation_restore_target IS NOT NULL AND NOT EXISTS (
SELECT 1
FROM snapshots newer
WHERE newer.sandbox_id = s.id
AND newer.type = 'full'
AND newer.timestamp > sn.timestamp
))
                            OR (
                                s.evacuation_restore_target IS NULL
                                AND (
                                    s.placement_changed_at IS NULL
                                    OR sn.timestamp >= s.placement_changed_at
                                )
                            )
                        )
                    )
                )
         )
               ) gate_blockers
          WHERE gate_blockers.node_id = $1
               ) node_blockers"#;

/// `pplx_space_sandbox_store::drain::mark_sandbox_lost_query`: the drain's
/// give-up after `ArtifactMissing`. `$1` is the sandbox, `$2` the node.
pub(super) const MARK_SANDBOX_LOST: &str = r#"UPDATE sandboxes
            SET lifecycle = 'failed'
          WHERE id = $1
            AND lifecycle = 'suspended'
            AND target_node_id = $2
            AND NOT EXISTS (
             SELECT 1
               FROM snapshots sn
               JOIN snapshot_storage_locations ssl ON ssl.snapshot_id = sn.id
              WHERE sn.sandbox_id = sandboxes.id
                AND sn.type = 'full'
                AND sn.state = 'ready'
                AND ssl.orphaned_at IS NULL
                AND (
                    (
sandboxes.current_local_artifact_id IS NOT NULL
AND sn.local_artifact_id IS NOT NULL
AND (
sn.local_artifact_id = sandboxes.current_local_artifact_id
AND sn.artifact_kind = sandboxes.current_local_artifact_kind
)
)
                    OR (
                        sandboxes.current_local_artifact_id IS NULL
                        AND (
                            (sandboxes.evacuation_restore_target IS NOT NULL AND NOT EXISTS (
SELECT 1
FROM snapshots newer
WHERE newer.sandbox_id = sandboxes.id
AND newer.type = 'full'
AND newer.timestamp > sn.timestamp
))
                            OR (
                                sandboxes.evacuation_restore_target IS NULL
                                AND (
                                    sandboxes.placement_changed_at IS NULL
                                    OR sn.timestamp >= sandboxes.placement_changed_at
                                )
                            )
                        )
                    )
                )
         )
            AND NOT EXISTS (
SELECT 1
FROM snapshots sn
WHERE sn.sandbox_id = sandboxes.id
AND sn.type = 'full'
AND sn.state = 'creating'
AND (
(
sandboxes.current_local_artifact_id IS NOT NULL
AND sn.local_artifact_id IS NOT NULL
AND (
sn.local_artifact_id = sandboxes.current_local_artifact_id
AND sn.artifact_kind = sandboxes.current_local_artifact_kind
)
)
OR (
sandboxes.current_local_artifact_id IS NULL
AND (
sandboxes.placement_changed_at IS NULL
OR sn.timestamp >= sandboxes.placement_changed_at
)
)
)
)"#;

/// `pplx_space_sandbox_store::lost::mark_unrecoverable_sandboxes_lost_query`:
/// the drained-node terminator's give-up. `$1` is the node, `$2` its kube uid,
/// `$3` the claimed ids, `$4` the settle seconds and `$5` the substrate kind.
pub(super) const MARK_UNRECOVERABLE_SANDBOXES_LOST: &str = r#"UPDATE sandboxes
            SET lifecycle = 'failed'
          WHERE lifecycle = 'suspended'
            AND target_node_id = $1
            -- Settle guard on same-node creating uploads ($4 = quiet
            -- seconds, see DEAD_UPLOAD_SETTLE): EC2 'terminated' proves the
            -- worker is dead but not that a READY finalize for already-
            -- durable bytes isn't still in the API path. A row heartbeated
            -- (or created) inside the window defers the flip; the sandbox
            -- keeps blocking the reap and later ticks re-check.
            AND NOT EXISTS (
                SELECT 1 FROM snapshots sn
                 WHERE sn.sandbox_id = sandboxes.id
                   AND sn.source_node_id = $1
                   AND sn.type = 'full'
                   AND sn.state = 'creating'
                   AND COALESCE(sn.last_attempt_at, sn.timestamp)
                       >= NOW() - make_interval(secs => $4::BIGINT)
            )
            -- Only rows the caller FOR UPDATE-claimed may flip. Under READ
            -- COMMITTED the claim only fences rows that matched at its own
            -- snapshot; without this restriction a row entering the
            -- candidate set between the claim and this statement would flip
            -- un-fenced, re-opening the FK-lock race with a concurrent
            -- BeginArtifactBackup insert. Late arrivals wait for the next
            -- tick's claim instead.
            AND id = ANY($3)
            AND NOT EXISTS (
             SELECT 1
               FROM snapshots sn
               JOIN snapshot_storage_locations ssl ON ssl.snapshot_id = sn.id
              WHERE sn.sandbox_id = sandboxes.id
                AND sn.type = 'full'
                AND sn.state = 'ready'
                AND ssl.orphaned_at IS NULL
                AND (
                    (
sandboxes.current_local_artifact_id IS NOT NULL
AND sn.local_artifact_id IS NOT NULL
AND (
sn.local_artifact_id = sandboxes.current_local_artifact_id
AND sn.artifact_kind = sandboxes.current_local_artifact_kind
)
)
                    OR (
                        sandboxes.current_local_artifact_id IS NULL
                        AND (
                            (sandboxes.evacuation_restore_target IS NOT NULL AND NOT EXISTS (
SELECT 1
FROM snapshots newer
WHERE newer.sandbox_id = sandboxes.id
AND newer.type = 'full'
AND newer.timestamp > sn.timestamp
))
                            OR (
                                sandboxes.evacuation_restore_target IS NULL
                                AND (
                                    sandboxes.placement_changed_at IS NULL
                                    OR sn.timestamp >= sandboxes.placement_changed_at
                                )
                            )
                        )
                    )
                )
         )
            AND NOT EXISTS (
SELECT 1
FROM snapshots sn
WHERE sn.sandbox_id = sandboxes.id
AND sn.source_node_id IS DISTINCT FROM $1
AND sn.type = 'full'
AND sn.state = 'creating'
AND (
(
sandboxes.current_local_artifact_id IS NOT NULL
AND sn.local_artifact_id IS NOT NULL
AND (
sn.local_artifact_id = sandboxes.current_local_artifact_id
AND sn.artifact_kind = sandboxes.current_local_artifact_kind
)
)
OR (
sandboxes.current_local_artifact_id IS NULL
AND (
sandboxes.placement_changed_at IS NULL
OR sn.timestamp >= sandboxes.placement_changed_at
)
)
)
)
            AND EXISTS (
                SELECT 1 FROM nodes
                WHERE id = $1 AND drain_phase = 'drained' AND managed_by = $5
            )
            AND EXISTS (
                SELECT 1 FROM node_k8s
                WHERE node_id = $1
                  AND k8s_node_uid IS NOT DISTINCT FROM $2
            )
          RETURNING id"#;
