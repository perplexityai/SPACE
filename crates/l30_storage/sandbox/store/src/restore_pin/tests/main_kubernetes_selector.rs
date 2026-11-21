//! Main's Kubernetes suspend-restore selector, frozen as it rendered at the
//! base commit of the change that added the capture-order rules: the
//! statement `K8sStore::selected_suspend_restore_storage_uri` ran for a
//! `suspended` row, built from `pplx_space_sandbox_portability` fragments
//! (`snapshot_is_current_local_artifact`, `fresh_ready_snapshot_exists`,
//! `no_newer_full_snapshot`) as they were then. A rollback runs exactly this,
//! so it's a copy, not a call: the current fragments may change without
//! changing what a rolled-back control plane reads.

/// `RestoreSnapshotChoice::SourceNodeOrFresh`, a user restore.
pub(super) const SOURCE_NODE_OR_FRESH: &str = r#"SELECT ssl.storage_connection_string
               FROM sandboxes s
               JOIN snapshots sn ON sn.sandbox_id = s.id
               JOIN snapshot_storage_locations ssl ON ssl.snapshot_id = sn.id
              WHERE s.id = $1
                AND s.lifecycle IN ('suspended', 'restoring')
                AND ssl.orphaned_at IS NULL
                AND sn.type = 'full'
                AND sn.state = 'ready'
                AND (
                    (
s.current_local_artifact_id IS NOT NULL
AND sn.local_artifact_id IS NOT NULL
AND (
sn.local_artifact_id = s.current_local_artifact_id
AND sn.artifact_kind = s.current_local_artifact_kind
)
)
                    OR (s.current_local_artifact_id IS NULL AND (
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
AND (
s.placement_changed_at IS NULL
OR sn.timestamp >= s.placement_changed_at
)
))
))
                )
              ORDER BY sn.timestamp DESC
              LIMIT 1"#;

/// `RestoreSnapshotChoice::NewestRemote`, the node-drain cohort restore.
pub(super) const NEWEST_REMOTE: &str = r#"SELECT ssl.storage_connection_string
               FROM sandboxes s
               JOIN snapshots sn ON sn.sandbox_id = s.id
               JOIN snapshot_storage_locations ssl ON ssl.snapshot_id = sn.id
              WHERE s.id = $1
                AND s.lifecycle IN ('suspended', 'restoring')
                AND ssl.orphaned_at IS NULL
                AND sn.type = 'full'
                AND sn.state = 'ready'
                AND (
                    (
s.current_local_artifact_id IS NOT NULL
AND sn.local_artifact_id IS NOT NULL
AND (
sn.local_artifact_id = s.current_local_artifact_id
AND sn.artifact_kind = s.current_local_artifact_kind
)
)
                    OR (s.current_local_artifact_id IS NULL AND NOT EXISTS (
SELECT 1
FROM snapshots newer
WHERE newer.sandbox_id = s.id
AND newer.type = 'full'
AND newer.timestamp > sn.timestamp
))
                )
              ORDER BY sn.timestamp DESC
              LIMIT 1"#;

/// A `restoring` row's retry (`restore_retry` with
/// `RestoreSnapshotChoice::SourceNodeOrFresh`), built from
/// `restore_retry_source_snapshot_is_current`.
pub(super) const RETRY: &str = r#"SELECT ssl.storage_connection_string
               FROM sandboxes s
               JOIN snapshots sn ON sn.sandbox_id = s.id
               JOIN snapshot_storage_locations ssl ON ssl.snapshot_id = sn.id
              WHERE s.id = $1
                AND s.lifecycle IN ('suspended', 'restoring')
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
              ORDER BY sn.timestamp DESC
              LIMIT 1"#;
