//! Main's node lookup, `GetSuspendSnapshotLocation`, frozen as it was at the
//! base commit of the change that added the capture-order rules:
//! `pplx_space_snapshot_store::latest_ready_suspend_location_placed_on`'s
//! statement, verbatim. A rollback runs exactly this, so it's a copy, not a
//! call.

/// `$1` is the sandbox, `$2` the caller's node ids.
pub(super) const LATEST_READY_SUSPEND_LOCATION_PLACED_ON: &str = r#"SELECT s.id AS snapshot_id,
                ssl.storage_connection_string,
                s.size_bytes
         FROM snapshots s
         JOIN snapshot_storage_locations ssl ON ssl.snapshot_id = s.id
         JOIN sandboxes sb ON sb.id = s.sandbox_id
         WHERE s.sandbox_id = $1
           AND (sb.active_node_id = ANY($2) OR sb.target_node_id = ANY($2))
           AND s.type = 'full'
           AND (s.artifact_origin IS NULL OR s.artifact_origin = 'suspend')
           AND s.state = 'ready'
         ORDER BY s.timestamp DESC
         LIMIT 1"#;
