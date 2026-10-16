//! `local_artifacts` rows and the sandbox's published artifact pointer.

use super::*;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalArtifactRow {
    pub local_artifact_id: LocalArtifactId,
    pub sandbox_id: String,
    pub artifact_kind: ArtifactKind,
    pub artifact_origin: ArtifactOrigin,
    pub snapshot_name: Option<SnapshotName>,
    pub local_path: String,
}

/// Make `local_artifact_id` the sole suspend-origin artifact for a sandbox.
/// Reusing the same immutable identity updates its path and touch time without
/// discarding its backup-completion marker. A different identity replaces the
/// stale row because the per-sandbox snapshot path has only one byte owner.
pub(super) fn upsert_suspend_local_artifact(
    conn: &Connection,
    sandbox_id: &str,
    local_artifact_id: LocalArtifactId,
    local_path: &str,
) -> Result<()> {
    conn.execute(
        "DELETE FROM local_artifacts
         WHERE sandbox_id = ?1
           AND artifact_origin = 'suspend'
           AND local_artifact_id != ?2",
        params![sandbox_id, local_artifact_id.to_string()],
    )?;
    let staged_artifact_exists = conn.query_row(
        "SELECT EXISTS (
             SELECT 1 FROM local_artifacts
             WHERE local_artifact_id = ?1
               AND sandbox_id = ?2
               AND artifact_kind = 'full'
               AND artifact_origin = 'suspend'
               AND snapshot_name IS NULL
         )",
        params![local_artifact_id.to_string(), sandbox_id],
        |row| row.get(0),
    )?;
    if staged_artifact_exists {
        conn.execute(
            "UPDATE local_artifacts
             SET local_path = ?1, last_touched_at = ?2
             WHERE local_artifact_id = ?3",
            params![local_path, now()?, local_artifact_id.to_string()],
        )?;
    } else {
        insert_local_artifact(
            conn,
            sandbox_id,
            local_artifact_id,
            ArtifactKind::Full,
            ArtifactOrigin::Suspend,
            None,
            local_path,
        )?;
    }
    Ok(())
}

pub fn pin_suspended_artifact_for_cleanup(
    conn: &Connection,
    id: &str,
    local_artifact_id: LocalArtifactId,
    local_path: &str,
) -> Result<()> {
    let tx = immediate_transaction(conn)?;
    upsert_suspend_local_artifact(&tx, id, local_artifact_id, local_path)?;
    let changed = tx.execute(
        "UPDATE sandboxes
         SET state = 'cleanup_required',
             current_local_artifact_id = ?1,
             current_local_artifact_kind = ?2,
             current_local_artifact_origin = ?3,
             suspend_predecessor_artifact_id = NULL,
             cleanup_completion = ?4,
             cleanup_keep_filesystem = TRUE,
             failure_reason = NULL,
             state_changed_at = CASE
                 WHEN state != 'cleanup_required' THEN ?5
                 ELSE state_changed_at
             END,
             updated_at = ?5
         WHERE id = ?6
           AND state = 'suspending'",
        params![
            local_artifact_id.to_string(),
            ArtifactKind::Full.as_str(),
            ArtifactOrigin::Suspend.as_str(),
            CleanupCompletion::MarkSuspended.as_str(),
            now()?,
            id
        ],
    )?;
    if changed != 1 {
        return Err(rusqlite::Error::QueryReturnedNoRows);
    }
    tx.commit()?;
    Ok(())
}

pub fn adopt_suspended_local_artifact(
    conn: &Connection,
    id: &str,
    local_artifact_id: LocalArtifactId,
    local_path: &str,
) -> Result<bool> {
    adopt_suspended_local_artifact_impl(conn, id, local_artifact_id, local_path, true)
}

pub fn adopt_suspended_local_artifact_preserving_updated_at(
    conn: &Connection,
    id: &str,
    local_artifact_id: LocalArtifactId,
    local_path: &str,
) -> Result<bool> {
    adopt_suspended_local_artifact_impl(conn, id, local_artifact_id, local_path, false)
}

fn adopt_suspended_local_artifact_impl(
    conn: &Connection,
    id: &str,
    local_artifact_id: LocalArtifactId,
    local_path: &str,
    touch_sandbox: bool,
) -> Result<bool> {
    let tx = immediate_transaction(conn)?;
    let changed = if touch_sandbox {
        tx.execute(
            "UPDATE sandboxes
             SET current_local_artifact_id = ?1,
                 current_local_artifact_kind = ?2,
                 current_local_artifact_origin = ?3,
                 updated_at = ?4
             WHERE id = ?5
               AND state = 'suspended'
               AND current_local_artifact_id IS NULL",
            params![
                local_artifact_id.to_string(),
                ArtifactKind::Full.as_str(),
                ArtifactOrigin::Suspend.as_str(),
                now()?,
                id
            ],
        )?
    } else {
        tx.execute(
            "UPDATE sandboxes
             SET current_local_artifact_id = ?1,
                 current_local_artifact_kind = ?2,
                 current_local_artifact_origin = ?3
             WHERE id = ?4
               AND state = 'suspended'
               AND current_local_artifact_id IS NULL",
            params![
                local_artifact_id.to_string(),
                ArtifactKind::Full.as_str(),
                ArtifactOrigin::Suspend.as_str(),
                id
            ],
        )?
    };
    if changed == 1 {
        insert_local_artifact(
            &tx,
            id,
            local_artifact_id,
            ArtifactKind::Full,
            ArtifactOrigin::Suspend,
            None,
            local_path,
        )?;
    }
    tx.commit()?;
    Ok(changed != 0)
}

pub fn publish_restored_suspended_artifact(
    conn: &Connection,
    id: &str,
    local_artifact_id: LocalArtifactId,
    local_path: &str,
) -> Result<()> {
    let tx = immediate_transaction(conn)?;
    // Same dedupe rationale as suspended cleanup publication: a cross-node
    // restore back to a node that previously suspended this sandbox can land
    // here with a stale `suspend`-origin row already
    // pointing at the per-sandbox `{CACHE_DIR}/{id}/snapshot` path.
    // Without the DELETE the new row and the stale row would share
    // `local_path`, and the reaper's `SharedLocalPath` safety gate
    // would refuse to reap either — turning the reaper permanently
    // inert for this artifact. The on-disk bytes have just been
    // restored to that path, so the stale row no longer owns them;
    // its cluster-side backup row ages out via
    // cluster-controller's stuck_snapshot_reaper.
    tx.execute(
        "DELETE FROM local_artifacts
         WHERE sandbox_id = ?1 AND artifact_origin = 'suspend'",
        params![id],
    )?;
    insert_local_artifact(
        &tx,
        id,
        local_artifact_id,
        ArtifactKind::Full,
        ArtifactOrigin::Suspend,
        None,
        local_path,
    )?;
    let changed = tx.execute(
        "UPDATE sandboxes
         SET state = 'suspended',
             restore_origin = NULL,
             restore_phase = NULL,
             restore_storage_uri = NULL,
             current_local_artifact_id = ?1,
             current_local_artifact_kind = ?2,
             current_local_artifact_origin = ?3,
             state_changed_at = CASE WHEN state != 'suspended' THEN ?4 ELSE state_changed_at END,
             updated_at = ?4
         WHERE id = ?5
           AND state = 'restoring'
           AND current_local_artifact_id IS NULL",
        params![
            local_artifact_id.to_string(),
            ArtifactKind::Full.as_str(),
            ArtifactOrigin::Suspend.as_str(),
            now()?,
            id
        ],
    )?;
    if changed != 1 {
        return Err(rusqlite::Error::QueryReturnedNoRows);
    }
    retire_restore_transfer_scope(&tx, id)?;
    tx.commit()?;
    Ok(())
}

pub fn publish_restored_suspended_without_artifact(conn: &Connection, id: &str) -> Result<()> {
    let tx = immediate_transaction(conn)?;
    let changed = tx.execute(
        "UPDATE sandboxes
         SET state = 'suspended',
             failure_reason = NULL,
             restore_origin = NULL,
             restore_phase = NULL,
             restore_storage_uri = NULL,
             current_local_artifact_id = NULL,
             current_local_artifact_kind = NULL,
             current_local_artifact_origin = NULL,
             state_changed_at = CASE WHEN state != 'suspended' THEN ?1 ELSE state_changed_at END,
             updated_at = ?1
         WHERE id = ?2
           AND state = 'restoring'
           AND current_local_artifact_id IS NULL",
        params![now()?, id],
    )?;
    if changed != 1 {
        return Err(rusqlite::Error::QueryReturnedNoRows);
    }
    retire_restore_transfer_scope(&tx, id)?;
    tx.commit()?;
    Ok(())
}

pub fn publish_restored_disk_as_new(conn: &Connection, id: &str) -> Result<()> {
    let tx = immediate_transaction(conn)?;
    let changed = tx.execute(
        "UPDATE sandboxes
         SET state = 'stopped',
             failure_reason = NULL,
             restore_origin = NULL,
             restore_phase = NULL,
             restore_storage_uri = NULL,
             current_local_artifact_id = NULL,
             current_local_artifact_kind = NULL,
             current_local_artifact_origin = NULL,
             state_changed_at = CASE WHEN state != 'stopped' THEN ?1 ELSE state_changed_at END,
             updated_at = ?1
         WHERE id = ?2 AND state = 'restoring'",
        params![now()?, id],
    )?;
    if changed != 1 {
        return Err(rusqlite::Error::QueryReturnedNoRows);
    }
    clear_runtime_fields(&tx, id)?;
    retire_restore_transfer_scope(&tx, id)?;
    tx.commit()?;
    Ok(())
}

pub(super) fn insert_local_artifact(
    conn: &Connection,
    sandbox_id: &str,
    local_artifact_id: LocalArtifactId,
    artifact_kind: ArtifactKind,
    artifact_origin: ArtifactOrigin,
    snapshot_name: Option<&SnapshotName>,
    local_path: &str,
) -> Result<()> {
    let ts = now()?;
    conn.execute(
        "INSERT INTO local_artifacts (
             local_artifact_id, sandbox_id, artifact_kind, artifact_origin,
             snapshot_name, local_path, created_at, last_touched_at
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?7)",
        params![
            local_artifact_id.to_string(),
            sandbox_id,
            artifact_kind.as_str(),
            artifact_origin.as_str(),
            snapshot_name.map(SnapshotName::as_str),
            local_path,
            ts,
        ],
    )?;
    Ok(())
}

/// Mark the artifact's remote backup as durably completed. Written by
/// the snapshot-upload worker right after `finalize_artifact_backup`
/// returns `ready` from the cluster API. The reaper gates eviction
/// on `backup_completed_at IS NOT NULL`, which is the per-artifact
/// equivalent of the cluster `snapshots.state='ready'` row — without
/// adding a network dependency for GC.
pub fn mark_local_artifact_backup_completed(
    conn: &Connection,
    local_artifact_id: &str,
) -> Result<()> {
    conn.execute(
        "UPDATE local_artifacts SET backup_completed_at = ?1 WHERE local_artifact_id = ?2",
        params![now()?, local_artifact_id],
    )?;
    Ok(())
}

/// True when the sandbox row is parked `suspended` with
/// `current_local_artifact_id` equal to `local_artifact_id`.
///
/// This is the safety gate for reclaiming per-artifact files under the
/// published snapshot directory after a backup finalizes ready. Every
/// suspend stages into a sibling directory and flips the row to
/// `suspended` with the freshly minted artifact pinned in one
/// transaction, so while this predicate holds — checked under the same
/// connection mutex every state transition serializes on — the files at
/// the published snapshot path are exactly the pinned artifact's bytes,
/// never a newer suspend's output mid-encode or mid-publish.
pub fn sandbox_suspended_with_pinned_artifact(
    conn: &Connection,
    sandbox_id: &str,
    local_artifact_id: &str,
) -> Result<bool> {
    conn.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM sandboxes
             WHERE id = ?1
               AND state = 'suspended'
               AND current_local_artifact_id = ?2
         )",
        params![sandbox_id, local_artifact_id],
        |r| r.get(0),
    )
}

pub fn publish_named_local_artifact(
    conn: &Connection,
    sandbox_id: &str,
    local_artifact_id: LocalArtifactId,
    artifact_kind: ArtifactKind,
    snapshot_name: &SnapshotName,
    local_path: &str,
) -> Result<()> {
    insert_local_artifact(
        conn,
        sandbox_id,
        local_artifact_id,
        artifact_kind,
        ArtifactOrigin::Snapshot,
        Some(snapshot_name),
        local_path,
    )
}

pub fn publish_stopped_artifact(
    conn: &Connection,
    sandbox_id: &str,
    local_artifact_id: LocalArtifactId,
    local_path: &str,
) -> Result<()> {
    let tx = immediate_transaction(conn)?;
    insert_local_artifact(
        &tx,
        sandbox_id,
        local_artifact_id,
        ArtifactKind::Disk,
        ArtifactOrigin::Stop,
        None,
        local_path,
    )?;
    let changed = tx.execute(
        "UPDATE sandboxes
         SET state = 'stopped',
             current_local_artifact_id = ?1,
             current_local_artifact_kind = ?2,
             current_local_artifact_origin = ?3,
             uid = NULL,
             slot = NULL,
             guest_cid = NULL,
             pid = NULL,
             spaced_host_pid = NULL,
             netgtw_pid = NULL,
             vsock_uds_path = NULL,
             api_socket_path = NULL,
             space_api_route_generation = NULL,
             space_api_route_node_id = NULL,
             space_api_route_execution_generation = NULL,
             space_api_route_pod_uid = NULL,
             space_api_route_substrate_kind = NULL,
             space_api_route_activation_attempt_id = NULL,
             space_api_route_identity_provenance = NULL,
             spaced_endpoint_proof_json = NULL,
             guest_uptime_secs = NULL,
             cleanup_completion = NULL,
             cleanup_keep_filesystem = NULL,
             failure_reason = NULL,
             state_changed_at = CASE WHEN state != 'stopped' THEN ?4 ELSE state_changed_at END,
             updated_at = ?4
         WHERE id = ?5
           AND (
               state = 'stopping'
               OR (
                   state IN ('cleanup_required', 'cleanup_failed')
                   AND cleanup_completion = 'mark_stopped'
               )
               OR (
                   state = 'stopped'
                   AND current_local_artifact_id IS NULL
               )
           )",
        params![
            local_artifact_id.to_string(),
            ArtifactKind::Disk.as_str(),
            ArtifactOrigin::Stop.as_str(),
            now()?,
            sandbox_id,
        ],
    )?;
    if changed != 1 {
        return Err(rusqlite::Error::QueryReturnedNoRows);
    }
    // Clear the runtime first so the retention trigger observes its complete
    // identity, then close the exact attempt in the same transaction.
    route_activation_claims::close_claim(&tx, sandbox_id)?;
    tx.commit()?;
    Ok(())
}

pub fn get_local_artifact(
    conn: &Connection,
    local_artifact_id: LocalArtifactId,
) -> Result<Option<LocalArtifactRow>> {
    conn.query_row(
        "SELECT local_artifact_id, sandbox_id, artifact_kind, artifact_origin,
                snapshot_name, local_path
         FROM local_artifacts WHERE local_artifact_id = ?1",
        params![local_artifact_id.to_string()],
        row_to_local_artifact,
    )
    .optional()
}

pub fn get_named_local_artifact(
    conn: &Connection,
    sandbox_id: &str,
    snapshot_name: &SnapshotName,
) -> Result<Option<LocalArtifactRow>> {
    conn.query_row(
        "SELECT local_artifact_id, sandbox_id, artifact_kind, artifact_origin,
                snapshot_name, local_path
         FROM local_artifacts
         WHERE sandbox_id = ?1 AND artifact_origin = 'snapshot' AND snapshot_name = ?2",
        params![sandbox_id, snapshot_name.as_str()],
        row_to_local_artifact,
    )
    .optional()
}

pub fn delete_named_local_artifact(
    conn: &Connection,
    sandbox_id: &str,
    snapshot_name: &str,
) -> Result<()> {
    conn.execute(
        "DELETE FROM local_artifacts
         WHERE sandbox_id = ?1 AND artifact_origin = 'snapshot' AND snapshot_name = ?2",
        params![sandbox_id, snapshot_name],
    )?;
    Ok(())
}

fn row_to_local_artifact(row: &rusqlite::Row<'_>) -> Result<LocalArtifactRow> {
    Ok(LocalArtifactRow {
        local_artifact_id: row.get::<_, String>(0)?.parse().map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(0, Type::Text, Box::new(error))
        })?,
        sandbox_id: row.get(1)?,
        artifact_kind: row.get::<_, String>(2)?.parse().map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(2, Type::Text, Box::new(error))
        })?,
        artifact_origin: row.get::<_, String>(3)?.parse().map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(3, Type::Text, Box::new(error))
        })?,
        snapshot_name: row
            .get::<_, Option<String>>(4)?
            .map(SnapshotName::try_from)
            .transpose()
            .map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(4, Type::Text, Box::new(error))
            })?,
        local_path: row.get(5)?,
    })
}
