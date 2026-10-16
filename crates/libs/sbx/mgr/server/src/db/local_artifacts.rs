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
    publish_restored_artifact_impl(
        conn,
        id,
        local_artifact_id,
        local_path,
        RestoredArtifactPublication {
            state: "suspended",
            kind: ArtifactKind::Full,
            origin: ArtifactOrigin::Suspend,
            // A restored suspend checkpoint keeps whatever activation identity
            // carried the restore: the caller resumes the VM from it next.
            dedupe: StaleArtifactDedupe::SameOrigin,
            clear_runtime: false,
            // Preserved from before this shared helper existed; see the field's
            // doc.
            clear_failure_reason: false,
        },
    )
}

/// Finalize a same-id disk materialization: the local row leaves `restoring`
/// as `Stopped` with the staged `stop`-origin disk artifact pinned as its
/// current artifact.
///
/// This is deliberately not [`publish_restored_disk_as_new`]. That one serves
/// the fork transport, which nulls the pin so the staged disk stays an
/// immutable source for the child's cold boot. An evacuated stopped sandbox is
/// the opposite case: the pin is the sandbox's only remaining handle on its own
/// bytes, so it must survive the move.
///
/// Like the suspended arm, it leaves `backup_completed_at` unset, so the node
/// reports the pin as pending backup while it stays stopped. An
/// `EnqueueArtifactBackup` that finds the cluster's ready copy of this artifact
/// stamps it (`artifact_backup.rs`, `mark_backup_completed`).
pub fn publish_restored_stopped_artifact(
    conn: &Connection,
    id: &str,
    local_artifact_id: LocalArtifactId,
    local_path: &str,
) -> Result<()> {
    publish_restored_artifact_impl(
        conn,
        id,
        local_artifact_id,
        local_path,
        RestoredArtifactPublication {
            state: "stopped",
            kind: ArtifactKind::Disk,
            origin: ArtifactOrigin::Stop,
            // A materialized stopped sandbox was never launched here, so
            // whatever the restore left on the row describes an activation that
            // does not exist.
            dedupe: StaleArtifactDedupe::SamePath,
            clear_runtime: true,
            clear_failure_reason: true,
        },
    )
}

/// The lifecycle a restore finalize publishes, and what it does to the row
/// besides pinning the artifact.
struct RestoredArtifactPublication {
    state: &'static str,
    kind: ArtifactKind,
    origin: ArtifactOrigin,
    clear_runtime: bool,
    /// Which stale `local_artifacts` row, if any, the publication must remove
    /// so the reaper does not go inert on these bytes.
    dedupe: StaleArtifactDedupe,
    /// Whether the finalize also clears a stale `failure_reason`.
    ///
    /// A flag rather than a uniform `NULL` because the two arms genuinely
    /// differ today: the suspended publication has never cleared it, while
    /// every other restore finalize
    /// (`publish_restored_suspended_without_artifact`,
    /// `publish_restored_disk_as_new`) does. Unifying them is defensible but
    /// it would change the suspended path, which this is not the change for —
    /// so the asymmetry is preserved and named rather than quietly resolved.
    clear_failure_reason: bool,
}

/// Which stale row a restore finalize deletes before inserting its own.
///
/// The predicate differs per arm because its justification does. The reaper's
/// `SharedLocalPath` gate refuses to reap *either* of two rows that name the
/// same `local_path`, so a finalize has to remove a predecessor that would
/// collide on the path it is about to claim — and only that one.
enum StaleArtifactDedupe {
    /// Delete the prior row of this origin. What the suspended finalize has
    /// always done: a `suspend`-origin row for this sandbox lives at the same
    /// per-sandbox `{id}/snapshot` path the restore has just rewritten, so
    /// origin and path coincide.
    SameOrigin,
    /// Delete a row that names this path, or one already holding the artifact
    /// id about to be inserted. A locally published `stop` artifact lives in
    /// its own `{id}/snapshots/d-stop-*` btrfs subvolume, so deleting by origin
    /// would drop a row owning *different* bytes — and since the reaper is the
    /// only thing that would ever free them, that leaks the subvolume rather
    /// than reclaiming it.
    ///
    /// The id half is not redundant with the path half. The inserted id is the
    /// *source node's* pin, and `local_artifacts.local_artifact_id` is the
    /// primary key, so a row already holding that id would collide, roll the
    /// whole transaction back, and leave the row `restoring` with every retry
    /// failing identically. The stopped arm records its pin at
    /// `d-stop-{artifact}`, the same name a local publication of that artifact
    /// uses, so the usual id holder names this path too; the id half is what
    /// still clears one recorded anywhere else.
    SamePath,
}

/// The shape shared by every `restoring` → terminal finalize that pins an
/// artifact.
///
/// Both publications open an immediate transaction, delete the stale row the
/// arm's `StaleArtifactDedupe` names (same origin, or same path or id), insert the new
/// artifact, CAS the sandbox row out of `restoring` while it is still unpinned,
/// and retire the transfer scope. Keeping one copy is what makes a change to
/// the CAS or the dedupe land on both arms instead of only the one the author
/// happened to be looking at.
fn publish_restored_artifact_impl(
    conn: &Connection,
    id: &str,
    local_artifact_id: LocalArtifactId,
    local_path: &str,
    publication: RestoredArtifactPublication,
) -> Result<()> {
    let tx = immediate_transaction(conn)?;
    // Remove the predecessor that would collide on the path or the primary key
    // this is about to claim; see `StaleArtifactDedupe` for why the predicate
    // differs per arm. A row at the same path no longer owns bytes the restore
    // has just rewritten; a row holding the same id at another path is the trade
    // `StaleArtifactDedupe::SamePath` describes. Either way its cluster-side
    // backup row ages out via cluster-controller's stuck_snapshot_reaper.
    match publication.dedupe {
        StaleArtifactDedupe::SameOrigin => tx.execute(
            "DELETE FROM local_artifacts
             WHERE sandbox_id = ?1 AND artifact_origin = ?2",
            params![id, publication.origin.as_str()],
        )?,
        StaleArtifactDedupe::SamePath => tx.execute(
            "DELETE FROM local_artifacts
             WHERE sandbox_id = ?1 AND (local_path = ?2 OR local_artifact_id = ?3)",
            params![id, local_path, local_artifact_id.to_string()],
        )?,
    };
    insert_local_artifact(
        &tx,
        id,
        local_artifact_id,
        publication.kind,
        publication.origin,
        None,
        local_path,
    )?;
    let changed = tx.execute(
        "UPDATE sandboxes
         SET state = ?1,
             failure_reason = CASE WHEN ?7 THEN NULL ELSE failure_reason END,
             restore_origin = NULL,
             restore_phase = NULL,
             restore_storage_uri = NULL,
             current_local_artifact_id = ?2,
             current_local_artifact_kind = ?3,
             current_local_artifact_origin = ?4,
             state_changed_at = CASE WHEN state != ?1 THEN ?5 ELSE state_changed_at END,
             updated_at = ?5
         WHERE id = ?6
           AND state = 'restoring'
           AND current_local_artifact_id IS NULL",
        params![
            publication.state,
            local_artifact_id.to_string(),
            publication.kind.as_str(),
            publication.origin.as_str(),
            now()?,
            id,
            publication.clear_failure_reason
        ],
    )?;
    if changed != 1 {
        return Err(rusqlite::Error::QueryReturnedNoRows);
    }
    if publication.clear_runtime {
        clear_runtime_fields(&tx, id)?;
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
    crate::route_activation_claims::close_claim(&tx, sandbox_id)?;
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
