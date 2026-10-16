//! Durable import ownership uses the same interrupted-activation cleanup as Create.
//!
//! Directory claims share the immediate transaction with identity checks.
//! Snapshot catalog commits precede canonical visibility so predecessor readers
//! cannot mistake an uncommitted directory for a ready snapshot. The caller
//! retains the connection mutex and artifact exclusion through the final rename.
//! Streaming, hashing, and retry byte comparison stay outside the SQLite lock.

use super::*;

#[derive(Debug, thiserror::Error)]
pub(crate) enum ReserveImportError {
    #[error("reserve sandbox import: {0}")]
    Database(#[from] Error),
    #[error("claim sandbox cache: {0}")]
    Claim(#[from] std::io::Error),
}

pub(crate) struct ImportReservation<'a> {
    pub params: InsertSandboxParams<'a>,
    pub episode: CleanupEpisodeSpec,
    pub execution: &'a ExecutionMetadata,
    pub created_at: &'a str,
    pub uptime: Option<u64>,
    pub cache_root: &'a Path,
}

pub(crate) fn reserve_import(
    conn: &Connection,
    reservation: ImportReservation<'_>,
) -> std::result::Result<(), ReserveImportError> {
    let ImportReservation {
        params,
        episode,
        execution,
        created_at,
        uptime,
        cache_root,
    } = reservation;
    let tx = immediate_transaction(conn)?;
    let id = params.id.to_owned();
    insert_sandbox(&tx, params)?;
    insert_cleanup_episode(&tx, &id, episode)?;
    upsert_sandbox_execution_metadata_in_transaction(&tx, &id, execution)?;
    preserve_import_timestamps(&tx, &id, created_at, uptime)?;
    // Only an exclusive successful mkdir grants filesystem disposal authority.
    // A crash before commit can leave an unregistered root; inventory preserves
    // that evidence rather than exposing it as a completed import.
    std::fs::create_dir(cache_root)?;
    if let Some(parent) = cache_root.parent() {
        std::fs::File::open(parent)?.sync_all()?;
    }
    tx.commit()?;
    Ok(())
}

pub(crate) fn complete_import(
    conn: &Connection,
    id: &str,
    cleanup_id: SandboxCleanupEpisodeId,
    suspended: bool,
) -> Result<bool> {
    let tx = immediate_transaction(conn)?;
    let target = if suspended {
        SandboxState::Suspended
    } else {
        SandboxState::Stopped
    };
    let changed = tx.execute(
        "UPDATE sandboxes SET state = ?1, state_changed_at = ?2, updated_at = ?2,
             cleanup_completion = NULL, cleanup_keep_filesystem = NULL
         WHERE id = ?3 AND state = 'creating'
           AND EXISTS (SELECT 1 FROM sandbox_cleanup_episodes
                       WHERE sandbox_id = ?3 AND cleanup_id = ?4 AND phase = 'armed')",
        params![target.as_str(), now()?, id, cleanup_id.to_string()],
    )?;
    if changed == 1 {
        tx.execute(
            "DELETE FROM sandbox_cleanup_episodes WHERE sandbox_id = ?1 AND cleanup_id = ?2",
            params![id, cleanup_id.to_string()],
        )?;
    }
    tx.commit()?;
    Ok(changed == 1)
}

pub(crate) fn preserve_import_timestamps(
    conn: &Connection,
    id: &str,
    created_at: &str,
    guest_uptime: Option<u64>,
) -> Result<()> {
    if !created_at.is_empty() {
        conn.execute(
            "UPDATE sandboxes SET created_at = ?1 WHERE id = ?2",
            params![created_at, id],
        )?;
    }
    if let Some(uptime) = guest_uptime {
        update_guest_uptime(conn, id, uptime)?;
    }
    Ok(())
}

pub(crate) struct SnapshotPublication<'a> {
    pub id: &'a str,
    pub template: &'a str,
    pub hash: &'a str,
    pub staging: &'a Path,
    pub destination: &'a Path,
    pub binding: &'a TransferBinding,
    pub verified_existing: bool,
}

pub(crate) fn publish_snapshot(
    conn: &Connection,
    publication: SnapshotPublication<'_>,
) -> std::result::Result<bool, ReserveImportError> {
    let SnapshotPublication {
        id,
        template,
        hash,
        staging,
        destination,
        binding,
        verified_existing,
    } = publication;
    let tx = immediate_transaction(conn)?;
    // A rowless publication would claim the canonical cache root and prevent
    // the later ImportSandbox from acquiring its exclusive directory claim.
    let Some(row) = get_sandbox(&tx, id)? else {
        return Ok(false);
    };
    if row.template != template
        || row.template_hash.as_deref() != Some(hash)
        || row.volume_encryption
        || matches!(
            row.state,
            SandboxState::Creating
                | SandboxState::Deleting
                | SandboxState::Stopping
                | SandboxState::Quarantined
        )
    {
        return Ok(false);
    }
    if let Some(parent) = destination.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let already_published = match std::fs::symlink_metadata(destination) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
        Ok(metadata) if verified_existing => {
            let error = std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                "snapshot retry destination changed",
            );
            if !metadata.is_dir() || metadata.file_type().is_symlink() {
                return Err(error.into());
            }
            let existing: TransferBinding =
                serde_json::from_slice(&std::fs::read(destination.join(".transfer.json"))?)
                    .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
            if &existing != binding {
                return Err(error.into());
            }
            for entry in std::fs::read_dir(staging)? {
                let entry = entry?;
                let expected = entry.metadata()?;
                let actual = std::fs::symlink_metadata(destination.join(entry.file_name()))?;
                if !actual.is_file() || actual.len() != expected.len() {
                    return Err(error.into());
                }
            }
            true
        }
        Ok(_) => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                "snapshot destination is occupied",
            )
            .into());
        }
        Err(error) => return Err(error.into()),
    };
    let snapshot_name = destination
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "invalid snapshot destination",
            )
        })?
        .parse::<SnapshotName>()
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))?;
    let kind = if snapshot_name.as_str().starts_with("f-") {
        ArtifactKind::Full
    } else {
        ArtifactKind::Disk
    };
    let local_path = destination.to_str().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "non-UTF8 snapshot destination",
        )
    })?;
    if let Some(existing) = get_named_local_artifact(&tx, id, &snapshot_name)? {
        let ready: bool = tx.query_row(
            "SELECT state = 'ready' FROM local_artifacts WHERE local_artifact_id = ?1",
            [existing.local_artifact_id.to_string()],
            |row| row.get(0),
        )?;
        if existing.local_path != local_path || existing.artifact_kind != kind || !ready {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                "snapshot publication has another owner",
            )
            .into());
        }
    } else {
        publish_named_local_artifact(
            &tx,
            id,
            LocalArtifactId::new(),
            kind,
            &snapshot_name,
            local_path,
        )?;
    }
    tx.commit()?;
    // A crash here leaves catalog evidence without a visible directory, never
    // the reverse. Both reader versions require the directory; a new import
    // retry can reuse this matching catalog row and publish fresh staging.
    // Missing-path maintenance may retire the row first, which is also safe.
    if !already_published {
        rename_unoccupied(staging, destination)?;
    }
    if let Some(parent) = destination.parent() {
        std::fs::File::open(parent)?.sync_all()?;
    }
    Ok(true)
}

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub(crate) struct TransferBinding {
    pub sandbox_id: String,
    pub template: String,
    pub content_hash: String,
    pub stream_fingerprint: String,
}

pub(crate) fn validate_snapshot_binding(path: &Path, sandbox: &SandboxRow) -> std::io::Result<()> {
    let bytes = match std::fs::read(path.join(".transfer.json")) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    let binding: TransferBinding = serde_json::from_slice(&bytes)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
    if binding.sandbox_id != sandbox.id
        || binding.template != sandbox.template
        || sandbox.template_hash.as_deref() != Some(binding.content_hash.as_str())
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "transferred snapshot belongs to a different sandbox or template",
        ));
    }
    Ok(())
}

pub(crate) fn sync_import_directory(path: &Path) -> std::io::Result<()> {
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let metadata = entry.file_type()?;
        if metadata.is_symlink() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "import contains a symlink",
            ));
        }
        if metadata.is_file() {
            std::fs::File::open(entry.path())?.sync_all()?;
        }
    }
    std::fs::File::open(path)?.sync_all()
}

/// Publication must not replace even an empty destination or dangling symlink.
pub(crate) fn rename_unoccupied(source: &Path, destination: &Path) -> std::io::Result<()> {
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    {
        rustix::fs::renameat_with(
            rustix::fs::CWD,
            source,
            rustix::fs::CWD,
            destination,
            rustix::fs::RenameFlags::NOREPLACE,
        )
        .map_err(Into::into)
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = (source, destination);
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "atomic exclusive rename is unavailable",
        ))
    }
}
