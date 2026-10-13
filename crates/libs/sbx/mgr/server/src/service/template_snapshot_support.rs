use super::cleanup_support::{require_state_write_from, update_state_from_best_effort};
use super::*;

pub(super) fn compute_partial_sha256(path: &std::path::Path) -> std::io::Result<String> {
    use std::io::Read;
    let mut file = std::fs::File::open(path)?;
    let mut hasher = sha2::Sha256::new();
    let mut buf = vec![0u8; 1024 * 1024]; // 1MB
    let n = file.read(&mut buf)?;
    use sha2::Digest;
    hasher.update(buf.get(..n).unwrap_or(&buf));
    let result = hasher.finalize();
    Ok(result
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<String>())
}

pub(super) struct TemplateBaseSnapshotHash {
    pub(super) hash: String,
    pub(super) bytes_hashed: u64,
}

pub(super) fn compute_template_base_snapshot_hash(
    snapshot_dir: &Path,
) -> std::io::Result<TemplateBaseSnapshotHash> {
    use sha2::Digest;
    use std::io::Read;

    let mut hasher = sha2::Sha256::new();
    let mut buf = vec![0u8; 1024 * 1024];
    let mut bytes_hashed = 0_u64;
    for name in ["snapshot.base", "vmstate.base"] {
        let path = snapshot_dir.join(name);
        let mut file = std::fs::File::open(&path)?;
        let len = file.metadata()?.len();
        hasher.update(name.as_bytes());
        hasher.update(len.to_le_bytes());
        loop {
            let n = file.read(&mut buf)?;
            if n == 0 {
                break;
            }
            let n_u64 = u64::try_from(n).unwrap_or(u64::MAX);
            bytes_hashed = bytes_hashed.saturating_add(n_u64);
            hasher.update(buf.get(..n).unwrap_or(&buf));
        }
    }
    let hash = hasher
        .finalize()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    Ok(TemplateBaseSnapshotHash { hash, bytes_hashed })
}

pub(super) fn set_template_cache_permissions(path: &Path, mode: u32) -> Result<(), Status> {
    set_template_cache_permissions_io(path, mode).map_err(|e| {
        Status::internal(format!(
            "set template cache permissions on {}: {e}",
            path.display()
        ))
    })
}

pub(super) fn normalize_template_cache_tree(path: &Path) -> Result<(), Status> {
    normalize_template_cache_tree_io(path).map_err(|e| {
        Status::internal(format!(
            "normalize template cache tree {}: {e}",
            path.display()
        ))
    })
}

pub(super) fn publish_template_base_snapshot_dir(
    staging_dir: &Path,
    snapshot_dir: &Path,
) -> Result<(), Status> {
    let parent = snapshot_dir.parent().ok_or_else(|| {
        Status::internal(format!(
            "template snapshot dir has no parent: {}",
            snapshot_dir.display()
        ))
    })?;
    std::fs::create_dir_all(parent)
        .map_err(|e| Status::internal(format!("create template snapshot parent dir: {e}")))?;
    if let Some(snapshots_root) = parent.parent() {
        set_template_cache_permissions(snapshots_root, TEMPLATE_CACHE_DIR_MODE)?;
    }
    set_template_cache_permissions(parent, TEMPLATE_CACHE_DIR_MODE)?;
    normalize_template_cache_tree(staging_dir)?;

    let file_name = snapshot_dir
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            Status::internal(format!(
                "template snapshot dir has invalid file name: {}",
                snapshot_dir.display()
            ))
        })?;
    let backup_dir = snapshot_dir.with_file_name(format!("{file_name}.old-{}", Uuid::now_v7()));

    let had_existing = match std::fs::rename(snapshot_dir, &backup_dir) {
        Ok(()) => true,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
        Err(error) => {
            return Err(Status::internal(format!(
                "move old template snapshot dir aside: {error}"
            )));
        }
    };

    if let Err(error) = std::fs::rename(staging_dir, snapshot_dir) {
        if had_existing && let Err(restore_error) = std::fs::rename(&backup_dir, snapshot_dir) {
            tracing::error!(
                staging_dir = %staging_dir.display(),
                snapshot_dir = %snapshot_dir.display(),
                backup_dir = %backup_dir.display(),
                %restore_error,
                "failed to restore previous template snapshot dir after publish failure"
            );
        }
        return Err(Status::internal(format!(
            "publish template snapshot dir: {error}"
        )));
    }
    normalize_template_cache_tree(snapshot_dir)?;

    if had_existing && let Err(error) = std::fs::remove_dir_all(&backup_dir) {
        tracing::warn!(
            backup_dir = %backup_dir.display(),
            %error,
            "failed to remove previous template snapshot dir backup"
        );
    }

    Ok(())
}

/// Template base-snapshot admission moves the node-local SQLite
/// `sandboxes.state` from `paused` to `checkpointing`. The caller checks the
/// row under the same connection guard.
pub(super) fn begin_template_base_snapshot_state(
    db: &rusqlite::Connection,
    sandbox_id: &str,
) -> Result<(), Status> {
    require_state_write_from(
        crate::db::update_state_from(db, sandbox_id, &crate::db::edges::TEMPLATE_SNAPSHOT_BEGIN),
        "begin template checkpoint",
    )
}

/// Template base-snapshot completion returns `checkpointing` to `paused`. The
/// wake lock excludes neither Stop nor Start, so a Stop that completed during
/// capture or hashing leaves `stopped`, or whatever a later Start or Pause
/// moved it to; the completion reports that it was superseded instead of
/// overwriting the row.
pub(super) fn complete_template_base_snapshot_state(
    db: &rusqlite::Connection,
    sandbox_id: &str,
) -> Result<(), Status> {
    require_state_write_from(
        crate::db::update_state_from(
            db,
            sandbox_id,
            &crate::db::edges::CHECKPOINT_COMPLETE_PAUSED,
        ),
        "complete template checkpoint",
    )
}

/// After a failed template base snapshot, return `checkpointing` to `paused`
/// on a best-effort basis. A row another transition moved, such as a Stop,
/// keeps its state.
pub(super) fn rollback_template_base_snapshot_state(
    db: &Arc<Mutex<rusqlite::Connection>>,
    sandbox_id: &str,
) {
    update_state_from_best_effort(
        db,
        sandbox_id,
        &crate::db::edges::CHECKPOINT_ROLLBACK_PAUSED,
    );
}
